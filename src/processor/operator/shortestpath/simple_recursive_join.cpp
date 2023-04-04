#include "processor/operator/shortestpath/simple_recursive_join.h"

#include "common/types/internal_id_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void SimpleRecursiveJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    inputIDVector = resultSet->getValueVector(inputIDPos);
    for (auto [dataPos, _] : payloadsPosAndType) {
        auto vector = resultSet->getValueVector(dataPos);
        vectorsToCollect.push_back(vector.get());
    }
    localFTable = std::make_unique<FactorizedTable>(context->memoryManager, populateTableSchema());
}

void SimpleRecursiveJoin::executeInternal(kuzu::processor::ExecutionContext* context) {
    while (true) {
        /*
         * If the child operator returns false, there are 2 cases to be considered -
         *
         * 1) ScanBFSLevel failed to grab an SSSPMorsel: The assigned morsel for the thread will be
         * null, and we should merge the local factorized table to the global table and exit.
         *
         * 2) The SSSPMorsel is complete: If the morsel is complete, then we try to write the
         * destination distances to the distance value vector. If the total distances written is
         * greater than 0, that indicates some destinations were reached, and we should append the
         * vectors to the factorized table. If not, we continue (meaning fetch another morsel).
         */
        if (!children[0]->getNextTuple()) {
            auto ssspMorsel = simpleRecursiveJoinGlobalState->getAssignedSSSPMorsel(threadID);
            if (!ssspMorsel) {
                sharedState->mergeLocalTable(*localFTable);
                return;
            } else if (ssspMorsel->isComplete(upperBound) &&
                       writeDistToOutputVector(ssspMorsel) > 0) {
                localFTable->append(vectorsToCollect);
            }
            continue;
        }
        /*
         * If an SSSPMorsel is complete, ignore traversing through the inputIDVector.
         * This can happen when we have finished a morsel but ScanRelTable is still extending nodes.
         */
        auto ssspMorsel = simpleRecursiveJoinGlobalState->getAssignedSSSPMorsel(threadID);
        if (ssspMorsel->isComplete(upperBound)) {
            continue;
        }
        auto& visitedNodes = ssspMorsel->bfsVisitedNodes;
        for (int i = 0; i < inputIDVector->state->selVector->selectedSize; i++) {
            auto selectedPos = inputIDVector->state->selVector->selectedPositions[i];
            auto nodeID = ((nodeID_t*)(inputIDVector->getData()))[selectedPos];
            if (visitedNodes[nodeID.offset] == NOT_VISITED_DST) {
                visitedNodes[nodeID.offset] = VISITED_DST;
                ssspMorsel->numDstNodesNotReached--;
                ssspMorsel->dstDistances[nodeID.offset] = ssspMorsel->nextBFSLevel->levelNumber;
            } else if (visitedNodes[nodeID.offset] == NOT_VISITED) {
                visitedNodes[nodeID.offset] = VISITED;
            } else {
                continue;
            }
            ssspMorsel->nextBFSLevel->bfsLevelNodes.emplace_back(nodeID);
        }
        if (ssspMorsel->isComplete(upperBound) && writeDistToOutputVector(ssspMorsel) > 0) {
            localFTable->append(vectorsToCollect);
        }
    }
}

// Write (only) reached destination distances to output value vector.
// This function returns the number of destinations for which a distance was written (meaning it was
// reached). If we return 0, it is an indication to NOT append the vectors to the factorized table.
uint16_t SimpleRecursiveJoin::writeDistToOutputVector(SSSPMorsel* ssspMorsel) {
    std::vector<uint16_t> newSelPositions = std::vector<uint16_t>();
    auto dstDistancesVector = resultSet->getValueVector(dstDistancesPos);
    auto dstIDVector = resultSet->getValueVector(dstIDPos);
    uint16_t distancesWritten = 0u;
    for (int i = 0; i < dstIDVector->state->selVector->selectedSize; i++) {
        auto dstIdx = dstIDVector->state->selVector->selectedPositions[i];
        if (!dstIDVector->isNull(dstIdx)) {
            auto nodeOffset = dstIDVector->readNodeOffset(dstIdx);
            auto visitedState = ssspMorsel->bfsVisitedNodes[nodeOffset];
            if (visitedState == VISITED_DST &&
                ssspMorsel->dstDistances[nodeOffset] >= lowerBound) {
                newSelPositions.push_back(dstIdx);
                dstDistancesVector->setValue<int64_t>(
                    dstIdx, ssspMorsel->dstDistances[nodeOffset]);
                distancesWritten++;
            }
        }
    }
    dstIDVector->state->selVector->resetSelectorToValuePosBufferWithSize(newSelPositions.size());
    for (int i = 0; i < newSelPositions.size(); i++) {
        dstIDVector->state->selVector->selectedPositions[i] = newSelPositions[i];
    }
    return distancesWritten;
}

void SimpleRecursiveJoin::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    sharedState->initTableIfNecessary(context->memoryManager, populateTableSchema());
}

std::unique_ptr<FactorizedTableSchema> SimpleRecursiveJoin::populateTableSchema() {
    std::unique_ptr<FactorizedTableSchema> tableSchema = std::make_unique<FactorizedTableSchema>();
    for (auto i = 0u; i < payloadsPosAndType.size(); ++i) {
        auto [dataPos, dataType] = payloadsPosAndType[i];
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(!isPayloadFlat[i], dataPos.dataChunkPos,
                isPayloadFlat[i] ? Types::getDataTypeSize(dataType) :
                                   (uint32_t)sizeof(overflow_value_t)));
    }
    return tableSchema;
}

} // namespace processor
} // namespace kuzu
