#include "processor/operator/shortestpath/simple_recursive_join.h"

#include "common/types/internal_id_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void SimpleRecursiveJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    inputNodeIDVector = resultSet->getValueVector(inputNodeIDDataPos);
    for (auto& dataPos : srcDstNodeIDDataPos) {
        srcDstNodeIDValueVectors.push_back(resultSet->getValueVector(dataPos));
    }
    dstDistances = resultSet->getValueVector(dstDistanceVectorDataPos);
}

bool SimpleRecursiveJoin::getNextTuplesInternal() {
    while (true) {
        if (!children[0]->getNextTuple()) {
            auto ssspMorsel = simpleRecursiveJoinGlobalState->getAssignedSSSPMorsel(threadID);
            if (!ssspMorsel) {
                return false;
            } else if (ssspMorsel->isComplete(bfsUpperBound) &&
                       writeDistToOutputVector(ssspMorsel)) {
                return true;
            } else {
                continue;
            }
        }
        auto ssspMorsel = simpleRecursiveJoinGlobalState->getAssignedSSSPMorsel(threadID);
        if (ssspMorsel->isComplete(bfsUpperBound)) {
            continue;
        }
        auto& visitedNodes = ssspMorsel->bfsVisitedNodes;
        for (int i = 0; i < inputNodeIDVector->state->selVector->selectedSize; i++) {
            auto selectedPos = inputNodeIDVector->state->selVector->selectedPositions[i];
            auto nodeID = ((nodeID_t*)(inputNodeIDVector->getData()))[selectedPos];
            if (visitedNodes[nodeID.offset] == NOT_VISITED_DST) {
                visitedNodes[nodeID.offset] = VISITED_DST;
                ssspMorsel->numDstNodesNotReached--;
                ssspMorsel->dstNodeDistances[nodeID.offset] = ssspMorsel->nextBFSLevel->levelNumber;
            } else if (visitedNodes[nodeID.offset] == NOT_VISITED) {
                visitedNodes[nodeID.offset] = VISITED;
            } else {
                continue;
            }
            ssspMorsel->nextBFSLevel->bfsLevelNodes.emplace_back(nodeID);
        }
        if (ssspMorsel->isComplete(bfsUpperBound) && writeDistToOutputVector(ssspMorsel)) {
            return true;
        }
    }
}

// Write (only) reached destination distances to output value vector.
bool SimpleRecursiveJoin::writeDistToOutputVector(SSSPMorsel* ssspMorsel) {
    auto dstValVector = srcDstNodeIDValueVectors[1];
    auto srcValVector = srcDstNodeIDValueVectors[0];
    std::vector<uint16_t> newSelPositions = std::vector<uint16_t>();
    for (int i = 0; i < dstValVector->state->selVector->selectedSize; i++) {
        auto destIdx = dstValVector->state->selVector->selectedPositions[i];
        if (!dstValVector->isNull(destIdx)) {
            auto nodeOffset = dstValVector->readNodeOffset(destIdx);
            auto visitedState = ssspMorsel->bfsVisitedNodes[nodeOffset];
            if (visitedState == VISITED_DST &&
                ssspMorsel->dstNodeDistances[nodeOffset] >= bfsLowerBound) {
                newSelPositions.push_back(destIdx);
                dstDistances->setValue<int64_t>(destIdx, ssspMorsel->dstNodeDistances[nodeOffset]);
            }
        }
    }
    // In case NO destination was reached in an SSSPMorsel, then the source should also not be
    // printed in the final output. This is the same semantics as Neo4j, where an empty result is
    // returned when no dest is reached. That's why we set both srcValVector and the dstValVector
    // sharedState's selectedSize to be 0.
    if (newSelPositions.empty()) {
        dstValVector->state->selVector->selectedSize = 0u;
        srcValVector->state->selVector->selectedSize = 0u;
        return false;
    }
    dstValVector->state->selVector->resetSelectorToValuePosBufferWithSize(newSelPositions.size());
    for (int i = 0; i < newSelPositions.size(); i++) {
        dstValVector->state->selVector->selectedPositions[i] = newSelPositions[i];
    }
    return true;
}

} // namespace processor
} // namespace kuzu
