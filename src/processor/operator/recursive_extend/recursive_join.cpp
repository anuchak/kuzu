#include "processor/operator/recursive_extend/recursive_join.h"

namespace kuzu {
namespace processor {

bool ScanFrontier::getNextTuplesInternal(ExecutionContext* context) {
    if (!hasExecuted) {
        hasExecuted = true;
        return true;
    }
    return false;
}

void BaseRecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    for (auto& dataPos : vectorsToScanPos) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos).get());
    }
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDVectorPos);
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDVectorPos);
    distanceVector = resultSet->getValueVector(distanceVectorPos);
    initLocalRecursivePlan(context);
}

bool BaseRecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        auto ret = morselDispatcher->writeDstNodeIDAndDistance(sharedState->inputFTableSharedState,
            vectorsToScan, colIndicesToScan, dstNodeIDVector, distanceVector,
            nodeTable->getTableID());
        if (ret > 0) {
            return true;
        } else if (ret == 0) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(common::THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
            continue;
        } else {
            bfsMorsel->resetFlag = false;
            if (!computeBFS(context)) { // Phase 1
                return false;
            }
        }
    }
}

bool BaseRecursiveJoin::computeBFS(ExecutionContext* context) {
    while (true) {
        auto bfsComputationState =
            morselDispatcher->getBFSMorsel(sharedState->inputFTableSharedState, vectorsToScan,
                colIndicesToScan, srcNodeIDVector, bfsMorsel);
        if (!bfsMorsel->resetFlag) {
            switch (bfsComputationState) {
            case SSSP_MORSEL_INCOMPLETE:
                std::this_thread::sleep_for(
                    std::chrono::microseconds(common::THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
                continue;
            case SSSP_MORSEL_COMPLETE:
                return true;
            case SSSP_COMPUTATION_COMPLETE:
                return false;
            default:
                assert(false);
            }
        } else {
            common::offset_t nodeOffset = bfsMorsel->getNextNodeOffset();
            while (nodeOffset != common::INVALID_OFFSET) {
                scanFrontier->setNodeID(common::nodeID_t{nodeOffset, nodeTable->getTableID()});
                while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
                    bfsMorsel->addToLocalNextBFSLevel(tmpDstNodeIDVector);
                }
                nodeOffset = bfsMorsel->getNextNodeOffset();
            }
            if (morselDispatcher->finishBFSMorsel(bfsMorsel)) {
                return true;
            }
        }
    }
}

// ResultSet for list extend, i.e. 2 data chunks each with 1 vector.
static std::unique_ptr<ResultSet> populateResultSetWithTwoDataChunks() {
    auto resultSet = std::make_unique<ResultSet>(2);
    auto dataChunk0 = std::make_shared<common::DataChunk>(1);
    dataChunk0->state = common::DataChunkState::getSingleValueDataChunkState();
    dataChunk0->insert(0, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    auto dataChunk1 = std::make_shared<common::DataChunk>(1);
    dataChunk1->insert(0, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    resultSet->insert(0, std::move(dataChunk0));
    resultSet->insert(1, std::move(dataChunk1));
    return resultSet;
}

// ResultSet for column extend, i.e. 1 data chunk with 2 vectors.
static std::unique_ptr<ResultSet> populateResultSetWithOneDataChunk() {
    auto resultSet = std::make_unique<ResultSet>(1);
    auto dataChunk0 = std::make_shared<common::DataChunk>(2);
    dataChunk0->state = common::DataChunkState::getSingleValueDataChunkState();
    dataChunk0->insert(0, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    dataChunk0->insert(1, std::make_shared<common::ValueVector>(common::INTERNAL_ID, nullptr));
    resultSet->insert(0, std::move(dataChunk0));
    return resultSet;
}

std::unique_ptr<ResultSet> BaseRecursiveJoin::getLocalResultSet() {
    auto numDataChunks = tmpDstNodeIDVectorPos.dataChunkPos + 1;
    if (numDataChunks == 2) {
        return populateResultSetWithTwoDataChunks();
    } else {
        assert(tmpDstNodeIDVectorPos.dataChunkPos == 0);
        return populateResultSetWithOneDataChunk();
    }
}

void BaseRecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto op = recursiveRoot.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    scanFrontier = (ScanFrontier*)op;
    localResultSet = getLocalResultSet();
    tmpDstNodeIDVector = localResultSet->getValueVector(tmpDstNodeIDVectorPos);
    recursiveRoot->initLocalState(localResultSet.get(), context);
}

} // namespace processor
} // namespace kuzu
