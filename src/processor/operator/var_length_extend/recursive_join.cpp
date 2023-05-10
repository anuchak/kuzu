#include "processor/operator/var_length_extend/recursive_join.h"

namespace kuzu {
namespace processor {

bool ScanFrontier::getNextTuplesInternal(ExecutionContext* context) {
    if (!hasExecuted) {
        hasExecuted = true;
        return true;
    }
    return false;
}

void RecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    morselDispatcher->setNumThreads(context->numThreads);
    for (auto& dataPos : vectorsToScanPos) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos).get());
    }
    srcNodeIDVector = resultSet->getValueVector(srcNodeIDVectorPos);
    dstNodeIDVector = resultSet->getValueVector(dstNodeIDVectorPos);
    distanceVector = resultSet->getValueVector(distanceVectorPos);
    initLocalRecursivePlan(context);
}

// There are two high level steps.
//
// (1) BFS Computation phase: Grab a new source to do a BFS and compute an entire BFS starting from
// a single source;
//
// (2) Outputting phase: Once a computation finishes, we output the results in pieces of vectors to
// the parent operator.
//
// These 2 steps are repeated iteratively until all sources to do a recursive computation are
// exhausted. The first if statement checks if we are in the outputting phase and if so, scans a
// vector to output and returns true. Otherwise, we compute a new BFS.
bool RecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        auto ret =
            morselDispatcher->writeDstNodeIDAndDistance(inputFTableSharedState, vectorsToScan,
                colIndicesToScan, dstNodeIDVector, distanceVector, nodeTable->getTableID());
        if (ret > 0) {
            return true;
        } else if (ret == 0) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(common::THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
            continue;
        } else {
            if (!computeBFS(context)) { // Phase 1
                return false;
            }
        }
    }
}

bool RecursiveJoin::computeBFS(ExecutionContext* context) {
    while (true) {
        auto bfsComputationState = morselDispatcher->getBFSMorsel(
            inputFTableSharedState, vectorsToScan, colIndicesToScan, srcNodeIDVector, bfsMorsel);
        if (!bfsMorsel) {
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
                while (root->getNextTuple(context)) { // Exhaust recursive plan.
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

std::unique_ptr<ResultSet> RecursiveJoin::getLocalResultSet() {
    // Create two datachunks each with 1 nodeID value vector.
    // DataChunk 1 has flat state and contains the temporary src node ID vector for recursive join.
    // DataChunk 2 has unFlat state and contains the temporary dst node ID vector for recursive
    // join.
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

void RecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto op = root.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    scanFrontier = (ScanFrontier*)op;
    localResultSet = getLocalResultSet();
    tmpDstNodeIDVector = localResultSet->getValueVector(getTmpDstNodeVectorPos());
    root->initLocalState(localResultSet.get(), context);
}

} // namespace processor
} // namespace kuzu
