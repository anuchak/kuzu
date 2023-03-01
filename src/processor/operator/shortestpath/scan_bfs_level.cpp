#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/configs.h"
#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SingleSrcSPState* SimpleRecursiveJoinGlobalState::grabSrcDestMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset, uint64_t maxMorselSize) {
    /// We are iterating over the single source SP global tracker to check if a threadID
    /// has been allotted a SP computation or not. If yes, we return the singleSrcSPState.
    /// Currently we have [1 thread -> 1 singleSrcSPState] enforced here.
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (singleSrcSPTracker.contains(threadID)) {
        return singleSrcSPTracker[threadID].get();
    }
    /// In case 1 thread is not allotted a singleSrcSPState yet, we use the fTable sharedState
    /// reference to allot a SrcDestSPMorsel to the thread. After this gets done, that thread is
    /// bound to that single SrcDestSPMorsel and will extend until it finishes.
    auto singleSrcSPState = std::make_unique<SingleSrcSPState>(maxNodeOffset);
    singleSrcSPState->setSrcDestSPMorsel(fTableOfSrcDest->getMorsel(maxMorselSize));
    return (singleSrcSPTracker[threadID] = std::move(singleSrcSPState)).get();
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : outVecPositions) {
        auto valueVector = resultSet->getValueVector(dataPos);
        vectorsToScan.push_back(valueVector);
    }
    maxMorselSize = simpleRecursiveJoinGlobalState->getFTableOfSrcDest()->getMaxMorselSize();
    bfsInputValueVector = resultSet->getValueVector(bfsInputVectorDataPos);
}

// Copy 2048 OR all nodes in last level to the BFS Input ValueVector.
// Then, ScalRelTableList will extend them.
uint16_t ScanBFSLevel::copyNodeIDsToVector(BFSLevel& lastBFSLevel) {
    auto size = std::max(
        DEFAULT_VECTOR_CAPACITY, lastBFSLevel.bfsLevelNodes.size() - lastBFSLevel.bfsLevelScanStartIdx);
    int idx;
    for (idx = lastBFSLevel.bfsLevelScanStartIdx; idx < (size + lastBFSLevel.bfsLevelScanStartIdx); idx++) {
        auto nodeID = lastBFSLevel.bfsLevelNodes[idx];
        bfsInputValueVector->setValue<nodeID_t>(idx, nodeID);
    }
    lastBFSLevel.bfsLevelScanStartIdx += size;
    bfsInputValueVector->state->initOriginalAndSelectedSize(size);
    return size;
}

bool ScanBFSLevel::getNextTuplesInternal() {
    auto singleSrcSPState =
        simpleRecursiveJoinGlobalState->grabSrcDestMorsel(threadID, maxNodeOffset, maxMorselSize);
    auto& srcDestSPMorsel = singleSrcSPState->getSrcDestSPMorsel();
    if (srcDestSPMorsel->numTuples == 0) {
        return false;
    }
    if (singleSrcSPState->getBFSLevels().empty()) {
        srcDestSPMorsel->table->scan(vectorsToScan, srcDestSPMorsel->startTupleIdx,
            srcDestSPMorsel->numTuples, colIndicesToScan);
        auto firstBFSLevel = std::make_unique<BFSLevel>();
        auto srcNodeID = ((nodeID_t*)(vectorsToScan[0]->getData()))[0];
        firstBFSLevel->bfsLevelNodes.push_back(srcNodeID);
        firstBFSLevel->bfsLevelScanStartIdx++; // we added 1 node (src) to the bfsLevel
        bfsInputValueVector->setValue<nodeID_t>(0 /* pos */, srcNodeID);
        bfsInputValueVector->state->initOriginalAndSelectedSize(1 /* size */);
        singleSrcSPState->getBFSLevels().push_back(std::move(firstBFSLevel));

        // We just initialize the 2nd level and push it, SimpleRecursiveJoin will add nodes to this.
        auto secondBFSLevel = std::make_unique<BFSLevel>();
        secondBFSLevel->bfsLevelScanStartIdx = 0;
        singleSrcSPState->getBFSLevels().push_back(std::move(secondBFSLevel));
    } else {
        auto& bfsLevels = singleSrcSPState->getBFSLevels();
        auto& lastBFSLevel = bfsLevels[bfsLevels.size() - 1];

        // If we have extended all nodes in last level OR this is the first time we are extending it.
        if (!lastBFSLevel->bfsLevelScanStartIdx ||
            lastBFSLevel->bfsLevelScanStartIdx == lastBFSLevel->bfsLevelNodes.size()) {
            auto newBFSLevel = std::make_unique<BFSLevel>();
            bfsLevels.push_back(std::move(newBFSLevel));
            lastBFSLevel->bfsLevelScanStartIdx = 0u;
        }
        copyNodeIDsToVector(*lastBFSLevel);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
