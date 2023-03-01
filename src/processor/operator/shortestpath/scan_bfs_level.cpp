#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/configs.h"
#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SingleSrcSPState* SimpleRecursiveJoinGlobalState::grabSrcDstMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset) {
    /// We are iterating over the single source SP global tracker to check if a threadID
    /// has been allotted a SP computation or not. If yes, we return the singleSrcSPState.
    /// Currently we have [1 thread -> 1 singleSrcSPState] enforced here.
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (singleSrcSPTracker.contains(threadID)) {
        return singleSrcSPTracker[threadID].get();
    }
    /// In case 1 thread is not allotted a singleSrcSPState yet, we use the fTable sharedState
    /// reference to allot a SrcDstSPMorsel to the thread. After this gets done, that thread is
    /// bound to that single SrcDstSPMorsel and will extend until it finishes.
    auto singleSrcSPState = std::make_unique<SingleSrcSPState>(maxNodeOffset);
    singleSrcSPState->srcDstSPMorsel = std::move(fTableOfSrcDst->getMorsel(1 /* morsel size */));
    return (singleSrcSPTracker[threadID] = std::move(singleSrcSPState)).get();
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : outVecPositions) {
        auto valueVector = resultSet->getValueVector(dataPos);
        vectorsToScan.push_back(valueVector);
    }
    nodesToExtendValueVector = resultSet->getValueVector(bfsInputVectorDataPos);
}

// Copy 2048 OR all nodes in current level to the nodesToExtend ValueVector.
// Then, ScalRelTableList will extend them.
uint32_t ScanBFSLevel::copyNodeIDsToVector(BFSLevel& currBFSLevel) {
    auto size = std::max(DEFAULT_VECTOR_CAPACITY,
        currBFSLevel.bfsLevelNodes.size() - currBFSLevel.bfsLevelScanStartIdx);
    uint32_t idx;
    for (idx = currBFSLevel.bfsLevelScanStartIdx; idx < (size + currBFSLevel.bfsLevelScanStartIdx);
         idx++) {
        auto nodeID = currBFSLevel.bfsLevelNodes[idx];
        nodesToExtendValueVector->setValue<nodeID_t>(idx, nodeID);
    }
    currBFSLevel.bfsLevelScanStartIdx += size;
    nodesToExtendValueVector->state->initOriginalAndSelectedSize(size);
    return size;
}

bool ScanBFSLevel::getNextTuplesInternal() {
    auto singleSrcSPState =
        simpleRecursiveJoinGlobalState->grabSrcDstMorsel(threadID, maxNodeOffset);
    auto& srcDstSPMorsel = singleSrcSPState->srcDstSPMorsel;
    if (srcDstSPMorsel->numTuples == 0) {
        return false;
    }
    auto& currBFSLevel = singleSrcSPState->currBFSLevel;
    auto& nextBFSLevel = singleSrcSPState->nextBFSLevel;
    if (currBFSLevel->bfsLevelNodes.empty()) {
        srcDstSPMorsel->table->scan(vectorsToScan, srcDstSPMorsel->startTupleIdx,
            srcDstSPMorsel->numTuples, colIndicesToScan);
        auto srcNodeID = ((nodeID_t*)(vectorsToScan[0]->getData()))[0];
        currBFSLevel->bfsLevelNodes[srcNodeID.offset] = srcNodeID;
        currBFSLevel->bfsLevelScanStartIdx++; // we added 1 node (src) to the bfsLevel
        nodesToExtendValueVector->setValue<nodeID_t>(0 /* pos */, srcNodeID);
        nodesToExtendValueVector->state->initOriginalAndSelectedSize(1 /* size */);

        // SimpleRecursiveJoin will add nodes to the next level.
        nextBFSLevel->bfsLevelHeight = currBFSLevel->bfsLevelHeight + 1;
    } else {
        // If we have extended all nodes in current level, we make the next level as the current
        // level. This indicates that now we are ready to extend a new batch of nodes.
        if (currBFSLevel->bfsLevelScanStartIdx == currBFSLevel->bfsLevelNodes.size()) {
            currBFSLevel = std::move(nextBFSLevel);
            rearrangeCurrBFSLevelNodes(singleSrcSPState);
            nextBFSLevel = std::make_unique<BFSLevel>();
            nextBFSLevel->bfsLevelHeight = currBFSLevel->bfsLevelHeight + 1;
        }
        copyNodeIDsToVector(*currBFSLevel);
    }
    return true;
}

/*
 * This function places the nodeIDs in next BFSLevel to be extended in order according
 * to the mask generated. We wait to reset the mask here and NOT in the SimpleRecursiveJoin because
 * we first place all of them in the `BFSLevel` struct.
 *
 * Because we can extend only 2048 nodes at a time & also only 2048 elements are read from the
 * adjacency list at a time we can't know at the top level when the current frontier extension
 * has been completed. Hence, we need to do it here.
 */
void ScanBFSLevel::rearrangeCurrBFSLevelNodes(SingleSrcSPState* singleSrcSPState) const {
    auto& currBFSLevel = singleSrcSPState->currBFSLevel;
    auto orderedNodeIDVector = std::vector<nodeID_t>();
    auto nodeMask = singleSrcSPState->nodeMask;
    for (common::offset_t nodeOffset = 0u; nodeOffset <= maxNodeOffset; nodeOffset++) {
        if (nodeMask[nodeOffset]) {
            orderedNodeIDVector.push_back(currBFSLevel->getBFSLevelNodeID(nodeOffset));
        }
    }
    currBFSLevel->bfsLevelNodes = orderedNodeIDVector;
    // Reset mask finally here when all nodeIDs have been read after extension.
    std::fill(nodeMask.begin(), nodeMask.end(), false);
}

} // namespace processor
} // namespace kuzu
