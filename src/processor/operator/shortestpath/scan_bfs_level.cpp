#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/configs.h"
#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SingleSrcSPMorsel* SimpleRecursiveJoinGlobalState::grabSrcDstMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset) {
    /// We are iterating over the single source SP global tracker to check if a threadID
    /// has been allotted a SP computation or not. If yes, we return the singleSrcSPState.
    /// Currently we have [1 thread -> 1 singleSrcSPState] enforced here.
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (ssSPMorselTracker.contains(threadID)) {
        return ssSPMorselTracker[threadID].get();
    }
    /// In case 1 thread is not allotted a singleSrcSPState yet, we use the fTable sharedState
    /// reference to allot a SrcDstSPMorsel to the thread. After this gets done, that thread is
    /// bound to that single SrcDstSPMorsel and will extend until it finishes.
    auto singleSrcSPMorsel = std::make_unique<SingleSrcSPMorsel>(maxNodeOffset);
    singleSrcSPMorsel->srcDstFTableMorsel =
        std::move(fTableOfSrcDst->getMorsel(1 /* morsel size */));
    return (ssSPMorselTracker[threadID] = std::move(singleSrcSPMorsel)).get();
}

BFSLevelMorsel SingleSrcSPMorsel::grabBFSLevelMorsel() {
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (currBFSLevel->bfsLevelScanStartIdx == currBFSLevel->bfsLevelNodes.size()) {
        return {currBFSLevel->bfsLevelScanStartIdx, 0 /* bfsLevelMorsel size */};
    }
    auto bfsLevelMorselSize = std::max(DEFAULT_VECTOR_CAPACITY,
        currBFSLevel->bfsLevelNodes.size() - currBFSLevel->bfsLevelScanStartIdx);
    BFSLevelMorsel bfsLevelMorsel =
        BFSLevelMorsel(currBFSLevel->bfsLevelScanStartIdx, bfsLevelMorselSize);
    currBFSLevel->bfsLevelScanStartIdx += bfsLevelMorselSize;
    return bfsLevelMorsel;
}

void SingleSrcSPMorsel::setDstNodeOffsets(std::shared_ptr<common::ValueVector>& valueVector) const {
    for (int i = 0; i < valueVector->state->selVector->selectedSize; i++) {
        auto destIdx = valueVector->state->selVector->selectedPositions[i];
        if (!valueVector->isNull(destIdx)) {
            dstNodeDistances->operator[](valueVector->readNodeOffset(destIdx)) = 0u;
        }
    }
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : inputValVectorPos) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos));
    }
    outputValueVector = resultSet->getValueVector(outputValVectorPos);
    nodesToExtendValueVector = resultSet->getValueVector(nodesToExtendDataPos);
}

bool ScanBFSLevel::getNextTuplesInternal() {
    if (!singleSrcSPMorsel) {
        singleSrcSPMorsel =
            simpleRecursiveJoinGlobalState->grabSrcDstMorsel(threadID, maxNodeOffset);
        auto& srcDstSPMorsel = singleSrcSPMorsel->srcDstFTableMorsel;
        if (srcDstSPMorsel->numTuples == 0) {
            return false;
        }
    }
    auto& currBFSLevel = singleSrcSPMorsel->currBFSLevel;
    // First BFS level extension, we have to scan the FTable for src and dst nodes.
    if (currBFSLevel->bfsLevelNodes.empty()) {
        initializeNewSSSPMorsel();
        return true;
    }
    auto bfsLevelMorsel = singleSrcSPMorsel->grabBFSLevelMorsel();
    // If there are no more nodes to extend in currLevel and nextLevel is empty (already visited).
    // Meaning we have no more bfsLevel extensions to do, we output the dstDistances to val vector.
    if (bfsLevelMorsel.bfsLevelMorselSize == 0 &&
        singleSrcSPMorsel->nextBFSLevel->bfsLevelNodes.empty()) {
        writeDistToOutputVector();
        return false;
    }
    initializeNextBFSLevel(bfsLevelMorsel);
    return true;
}

/*
 * This function is for the 1st time a SingleSrcSPMorsel has been allotted to the thread.
 * We have to scan the factorized table (containing the src & dest nodes).
 * We initialize currBFSLevel, nextBFSLevel and copy the src into the nodesToExtendValueVector.
 */
void ScanBFSLevel::initializeNewSSSPMorsel() {
    auto& srcDstFTableMorsel = singleSrcSPMorsel->srcDstFTableMorsel;
    auto& currBFSLevel = singleSrcSPMorsel->currBFSLevel;
    auto& nextBFSLevel = singleSrcSPMorsel->nextBFSLevel;
    srcDstFTableMorsel->table->scan(vectorsToScan, srcDstFTableMorsel->startTupleIdx,
        srcDstFTableMorsel->numTuples, colIndicesToScan);
    singleSrcSPMorsel->setDstNodeOffsets(vectorsToScan[1]);
    auto srcNodeID = ((nodeID_t*)(vectorsToScan[0]->getData()))[0];
    currBFSLevel->bfsLevelNodes[srcNodeID.offset] = srcNodeID;
    currBFSLevel->bfsLevelScanStartIdx++; // we added 1 node (src) to the bfsLevel
    nodesToExtendValueVector->setValue<nodeID_t>(0 /* pos */, srcNodeID);
    nodesToExtendValueVector->state->initOriginalAndSelectedSize(1 /* size */);

    // SimpleRecursiveJoin will add nodes to the next level.
    nextBFSLevel->bfsLevelNumber = currBFSLevel->bfsLevelNumber + 1;
}

// Write (only) reached destination distances to output value vector
void ScanBFSLevel::writeDistToOutputVector() {
    uint32_t size = 0u;
    for (auto& dstNodeDistPair : *singleSrcSPMorsel->dstNodeDistances) {
        auto& dstNodeOffset = dstNodeDistPair.first;
        auto& dstNodeBFSDistance = dstNodeDistPair.second;
        if (singleSrcSPMorsel->bfsVisitedNodes->operator[](dstNodeOffset)) {
            outputValueVector->setValue<int64_t>(size++, dstNodeBFSDistance);
        }
    }
    outputValueVector->state->initOriginalAndSelectedSize(size);
}

// Either set nextLevel to currLevel to extend the next set of nodes.
// Or extend next batch of currLevel nodes allotted to bfsLevelMorsel.
void ScanBFSLevel::initializeNextBFSLevel(BFSLevelMorsel& bfsLevelMorsel) {
    auto& currBFSLevel = singleSrcSPMorsel->currBFSLevel;
    auto& nextBFSLevel = singleSrcSPMorsel->nextBFSLevel;
    // There is no more bfsLevelMorsel to grab from the currLevel of the singleSrcSPMorsel.
    if (bfsLevelMorsel.bfsLevelMorselSize == 0) {
        // The next BFSLevelNodes vector is empty, meaning all of them have been visited.
        currBFSLevel = std::move(nextBFSLevel);
        rearrangeCurrBFSLevelNodes();
        nextBFSLevel = std::make_unique<BFSLevel>();
        nextBFSLevel->bfsLevelNumber = currBFSLevel->bfsLevelNumber + 1;
    }
    copyNodeIDsToVector(*currBFSLevel, bfsLevelMorsel);
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
void ScanBFSLevel::rearrangeCurrBFSLevelNodes() const {
    auto& currBFSLevel = singleSrcSPMorsel->currBFSLevel;
    auto orderedNodeIDVector = std::vector<nodeID_t>();
    auto nodeMask = singleSrcSPMorsel->nodeMask;
    for (common::offset_t nodeOffset = 0u; nodeOffset <= maxNodeOffset; nodeOffset++) {
        if (nodeMask[nodeOffset]) {
            orderedNodeIDVector.push_back(currBFSLevel->getBFSLevelNodeID(nodeOffset));
        }
    }
    currBFSLevel->bfsLevelNodes = orderedNodeIDVector;
    // Reset mask finally here when all nodeIDs have been read after extension.
    std::fill(nodeMask.begin(), nodeMask.end(), false);
}

void ScanBFSLevel::copyNodeIDsToVector(BFSLevel& currBFSLevel, BFSLevelMorsel& bfsLevelMorsel) {
    auto finalScanIdx = bfsLevelMorsel.bfsLevelScanStartIdx + bfsLevelMorsel.bfsLevelMorselSize;
    for (auto idx = bfsLevelMorsel.bfsLevelScanStartIdx; idx < finalScanIdx; idx++) {
        auto nodeID = currBFSLevel.bfsLevelNodes[idx];
        nodesToExtendValueVector->setValue<nodeID_t>(idx, nodeID);
    }
    nodesToExtendValueVector->state->initOriginalAndSelectedSize(bfsLevelMorsel.bfsLevelMorselSize);
}

} // namespace processor
} // namespace kuzu
