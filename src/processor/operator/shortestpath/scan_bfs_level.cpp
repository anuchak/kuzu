#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/configs.h"
#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SSSPMorsel* SimpleRecursiveJoinGlobalState::grabSrcDstMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset) {
    /// We are iterating over the single source SP global tracker to check if a threadID
    /// has been allotted a SP computation or not. If yes, we return the ssspMorsel.
    /// Currently we have [1 thread -> 1 ssspMorsel] enforced here.
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (ssspMorselTracker.contains(threadID)) {
        return ssspMorselTracker[threadID].get();
    }
    /// In case 1 thread is not allotted a ssspMorsel yet, we use the fTable sharedState
    /// reference to allot a SrcDstSPMorsel to the thread. After this gets done, that thread is
    /// bound to that single SrcDstSPMorsel and will extend until it finishes.
    auto ssspMorsel = std::make_unique<SSSPMorsel>(maxNodeOffset);
    ssspMorsel->srcDstFTableMorsel =
        std::move(fTableOfSrcDst->getMorsel(1 /* morsel size */));
    return (ssspMorselTracker[threadID] = std::move(ssspMorsel)).get();
}

BFSLevelMorsel SSSPMorsel::grabBFSLevelMorsel() {
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (curBFSLevel->bfsLevelScanStartIdx == curBFSLevel->bfsLevelNodes.size()) {
        return {curBFSLevel->bfsLevelScanStartIdx, 0 /* bfsLevelMorsel size */};
    }
    auto bfsLevelMorselSize = std::max(DEFAULT_VECTOR_CAPACITY,
        curBFSLevel->bfsLevelNodes.size() - curBFSLevel->bfsLevelScanStartIdx);
    BFSLevelMorsel bfsLevelMorsel =
        BFSLevelMorsel(curBFSLevel->bfsLevelScanStartIdx, bfsLevelMorselSize);
    curBFSLevel->bfsLevelScanStartIdx += bfsLevelMorselSize;
    return bfsLevelMorsel;
}

void SSSPMorsel::setDstNodeOffsets(std::shared_ptr<common::ValueVector>& valueVector) const {
    for (int i = 0; i < valueVector->state->selVector->selectedSize; i++) {
        auto destIdx = valueVector->state->selVector->selectedPositions[i];
        if (!valueVector->isNull(destIdx)) {
            bfsVisitedNodes->operator[](valueVector->readNodeOffset(destIdx)) = NOT_VISITED_DST;
            dstNodeDistances->operator[](valueVector->readNodeOffset(destIdx)) = 0u;
        }
    }
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : srcDstNodeIDVectorsDataPos) {
        srcDstNodeIDVectors.push_back(resultSet->getValueVector(dataPos));
    }
    dstBFSLevel = resultSet->getValueVector(dstBFSLevelVectorDataPos);
    nodesToExtend = resultSet->getValueVector(nodesToExtendDataPos);
}

bool ScanBFSLevel::getNextTuplesInternal() {
    if (!ssspMorsel) {
        ssspMorsel = simpleRecursiveJoinGlobalState->grabSrcDstMorsel(threadID, maxNodeOffset);
        auto& srcDstSPMorsel = ssspMorsel->srcDstFTableMorsel;
        if (srcDstSPMorsel->numTuples == 0) {
            return false;
        }
    }
    auto& curBFSLevel = ssspMorsel->curBFSLevel;
    // First BFS level extension, we have to scan the FTable for src and dst nodes.
    if (curBFSLevel->bfsLevelNodes.empty()) {
        initializeNewSSSPMorsel();
        return true;
    }
    auto bfsLevelMorsel = ssspMorsel->grabBFSLevelMorsel();
    // If there are no more nodes to extend in curLevel and nextLevel is empty (already visited).
    // Meaning we have no more bfsLevel extensions to do, we output the dstDistances to val vector.
    if (bfsLevelMorsel.bfsLevelMorselSize == 0 && ssspMorsel->nextBFSLevel->bfsLevelNodes.empty()) {
        writeDistToOutputVector();
        return false;
    }
    initializeNextBFSLevel(bfsLevelMorsel);
    return true;
}

/*
 * This function is for the 1st time a SSSPMorsel has been allotted to the thread.
 * We have to scan the factorized table (containing the src & dest nodes).
 * We initialize curLevel, nextBFSLevel and copy the src into the nodesToExtendValueVector.
 */
void ScanBFSLevel::initializeNewSSSPMorsel() {
    auto& srcDstFTableMorsel = ssspMorsel->srcDstFTableMorsel;
    auto& curBFSLevel = ssspMorsel->curBFSLevel;
    auto& nextBFSLevel = ssspMorsel->nextBFSLevel;
    srcDstFTableMorsel->table->scan(srcDstNodeIDVectors, srcDstFTableMorsel->startTupleIdx,
        srcDstFTableMorsel->numTuples, ftColIndicesOfSrcAndDstNodeIDs);
    ssspMorsel->setDstNodeOffsets(srcDstNodeIDVectors[1]);
    auto srcNodeID = ((nodeID_t*)(srcDstNodeIDVectors[0]->getData()))[0];
    curBFSLevel->bfsLevelNodes[srcNodeID.offset] = srcNodeID;
    curBFSLevel->bfsLevelScanStartIdx++; // we added 1 node (src) to the bfsLevel
    nodesToExtend->setValue<nodeID_t>(0 /* pos */, srcNodeID);
    nodesToExtend->state->initOriginalAndSelectedSize(1 /* size */);

    // SimpleRecursiveJoin will add nodes to the next level.
    nextBFSLevel->bfsLevelNumber = curBFSLevel->bfsLevelNumber + 1;
}

// Write (only) reached destination distances to output value vector
void ScanBFSLevel::writeDistToOutputVector() {
    uint32_t size = 0u;
    auto& visitedNodes = *ssspMorsel->bfsVisitedNodes;
    for (int nodeOffset = 0; nodeOffset < visitedNodes.size(); nodeOffset++) {
        if (visitedNodes[nodeOffset] == VISITED_DST) {
            auto distValue = ssspMorsel->dstNodeDistances->operator[](nodeOffset);
            dstBFSLevel->setValue<int64_t>(size++, distValue);
        }
    }
    dstBFSLevel->state->initOriginalAndSelectedSize(size);
}

// Either set nextLevel to curLevel to extend the next set of nodes.
// Or extend next batch of curLevel nodes allotted to bfsLevelMorsel.
void ScanBFSLevel::initializeNextBFSLevel(BFSLevelMorsel& bfsLevelMorsel) {
    auto& curBFSLevel = ssspMorsel->curBFSLevel;
    auto& nextBFSLevel = ssspMorsel->nextBFSLevel;
    // There is no more bfsLevelMorsel to grab from the curLevel of the ssspMorsel.
    if (bfsLevelMorsel.bfsLevelMorselSize == 0) {
        // The next BFSLevelNodes vector is empty, meaning all of them have been visited.
        curBFSLevel = std::move(nextBFSLevel);
        rearrangeCurBFSLevelNodes();
        nextBFSLevel = std::make_unique<BFSLevel>();
        nextBFSLevel->bfsLevelNumber = curBFSLevel->bfsLevelNumber + 1;
    }
    copyNodeIDsToVector(*curBFSLevel, bfsLevelMorsel);
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
void ScanBFSLevel::rearrangeCurBFSLevelNodes() const {
    auto& curBFSLevel = ssspMorsel->curBFSLevel;
    auto orderedNodeIDVector = std::vector<nodeID_t>();
    auto nodeMask = ssspMorsel->nodeMask;
    for (common::offset_t nodeOffset = 0u; nodeOffset <= maxNodeOffset; nodeOffset++) {
        if (nodeMask[nodeOffset]) {
            orderedNodeIDVector.push_back(curBFSLevel->getBFSLevelNodeID(nodeOffset));
        }
    }
    curBFSLevel->bfsLevelNodes = orderedNodeIDVector;
    // Reset mask finally here when all nodeIDs have been read after extension.
    std::fill(nodeMask.begin(), nodeMask.end(), false);
}

void ScanBFSLevel::copyNodeIDsToVector(BFSLevel& curBFSLevel, BFSLevelMorsel& bfsLevelMorsel) {
    auto finalScanIdx = bfsLevelMorsel.bfsLevelScanStartIdx + bfsLevelMorsel.bfsLevelMorselSize;
    for (auto idx = bfsLevelMorsel.bfsLevelScanStartIdx; idx < finalScanIdx; idx++) {
        auto nodeID = curBFSLevel.bfsLevelNodes[idx];
        nodesToExtend->setValue<nodeID_t>(idx, nodeID);
    }
    nodesToExtend->state->initOriginalAndSelectedSize(bfsLevelMorsel.bfsLevelMorselSize);
}

} // namespace processor
} // namespace kuzu
