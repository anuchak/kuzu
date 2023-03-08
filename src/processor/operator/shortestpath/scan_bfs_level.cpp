#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/configs.h"
#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SSSPMorsel* SimpleRecursiveJoinGlobalState::getSSSPMorsel(std::thread::id threadID) {
    std::unique_lock<std::shared_mutex> lck{mutex};
    if (ssspMorselTracker.contains(threadID)) {
        return ssspMorselTracker[threadID].get();
    }
    return nullptr;
}

/*
 * This function grabs and initialises a new SSSPMorsel, it completes the following operations:
 *
 * 1) Scanning the FTable to load the source and destination nodeIDs into the ValueVectors.
 * 2) Setting the destination node offset positions.
 * 3) Initialises bfsLevelNodes of curBFSLevel, adds the source nodeID.
 *
 */
SSSPMorsel* SimpleRecursiveJoinGlobalState::grabAndInitializeSSSPMorsel(std::thread::id threadID,
    common::offset_t maxNodeOffset,
    std::vector<std::shared_ptr<common::ValueVector>> srcDstNodeIDVectors,
    std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeIDs) {
    SSSPMorsel* ssspMorsel = grabSSSPMorsel(threadID, maxNodeOffset);
    auto& srcDstFTableMorsel = ssspMorsel->srcDstFTableMorsel;
    // If numTuples is 0, it means no more morsels left to allot, we exit.
    if (srcDstFTableMorsel->numTuples == 0) {
        return ssspMorsel;
    }
    auto& curBFSLevel = ssspMorsel->curBFSLevel;
    auto& nextBFSLevel = ssspMorsel->nextBFSLevel;
    srcDstFTableMorsel->table->scan(srcDstNodeIDVectors, srcDstFTableMorsel->startTupleIdx,
        srcDstFTableMorsel->numTuples, ftColIndicesOfSrcAndDstNodeIDs);
    ssspMorsel->setDstNodeOffsets(srcDstNodeIDVectors[1]);
    auto srcNodeID = ((nodeID_t*)(srcDstNodeIDVectors[0]->getData()))[0];
    ssspMorsel->dstTableID = ((nodeID_t*)(srcDstNodeIDVectors[1]->getData()))[0].tableID;
    curBFSLevel->bfsLevelNodes.push_back(srcNodeID);
    nextBFSLevel->bfsLevelNumber = curBFSLevel->bfsLevelNumber + 1;
    return ssspMorsel;
}

/*
 * This function creates a new SSSPMorsel and grabs a new srcDstFTableMorsel from fTableOfSrcDst.
 */
SSSPMorsel* SimpleRecursiveJoinGlobalState::grabSSSPMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset) {
    std::unique_lock<std::shared_mutex> lck{mutex};
    auto ssspMorsel = std::make_unique<SSSPMorsel>(maxNodeOffset);
    // If there are no morsels left, numTuples will be 0 for the srcDstFTableMorsel.
    ssspMorsel->srcDstFTableMorsel = std::move(fTableOfSrcDst->getMorsel(1 /* morsel size */));
    return (ssspMorselTracker[threadID] = std::move(ssspMorsel)).get();
}

BFSLevelMorsel SSSPMorsel::grabBFSLevelMorsel() {
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (bfsMorselNextStartIdx == curBFSLevel->bfsLevelNodes.size()) {
        return {bfsMorselNextStartIdx, 0 /* bfsLevelMorsel size */};
    }
    auto bfsLevelMorselSize = std::max(
        DEFAULT_VECTOR_CAPACITY, curBFSLevel->bfsLevelNodes.size() - bfsMorselNextStartIdx);
    BFSLevelMorsel bfsLevelMorsel = BFSLevelMorsel(bfsMorselNextStartIdx, bfsLevelMorselSize);
    bfsMorselNextStartIdx += bfsLevelMorselSize;
    return bfsLevelMorsel;
}

void SSSPMorsel::setDstNodeOffsets(std::shared_ptr<common::ValueVector>& valueVector) {
    for (int i = 0; i < valueVector->state->selVector->selectedSize; i++) {
        auto destIdx = valueVector->state->selVector->selectedPositions[i];
        if (!valueVector->isNull(destIdx)) {
            bfsVisitedNodes->operator[](valueVector->readNodeOffset(destIdx)) = NOT_VISITED_DST;
            dstNodeDistances->operator[](valueVector->readNodeOffset(destIdx)) = 0u;
            numDstNodesNotReached++;
        }
    }
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : srcDstNodeIDVectorsDataPos) {
        srcDstNodeIDVectors.push_back(resultSet->getValueVector(dataPos));
    }
    dstDistances = resultSet->getValueVector(dstDistanceVectorDataPos);
    nodesToExtend = resultSet->getValueVector(nodesToExtendDataPos);
}

bool ScanBFSLevel::getNextTuplesInternal() {
    if (!ssspMorsel || ssspMorsel->isSSSPMorselComplete) {
        ssspMorsel = simpleRecursiveJoinGlobalState->grabAndInitializeSSSPMorsel(
            threadID, maxNodeOffset, srcDstNodeIDVectors, ftColIndicesOfSrcAndDstNodeIDs);
        auto& srcDstFTableMorsel = ssspMorsel->srcDstFTableMorsel;
        if (srcDstFTableMorsel->numTuples == 0) {
            return false;
        }
    }
    // If numDstNodesNotReached is 0, it indicates we have visited ALL our destination nodes.
    if (ssspMorsel->numDstNodesNotReached == 0) {
        writeDistToOutputVector();
        ssspMorsel->isSSSPMorselComplete = true;
        return true;
    }
    auto bfsLevelMorsel = ssspMorsel->grabBFSLevelMorsel();
    // If there are no more nodes to extend in curLevel and nextLevel is empty (already visited).
    // Meaning we have no more bfsLevel extensions to do, we output the dstDistances to val vector.
    if (bfsLevelMorsel.bfsLevelMorselSize == 0 && ssspMorsel->nextBFSLevel->bfsLevelNodes.empty()) {
        writeDistToOutputVector();
        ssspMorsel->isSSSPMorselComplete = true;
        return true;
    }
    initializeNextBFSLevel(bfsLevelMorsel);
    return true;
}

// Write (only) reached destination distances to output value vector
void ScanBFSLevel::writeDistToOutputVector() {
    uint32_t size = 0u;
    auto& visitedNodes = *ssspMorsel->bfsVisitedNodes;
    for (int nodeOffset = 0; nodeOffset < visitedNodes.size(); nodeOffset++) {
        if (visitedNodes[nodeOffset] == VISITED_DST) {
            auto distValue = ssspMorsel->dstNodeDistances->operator[](nodeOffset);
            dstDistances->setValue<int64_t>(size++, distValue);
        }
    }
    dstDistances->state->initOriginalAndSelectedSize(size);
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
 * we first place all of them in the BFSLevel struct.
 * Because we can extend only 2048 nodes at a time & also only 2048 elements are read from the
 * adjacency list at a time we can't know at the top level when the current frontier extension
 * has been completed.
 */
void ScanBFSLevel::rearrangeCurBFSLevelNodes() const {
    auto& curBFSLevel = ssspMorsel->curBFSLevel;
    curBFSLevel->bfsLevelNodes = std::vector<common::nodeID_t>();
    auto& nextLevelNodeMask = ssspMorsel->nextLevelNodeMask;
    for (common::offset_t nodeOffset = 0u; nodeOffset <= maxNodeOffset; nodeOffset++) {
        if (nextLevelNodeMask[nodeOffset]) {
            curBFSLevel->bfsLevelNodes.emplace_back(nodeID_t(nodeOffset, ssspMorsel->dstTableID));
        }
    }
    // Reset mask finally here when all nodeIDs have been read after extension.
    std::fill(nextLevelNodeMask.begin(), nextLevelNodeMask.end(), false);
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
