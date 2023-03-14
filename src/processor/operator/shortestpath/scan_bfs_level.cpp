#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/configs.h"
#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

BFSLevelMorsel SSSPMorsel::grabBFSLevelMorsel() {
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (bfsMorselNextStartIdx == curBFSLevel->bfsLevelNodes.size()) {
        return BFSLevelMorsel(bfsMorselNextStartIdx, 0 /* bfsLevelMorsel size */);
    }
    auto bfsLevelMorselSize = std::min(
        DEFAULT_VECTOR_CAPACITY, curBFSLevel->bfsLevelNodes.size() - bfsMorselNextStartIdx);
    bfsMorselNextStartIdx += bfsLevelMorselSize;
    return BFSLevelMorsel(bfsMorselNextStartIdx - bfsLevelMorselSize, bfsLevelMorselSize);
}

// This function is required to track which node offsets are destination nodes, they are tracked
// by a different state NOT_VISITED_DST and set as VISITED_DST if they are visited later.
void SSSPMorsel::markDstNodeOffsets(
    common::offset_t srcNodeOffset, std::shared_ptr<common::ValueVector>& dstNodeIDValueVector) {
    for (int i = 0; i < dstNodeIDValueVector->state->selVector->selectedSize; i++) {
        auto destIdx = dstNodeIDValueVector->state->selVector->selectedPositions[i];
        if (!dstNodeIDValueVector->isNull(destIdx)) {
            auto destNodeOffset = dstNodeIDValueVector->readNodeOffset(destIdx);
            // This is an edge case we need to consider, if the source is a part of the destination
            // nodes, then we should not count it separately as a destination. Set it as visited
            // right here and place distance as 0;
            if (destNodeOffset == srcNodeOffset) {
                bfsVisitedNodes->operator[](srcNodeOffset) = VISITED_DST;
                dstNodeDistances->operator[](srcNodeOffset) = 0;
            } else {
                bfsVisitedNodes->operator[](destNodeOffset) = NOT_VISITED_DST;
                numDstNodesNotReached++;
            }
        }
    }
}

SSSPMorsel* SimpleRecursiveJoinGlobalState::getAssignedSSSPMorsel(std::thread::id threadID) {
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
    std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeIDs,
    std::vector<std::shared_ptr<common::ValueVector>> srcDstNodePropertiesVectors,
    std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeProperties) {
    auto ssspMorselAndFTableMorsel = getSSSPMorsel(threadID, maxNodeOffset);
    auto srcDstFTableMorsel = std::move(ssspMorselAndFTableMorsel.second);
    auto ssspMorsel = ssspMorselAndFTableMorsel.first;
    // If numTuples is 0, it means no more morsels left to allot, we exit.
    if (srcDstFTableMorsel->isEmpty()) {
        return ssspMorselAndFTableMorsel.first;
    }
    auto& curBFSLevel = ssspMorsel->curBFSLevel;
    auto& nextBFSLevel = ssspMorsel->nextBFSLevel;
    // Reset the unflat destination value vector size and selected positions to default (2048).
    srcDstNodeIDVectors[1]->state->selVector->resetSelectorToUnselected();
    srcDstFTableMorsel->table->scan(srcDstNodeIDVectors, srcDstFTableMorsel->startTupleIdx,
        srcDstFTableMorsel->numTuples, ftColIndicesOfSrcAndDstNodeIDs);
    srcDstFTableMorsel->table->scan(srcDstNodePropertiesVectors, srcDstFTableMorsel->startTupleIdx,
        srcDstFTableMorsel->numTuples, ftColIndicesOfSrcAndDstNodeProperties);
    auto srcNodeID = ((nodeID_t*)(srcDstNodeIDVectors[0]->getData()))[0];
    ssspMorsel->markDstNodeOffsets(srcNodeID.offset, srcDstNodeIDVectors[1]);
    ssspMorsel->dstTableID = ((nodeID_t*)(srcDstNodeIDVectors[1]->getData()))[0].tableID;
    // curBFSLevel's levelNumber is already set as 0 in constructor of BFSLevel.
    curBFSLevel->bfsLevelNodes.push_back(srcNodeID);
    nextBFSLevel->levelNumber = curBFSLevel->levelNumber + 1;
    return ssspMorsel;
}

std::pair<SSSPMorsel*, std::unique_ptr<FTableScanMorsel>>
SimpleRecursiveJoinGlobalState::getSSSPMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset) {
    std::unique_lock<std::shared_mutex> lck{mutex};
    auto ssspMorsel = std::make_unique<SSSPMorsel>(maxNodeOffset);
    // If there are no morsels left, numTuples will be 0 for the srcDstFTableMorsel.
    std::unique_ptr<FTableScanMorsel> srcDstFTableMorsel =
        std::move(fTableOfSrcDst->getMorsel(1 /* morsel size */));
    ssspMorselTracker[threadID] = std::move(ssspMorsel);
    return {ssspMorselTracker[threadID].get(), std::move(srcDstFTableMorsel)};
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : srcDstNodeIDVectorsDataPos) {
        srcDstNodeIDVectors.push_back(resultSet->getValueVector(dataPos));
    }
    for (auto& dataPos : srcDstNodePropertiesVectorsDataPos) {
        srcDstNodePropertiesVectors.push_back(resultSet->getValueVector(dataPos));
    }
    dstDistances = resultSet->getValueVector(dstDistanceVectorDataPos);
    nodesToExtend = resultSet->getValueVector(nodesToExtendDataPos);
}

bool ScanBFSLevel::getNextTuplesInternal() {
    if (!ssspMorsel || ssspMorsel->isSSSPMorselComplete) {
        ssspMorsel = simpleRecursiveJoinGlobalState->grabAndInitializeSSSPMorsel(threadID,
            maxNodeOffset, srcDstNodeIDVectors, ftColIndicesOfSrcAndDstNodeIDs,
            srcDstNodePropertiesVectors, ftColIndicesOfSrcAndDstNodeProperties);
        if (ssspMorsel->isEmpty()) {
            return false;
        }
    }
    // If numDstNodesNotReached is 0, it indicates we have visited ALL our destination nodes.
    if (ssspMorsel->numDstNodesNotReached == 0) {
        writeDistToOutputVector();
        ssspMorsel->isSSSPMorselComplete = true;
        return false;
    }
    auto bfsLevelMorsel = ssspMorsel->grabBFSLevelMorsel();
    // If there are no more nodes to extend in curLevel, there are two cases:
    // - the nextBFSLevel is empty, meaning all have been visited, termination condition.
    // - the curBFSLevel level is (upperBound-1), meaning we have reached upper limit of BFS.
    if (bfsLevelMorsel.isEmpty()) {
        if (ssspMorsel->nextBFSLevel->isEmpty() ||
            ssspMorsel->curBFSLevel->levelNumber == bfsUpperBound - 1) {
            writeDistToOutputVector();
            ssspMorsel->isSSSPMorselComplete = true;
            return false;
        } // Swap curBFSLevel with nextBFSLevel to extend next level of BFS nodeID's.
        else {
            ssspMorsel->bfsMorselNextStartIdx = 0u;
            auto& curBFSLevel = ssspMorsel->curBFSLevel;
            auto& nextBFSLevel = ssspMorsel->nextBFSLevel;
            curBFSLevel = std::move(nextBFSLevel);
            // Sort node offsets of BFS level in ascending order for sequential scan of adjList.
            std::sort(curBFSLevel->bfsLevelNodes.begin(), curBFSLevel->bfsLevelNodes.end(),
                NodeComparatorFunction());
            nextBFSLevel = std::make_unique<BFSLevel>();
            nextBFSLevel->levelNumber = curBFSLevel->levelNumber + 1;
            bfsLevelMorsel = ssspMorsel->grabBFSLevelMorsel();
        }
    }
    copyNodeIDsToVector(*ssspMorsel->curBFSLevel, bfsLevelMorsel);
    return true;
}

// Write (only) reached destination distances to output value vector.
void ScanBFSLevel::writeDistToOutputVector() {
    auto dstValVector = srcDstNodeIDVectors[1];
    auto srcValVector = srcDstNodeIDVectors[0];
    std::vector<uint16_t> newSelPositions = std::vector<uint16_t>();
    for (int i = 0; i < dstValVector->state->selVector->selectedSize; i++) {
        auto destIdx = dstValVector->state->selVector->selectedPositions[i];
        if (!dstValVector->isNull(destIdx)) {
            auto nodeOffset = dstValVector->readNodeOffset(destIdx);
            auto visitedState = ssspMorsel->bfsVisitedNodes->operator[](nodeOffset);
            auto distValue = ssspMorsel->dstNodeDistances->operator[](nodeOffset);
            if (visitedState == VISITED_DST && distValue >= bfsLowerBound) {
                newSelPositions.push_back(destIdx);
                dstDistances->setValue<int64_t>(destIdx, distValue);
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
        return;
    }
    dstValVector->state->selVector->resetSelectorToValuePosBufferWithSize(newSelPositions.size());
    for (int i = 0; i < newSelPositions.size(); i++) {
        dstValVector->state->selVector->selectedPositions[i] = newSelPositions[i];
    }
}

void ScanBFSLevel::copyNodeIDsToVector(BFSLevel& curBFSLevel, BFSLevelMorsel& bfsLevelMorsel) {
    auto finalScanIdx = bfsLevelMorsel.startIdx + bfsLevelMorsel.size;
    for (auto idx = bfsLevelMorsel.startIdx; idx < finalScanIdx; idx++) {
        auto nodeID = curBFSLevel.bfsLevelNodes[idx];
        nodesToExtend->setValue<nodeID_t>(idx - bfsLevelMorsel.startIdx, nodeID);
    }
    nodesToExtend->state->initOriginalAndSelectedSize(bfsLevelMorsel.size);
}

} // namespace processor
} // namespace kuzu
