#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

BFSLevelMorsel SSSPMorsel::getBFSLevelMorsel() {
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
    common::offset_t srcNodeOffset, common::ValueVector* dstNodeIDValueVector) {
    for (int i = 0; i < dstNodeIDValueVector->state->selVector->selectedSize; i++) {
        auto destIdx = dstNodeIDValueVector->state->selVector->selectedPositions[i];
        if (!dstNodeIDValueVector->isNull(destIdx)) {
            auto dstNodeOffset = dstNodeIDValueVector->readNodeOffset(destIdx);
            // This is an edge case we need to consider, if the source is a part of the destination
            // nodes, then we should not count it separately as a destination. Set it as visited
            // right here and place distance as 0;
            if (dstNodeOffset == srcNodeOffset) {
                bfsVisitedNodes[srcNodeOffset] = VISITED_DST;
                dstDistances[srcNodeOffset] = 0;
            } else {
                bfsVisitedNodes[dstNodeOffset] = NOT_VISITED_DST;
                numDstNodesNotReached++;
            }
        }
    }
}

SSSPMorsel* SSSPMorselTracker::getAssignedSSSPMorsel(std::thread::id threadID) {
    std::unique_lock<std::shared_mutex> lck{mutex};
    if (ssspMorselPerThread.contains(threadID)) {
        return ssspMorselPerThread[threadID].get();
    }
    return nullptr;
}

void SSSPMorselTracker::removePrevAssignedSSSPMorsel(std::thread::id threadID) {
    std::unique_lock<std::shared_mutex> lck{mutex};
    if (ssspMorselPerThread.contains(threadID)) {
        ssspMorselPerThread.erase(threadID);
    }
}

/*
 * Returns the: i) starting scan index from FTable ii) number of tuples to scan from FTable
 */
std::pair<uint64_t, uint64_t> SSSPMorselTracker::findSSSPMorselScanRange() {
    std::unique_lock<std::shared_mutex> lck{mutex};
    if (inputFTable->getTable()->getNumTuples() == scanStartIdx) {
        return {scanStartIdx, 0u};
    }
    uint64_t startIdx = scanStartIdx;
    uint64_t srcNodeOffset = UINT64_MAX, tmpNodeOffset;
    do {
        inputFTable->getTable()->scan(
            tmpSrcOffsetVector, scanStartIdx, 1 /* numTuplesToScan */, tmpSrcOffsetColIdx);
        tmpNodeOffset = ((nodeID_t*)(tmpSrcOffsetVector[0]->getData()))[0].offset;
        if (srcNodeOffset == UINT64_MAX) {
            srcNodeOffset = tmpNodeOffset;
        }
        if (tmpNodeOffset != srcNodeOffset) {
            break;
        }
        scanStartIdx++;
    } while (scanStartIdx < inputFTable->getTable()->getNumTuples());
    return {startIdx, std::min(scanStartIdx, inputFTable->getTable()->getNumTuples()) - startIdx};
}

/*
 * This function initialises a new SSSPMorsel, it completes the following operations:
 *
 * 1) Scanning the FTable to load the source and destination nodeIDs into the ValueVectors.
 * 2) Setting the destination node offset positions.
 * 3) Initialises bfsLevelNodes of curBFSLevel, adds the source nodeID.
 */
SSSPMorsel* SSSPMorselTracker::getSSSPMorsel(std::thread::id threadID,
    common::offset_t maxNodeOffset, std::vector<common::ValueVector*> srcDstValueVectors,
    std::vector<uint32_t>& ftColIndicesToScan) {
    // If there are no morsels left, ssspMorselScanRange.second (scan range size) will be 0.
    auto ssspMorselScanRange = findSSSPMorselScanRange();
    if (ssspMorselScanRange.second == 0u) {
        removePrevAssignedSSSPMorsel(threadID);
        return nullptr;
    }
    std::unique_ptr<SSSPMorsel> ssspMorsel = std::make_unique<SSSPMorsel>(
        maxNodeOffset, ssspMorselScanRange.first, ssspMorselScanRange.second);
    // Reset the unflat destination value vector size and selected positions to default (2048).
    srcDstValueVectors[1]->state->selVector->resetSelectorToUnselected();
    inputFTable->getTable()->scan(
        srcDstValueVectors, ssspMorsel->startScanIdx, 1 /* numTuplesToScan */, ftColIndicesToScan);
    auto srcNodeID = ((nodeID_t*)(srcDstValueVectors[0]->getData()))[0];
    ssspMorsel->markDstNodeOffsets(srcNodeID.offset, srcDstValueVectors[1]);
    // curBFSLevel's levelNumber is already set as 0 in constructor of BFSLevel.
    ssspMorsel->curBFSLevel->bfsLevelNodes.push_back(srcNodeID);
    ssspMorsel->nextBFSLevel->levelNumber = ssspMorsel->curBFSLevel->levelNumber + 1;
    for (auto i = 1u; i < ssspMorsel->numTuplesToScan; i++) {
        inputFTable->getTable()->scan(srcDstValueVectors, ssspMorsel->startScanIdx + i,
            1 /* numTuplesToScan */, ftColIndicesToScan);
        ssspMorsel->markDstNodeOffsets(srcNodeID.offset, srcDstValueVectors[1]);
    }
    std::unique_lock<std::shared_mutex> lck{mutex};
    return (ssspMorselPerThread[threadID] = std::move(ssspMorsel)).get();
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : srcDstVectorsDataPos) {
        srcDstValueVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
    nodesToExtend = resultSet->getValueVector(nodesToExtendDataPos);
}

bool ScanBFSLevel::getNextTuplesInternal() {
    if (!ssspMorsel || ssspMorsel->isComplete(upperBound)) {
        ssspMorsel = ssspMorselTracker->getSSSPMorsel(
            threadID, maxNodeOffset, srcDstValueVectors, ftColIndicesToScan);
        if (!ssspMorsel) {
            return false;
        }
    }
    auto bfsLevelMorsel = ssspMorsel->getBFSLevelMorsel();
    // If there are no more nodes to extend in curBFSLevel, just swap curBFSLevel with nextBFSLevel
    // to extend next level of BFS nodeID's.
    if (bfsLevelMorsel.isEmpty()) {
        ssspMorsel->bfsMorselNextStartIdx = 0u;
        ssspMorsel->curBFSLevel = std::move(ssspMorsel->nextBFSLevel);
        // Sort node offsets of BFS level in ascending order for sequential scan of adjList.
        std::sort(ssspMorsel->curBFSLevel->bfsLevelNodes.begin(),
            ssspMorsel->curBFSLevel->bfsLevelNodes.end(), NodeIDComparatorFunction());
        ssspMorsel->nextBFSLevel = std::make_unique<BFSLevel>();
        ssspMorsel->nextBFSLevel->levelNumber = ssspMorsel->curBFSLevel->levelNumber + 1;
        bfsLevelMorsel = ssspMorsel->getBFSLevelMorsel();
        if (ssspMorsel->isComplete(upperBound) || bfsLevelMorsel.isEmpty()) {
            return false;
        }
    }
    copyCurBFSLevelNodesToVector(*ssspMorsel->curBFSLevel, bfsLevelMorsel);
    return true;
}

void ScanBFSLevel::copyCurBFSLevelNodesToVector(
    BFSLevel& curBFSLevel, BFSLevelMorsel& bfsLevelMorsel) {
    auto finalScanIdx = bfsLevelMorsel.startIdx + bfsLevelMorsel.size;
    for (auto idx = bfsLevelMorsel.startIdx; idx < finalScanIdx; idx++) {
        auto nodeID = curBFSLevel.bfsLevelNodes[idx];
        nodesToExtend->setValue<nodeID_t>(idx - bfsLevelMorsel.startIdx, nodeID);
    }
    nodesToExtend->state->initOriginalAndSelectedSize(bfsLevelMorsel.size);
}

} // namespace processor
} // namespace kuzu
