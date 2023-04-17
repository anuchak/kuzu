#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

BFSLevelMorsel SSSPMorsel::getBFSLevelMorsel() {
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

void SSSPMorselTracker::initTmpSrcOffsetVector(storage::MemoryManager* memoryManager) {
    auto tmpSrcIDVector = new common::ValueVector(common::DataTypeID::INTERNAL_ID, memoryManager);
    auto dataChunkState = std::make_shared<common::DataChunkState>(1);
    tmpSrcIDVector->state = dataChunkState;
    tmpSrcOffsetVector = std::vector<common::ValueVector*>();
    tmpSrcOffsetVector.push_back(tmpSrcIDVector);
    tmpSrcOffsetColIdx = std::vector<ft_col_idx_t>({0 /* srcOffset column index */});
}

// We acquire a lock here because we map each threadID to a unique thread index. And then initialize
// the SSSPMorsel to nullptr. Later this threadIdx position will be populated with the actual
// SSSPMorsel.
uint64_t SSSPMorselTracker::getLocalThreadIdx(std::thread::id threadID) {
    std::unique_lock lck{threadIdxMutex};
    uint64_t localThreadIdx = nextLocalThreadID++;
    threadIdxMap.insert({threadID, localThreadIdx});
    ssspMorselPerThreadVector[localThreadIdx] = nullptr;
    return localThreadIdx;
}

uint64_t SSSPMorselTracker::getThreadIdxForThreadID(std::thread::id threadID) {
    std::unique_lock lck{threadIdxMutex};
    return threadIdxMap[threadID];
}

// This function is thread safe because we already initialize the ssspMorselPerThreadVector with
// size equal to numThreadsForExecution. This ensures that the vector does not change while other
// threads are reading from it.
SSSPMorsel* SSSPMorselTracker::getAssignedSSSPMorsel(uint64_t threadIdx) {
    return ssspMorselPerThreadVector[threadIdx].get();
}

// This function is thread safe, same reason as above. ssspMorselPerThreadVector size in initialized
// with total threads for execution.
void SSSPMorselTracker::removePrevAssignedSSSPMorsel(uint64_t threadIdx) {
    ssspMorselPerThreadVector[threadIdx] = nullptr;
}

/*
 * Returns the: i) starting scan index from FTable ii) number of tuples to scan from FTable
 */
std::pair<uint64_t, uint64_t> SSSPMorselTracker::findSSSPMorselScanRange() {
    std::unique_lock<std::shared_mutex> lck{ssspMorselScanRangeMutex};
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
SSSPMorsel* SSSPMorselTracker::getSSSPMorsel(uint64_t threadIdx, common::offset_t maxNodeOffset,
    std::vector<common::ValueVector*> srcDstValueVectors,
    std::vector<uint32_t>& ftColIndicesToScan) {
    // If there are no morsels left, ssspMorselScanRange.second (scan range size) will be 0.
    auto ssspMorselScanRange = findSSSPMorselScanRange();
    if (ssspMorselScanRange.second == 0u) {
        removePrevAssignedSSSPMorsel(threadIdx);
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
    std::unique_lock<std::shared_mutex> lck{threadIdxMutex};
    return (ssspMorselPerThreadVector[threadIdx] = std::move(ssspMorsel)).get();
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadIdx = ssspMorselTracker->getLocalThreadIdx(std::this_thread::get_id());
    for (auto& dataPos : srcDstVectorsDataPos) {
        srcDstValueVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
    nodesToExtend = resultSet->getValueVector(nodesToExtendDataPos);
}

bool ScanBFSLevel::getNextTuplesInternal(ExecutionContext* context) {
    if (!ssspMorsel || ssspMorsel->isComplete(upperBound)) {
        ssspMorsel = ssspMorselTracker->getSSSPMorsel(
            threadIdx, maxNodeOffset, srcDstValueVectors, ftColIndicesToScan);
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
