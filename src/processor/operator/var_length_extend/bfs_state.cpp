#include "processor/operator/var_length_extend/bfs_state.h"

namespace kuzu {
namespace processor {

void SSSPMorsel::reset() {
    currentLevel = 0u;
    nextScanStartIdx = 0u;
    curBFSLevel->resetState();
    nextBFSLevel->resetState();
    numVisitedNodes = 0u;
    visitedNodes = std::vector<uint8_t>(maxOffset + 1, NOT_VISITED);
    distance.clear();
    srcOffset = 0u;
    numThreadsActiveOnMorsel = 0u;
    nextDstScanStartIdx = 0u;
    inputFTableTupleIdx = 0u;
    threadsWritingDstDistances.clear();
}

bool SSSPMorsel::isComplete() {
    if (curBFSLevel->size() == 0) { // no more to extend.
        return true;
    }
    if (currentLevel == upperBound) { // upper limit reached.
        return true;
    }
    if (numVisitedNodes == maxOffset) { // all destinations have been reached.
        return true;
    }
    return false;
}

void SSSPMorsel::markSrc() {
    visitedNodes[srcOffset] = VISITED;
    distance[srcOffset] = 0;
    numVisitedNodes++;
    curBFSLevel->bfsLevelNodes.push_back(srcOffset);
}

void SSSPMorsel::markVisited(common::offset_t offset) {
    assert(visitedNodes[offset] == NOT_VISITED);
    visitedNodes[offset] = VISITED;
    distance[offset] = currentLevel + 1;
    numVisitedNodes++;
    nextBFSLevel->bfsLevelNodes.push_back(offset);
}

void SSSPMorsel::moveNextLevelAsCurrentLevel() {
    curBFSLevel = std::move(nextBFSLevel);
    currentLevel++;
    nextBFSLevel = std::make_unique<BFSLevel>();
    if (currentLevel == upperBound) { // No need to sort if we are not extending further.
        std::sort(curBFSLevel->bfsLevelNodes.begin(), curBFSLevel->bfsLevelNodes.end());
    }
    nextScanStartIdx = 0u;
}

common::offset_t BFSMorsel::getNextNodeOffset() {
    if (startScanIdx == endScanIdx) {
        return common::INVALID_NODE_OFFSET;
    }
    return curBFSLevel->bfsLevelNodes[startScanIdx++];
}

void BFSMorsel::addToLocalNextBFSLevel(
    const std::shared_ptr<common::ValueVector>& tmpDstNodeIDVector) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        if (!localBFSVisitedNodes.contains(nodeID.offset)) {
            localBFSVisitedNodes.insert(nodeID.offset);
        }
    }
}

void MorselDispatcher::resetSSSPComputationState() {
    state = SSSP_MORSEL_INCOMPLETE;
}

bool MorselDispatcher::finishBFSMorsel(std::unique_ptr<BFSMorsel>& bfsMorsel) {
    std::unique_lock lck{mutex};
    if (ssspMorsel->isComplete()) {
        ssspMorsel->numThreadsActiveOnMorsel--;
        return true;
    }
    for (auto offset : bfsMorsel->localBFSVisitedNodes) {
        if (ssspMorsel->visitedNodes[offset] == NOT_VISITED) {
            ssspMorsel->markVisited(offset);
        }
    }
    ssspMorsel->numThreadsActiveOnMorsel--;
    if (ssspMorsel->numThreadsActiveOnMorsel == 0 &&
        ssspMorsel->nextScanStartIdx == ssspMorsel->curBFSLevel->size()) {
        ssspMorsel->moveNextLevelAsCurrentLevel();
        if (ssspMorsel->isComplete()) {
            state = SSSP_MORSEL_COMPLETE;
            return true;
        }
    }
    return false;
}

SSSPComputationState MorselDispatcher::getBFSMorsel(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
    std::unique_ptr<BFSMorsel>& bfsMorsel) {
    std::unique_lock lck{mutex};
    if (state == SSSP_COMPUTATION_COMPLETE || state == SSSP_MORSEL_COMPLETE) {
        bfsMorsel.reset();
        return state;
    }
    if (ssspMorsel->isComplete() && ssspMorsel->numThreadsActiveOnMorsel == 0) {
        auto inputFTableMorsel = inputFTableSharedState->getMorsel(1);
        // Marks end of SSSP Computation, no more source tuples to trigger SSSP.
        if (inputFTableMorsel->numTuples == 0) {
            state = SSSP_COMPUTATION_COMPLETE;
            return state;
        }
        inputFTableSharedState->getTable()->scan(vectorsToScan, inputFTableMorsel->startTupleIdx,
            inputFTableMorsel->numTuples, colIndicesToScan);
        ssspMorsel->reset();
        ssspMorsel->inputFTableTupleIdx = inputFTableMorsel->startTupleIdx;
        auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
            srcNodeIDVector->state->selVector->selectedPositions[0]);
        ssspMorsel->srcOffset = nodeID.offset;
        ssspMorsel->markSrc();
    }
    // We don't swap here, because we want the last thread to complete its BFSMorsel and only then
    // swap the curBFSLevel and nextBFSLevel (happens in Line 93 in finishBFSMorsel function).
    if (ssspMorsel->nextScanStartIdx == ssspMorsel->curBFSLevel->size()) {
        bfsMorsel.reset();
        return state;
    }
    ssspMorsel->numThreadsActiveOnMorsel++;
    auto bfsMorselSize = std::min(common::DEFAULT_VECTOR_CAPACITY,
        ssspMorsel->curBFSLevel->size() - ssspMorsel->nextScanStartIdx);
    auto morselScanEndIdx = ssspMorsel->nextScanStartIdx + bfsMorselSize;
    bfsMorsel = std::move(std::make_unique<BFSMorsel>(
        ssspMorsel->nextScanStartIdx, morselScanEndIdx, ssspMorsel->curBFSLevel));
    ssspMorsel->nextScanStartIdx += bfsMorselSize;
    return state;
}

int64_t MorselDispatcher::writeDstNodeIDAndDistance(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    const std::shared_ptr<common::ValueVector>& dstNodeIDVector,
    const std::shared_ptr<common::ValueVector>& distanceVector, common::table_id_t tableID) {
    std::unique_lock lck{mutex};
    if (state != SSSP_MORSEL_COMPLETE) {
        return -1;
    }
    if (ssspMorsel->nextDstScanStartIdx == ssspMorsel->visitedNodes.size() &&
        !ssspMorsel->threadsWritingDstDistances.contains(std::this_thread::get_id())) {
        if (ssspMorsel->threadsWritingDstDistances.empty()) {
            resetSSSPComputationState();
            return -1;
        } else {
            return 0;
        }
    }
    auto sizeToScan = std::min(common::DEFAULT_VECTOR_CAPACITY,
        ssspMorsel->visitedNodes.size() - ssspMorsel->nextDstScanStartIdx);
    auto size = 0u;
    while (size < sizeToScan && ssspMorsel->nextDstScanStartIdx < ssspMorsel->visitedNodes.size()) {
        if (ssspMorsel->visitedNodes[ssspMorsel->nextDstScanStartIdx] == VISITED &&
            ssspMorsel->distance[ssspMorsel->nextDstScanStartIdx] >= ssspMorsel->lowerBound) {
            dstNodeIDVector->setValue<common::nodeID_t>(
                size, common::nodeID_t{ssspMorsel->nextDstScanStartIdx, tableID});
            distanceVector->setValue<int64_t>(
                size, ssspMorsel->distance[ssspMorsel->nextDstScanStartIdx]);
            size++;
        }
        ssspMorsel->nextDstScanStartIdx++;
    }
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        inputFTableSharedState->getTable()->scan(
            vectorsToScan, ssspMorsel->inputFTableTupleIdx, 1 /* numTuples */, colIndicesToScan);
        ssspMorsel->threadsWritingDstDistances.insert(std::this_thread::get_id());
        return size;
    }
    ssspMorsel->threadsWritingDstDistances.erase(std::this_thread::get_id());
    if (ssspMorsel->threadsWritingDstDistances.empty()) {
        resetSSSPComputationState();
        return -1;
    } else {
        return 0;
    }
}

} // namespace processor
} // namespace kuzu
