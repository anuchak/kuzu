#include "processor/operator/recursive_extend/bfs_state.h"

#include "chrono"

namespace kuzu {
namespace processor {

void SSSPMorsel::reset(std::vector<common::offset_t>& targetDstNodeOffsets) {
    currentLevel = 0u;
    nextScanStartIdx = 0u;
    curBFSLevel->resetState();
    nextBFSLevel->resetState();
    numVisitedNodes = 0u;
    if (targetDstNodeOffsets.size() == maxOffset || targetDstNodeOffsets.empty()) {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
    } else {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED);
        for (auto& dstOffset : targetDstNodeOffsets) {
            visitedNodes[dstOffset] = NOT_VISITED_DST;
        }
    }
    std::fill(distance.begin(), distance.end(), 0u);
    srcOffset = 0u;
    numThreadsActiveOnMorsel = 0u;
    nextDstScanStartIdx = 0u;
    inputFTableTupleIdx = 0u;
    lvlStartTimeInMillis = 0u;
    startTimeInMillis = 0u;
    distWriteStartTimeInMillis = 0u;
    threadsWritingDstDistances.clear();
}

bool SSSPMorsel::isComplete(uint64_t numDstNodesToVisit) {
    if (curBFSLevel->nodeOffsets.empty()) { // no more to extend.
        return true;
    }
    if (currentLevel == upperBound) { // upper limit reached.
        return true;
    }
    if (numVisitedNodes == numDstNodesToVisit) { // all destinations have been reached.
        return true;
    }
    return false;
}

void SSSPMorsel::markSrc(const std::vector<common::offset_t>& targetDstNodeOffsets) {
    if (std::find(targetDstNodeOffsets.begin(), targetDstNodeOffsets.end(), srcOffset) !=
        targetDstNodeOffsets.end()) {
        visitedNodes[srcOffset] = VISITED_DST;
        numVisitedNodes++;
        distance[srcOffset] = 0;
    } else {
        visitedNodes[srcOffset] = VISITED;
    }
    curBFSLevel->nodeOffsets.push_back(srcOffset);
}

void SSSPMorsel::moveNextLevelAsCurrentLevel() {
    curBFSLevel = std::move(nextBFSLevel);
    currentLevel++;
    nextBFSLevel = std::make_unique<Frontier>();
    nextScanStartIdx = 0u;
    if (currentLevel < upperBound) { // No need to sort if we are not extending further.
        std::sort(curBFSLevel->nodeOffsets.begin(), curBFSLevel->nodeOffsets.end());
    }
}

common::offset_t BaseBFSMorsel::getNextNodeOffset() {
    if (startScanIdx == endScanIdx) {
        return common::INVALID_OFFSET;
    }
    return ssspMorsel->curBFSLevel->nodeOffsets[startScanIdx++];
}

void BaseBFSMorsel::addToLocalNextBFSLevel(
    const std::shared_ptr<common::ValueVector>& tmpDstNodeIDVector) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = ssspMorsel->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(
                    &ssspMorsel->visitedNodes[nodeID.offset], state, VISITED_DST)) {
                ssspMorsel->distance[nodeID.offset] = ssspMorsel->currentLevel + 1;
                localNextBFSLevel->nodeOffsets.push_back(nodeID.offset);
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(
                    &ssspMorsel->visitedNodes[nodeID.offset], state, VISITED)) {
                localNextBFSLevel->nodeOffsets.push_back(nodeID.offset);
            }
        }
    }
}

bool MorselDispatcher::finishBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {
    std::unique_lock lck{mutex};
    ssspMorsel->numThreadsActiveOnMorsel--;
    if (ssspMorsel->currentLevel > 0) {
        ssspMorsel->numVisitedNodes += bfsMorsel->localNextBFSLevel->nodeOffsets.size();
    }
    ssspMorsel->nextBFSLevel->nodeOffsets.insert(ssspMorsel->nextBFSLevel->nodeOffsets.end(),
        bfsMorsel->localNextBFSLevel->nodeOffsets.begin(),
        bfsMorsel->localNextBFSLevel->nodeOffsets.end());
    if (ssspMorsel->numThreadsActiveOnMorsel == 0 &&
        ssspMorsel->nextScanStartIdx == ssspMorsel->curBFSLevel->size()) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        printf("%lu total nodes in level: %d finished in %lu ms\n",
            ssspMorsel->curBFSLevel->nodeOffsets.size(), ssspMorsel->currentLevel,
            millis - ssspMorsel->lvlStartTimeInMillis);
        ssspMorsel->lvlStartTimeInMillis = millis;
        ssspMorsel->moveNextLevelAsCurrentLevel();
        if (ssspMorsel->isComplete(bfsMorsel->getNumDstNodeOffsets())) {
            state = SSSP_MORSEL_COMPLETE;
            return true;
        }
    } else if (ssspMorsel->isComplete(bfsMorsel->getNumDstNodeOffsets())) {
        state = SSSP_MORSEL_COMPLETE;
        return true;
    }
    return false;
}

SSSPComputationState MorselDispatcher::getBFSMorsel(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
    std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {
    std::unique_lock lck{mutex};
    if (state == SSSP_COMPUTATION_COMPLETE || state == SSSP_MORSEL_COMPLETE) {
        bfsMorsel->threadCheckSSSPState = true;
        return state;
    }
    if (ssspMorsel->isComplete(bfsMorsel->getNumDstNodeOffsets()) &&
        ssspMorsel->numThreadsActiveOnMorsel == 0) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        auto inputFTableMorsel = inputFTableSharedState->getMorsel(1);
        // Marks end of SSSP Computation, no more source tuples to trigger SSSP.
        if (inputFTableMorsel->numTuples == 0) {
            printf("SSSP with src: %lu completed in: %lu ms\n", ssspMorsel->srcOffset,
                millis - ssspMorsel->startTimeInMillis);
            state = SSSP_COMPUTATION_COMPLETE;
            bfsMorsel->threadCheckSSSPState = true;
            return state;
        }
        inputFTableSharedState->getTable()->scan(vectorsToScan, inputFTableMorsel->startTupleIdx,
            inputFTableMorsel->numTuples, colIndicesToScan);
        if (ssspMorsel->startTimeInMillis != 0u) {
            printf("SSSP with src: %lu completed in: %lu ms\n", ssspMorsel->srcOffset,
                millis - ssspMorsel->startTimeInMillis);
        }
        ssspMorsel->reset(bfsMorsel->targetDstNodeOffsets);
        ssspMorsel->startTimeInMillis = millis;
        ssspMorsel->inputFTableTupleIdx = inputFTableMorsel->startTupleIdx;
        auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
            srcNodeIDVector->state->selVector->selectedPositions[0]);
        ssspMorsel->srcOffset = nodeID.offset;
        ssspMorsel->markSrc(bfsMorsel->targetDstNodeOffsets);
    }
    // We don't swap here, because we want the last thread to complete its BFSMorsel and only then
    // swap the curBFSLevel and nextBFSLevel (happens in Line 93 in finishBFSMorsel function).
    if (ssspMorsel->nextScanStartIdx == ssspMorsel->curBFSLevel->size()) {
        bfsMorsel->threadCheckSSSPState = true;
        // exit and sleep for some time
        return state;
    }
    if (ssspMorsel->nextScanStartIdx == 0u) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        ssspMorsel->lvlStartTimeInMillis = millis;
    }
    ssspMorsel->numThreadsActiveOnMorsel++;
    auto bfsMorselSize = std::min(common::DEFAULT_VECTOR_CAPACITY,
        ssspMorsel->curBFSLevel->size() - ssspMorsel->nextScanStartIdx);
    auto morselScanEndIdx = ssspMorsel->nextScanStartIdx + bfsMorselSize;
    bfsMorsel->reset(ssspMorsel->nextScanStartIdx, morselScanEndIdx, ssspMorsel.get());
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
    if (ssspMorsel->nextDstScanStartIdx == 0u) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        ssspMorsel->distWriteStartTimeInMillis =
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }
    auto sizeToScan = std::min(common::DEFAULT_VECTOR_CAPACITY,
        ssspMorsel->visitedNodes.size() - ssspMorsel->nextDstScanStartIdx);
    auto size = 0u;
    while (size < sizeToScan && ssspMorsel->nextDstScanStartIdx < ssspMorsel->visitedNodes.size()) {
        if (ssspMorsel->visitedNodes[ssspMorsel->nextDstScanStartIdx] == VISITED_DST &&
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
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        printf("SSSP with source: %lu took: %lu ms to write distances\n", ssspMorsel->srcOffset,
            millis - ssspMorsel->distWriteStartTimeInMillis);
        resetSSSPComputationState();
        return -1;
    } else {
        return 0;
    }
}

} // namespace processor
} // namespace kuzu
