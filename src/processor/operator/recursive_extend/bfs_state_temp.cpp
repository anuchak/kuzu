#include "processor/operator/recursive_extend/bfs_state_temp.h"

#include "chrono"

namespace kuzu {
namespace processor {

void SSSPMorsel::reset(std::vector<common::offset_t>& targetDstNodeOffsets) {
    ssspLocalState = MORSEL_EXTEND_IN_PROGRESS;
    currentLevel = 0u;
    nextScanStartIdx = 0u;
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
    std::fill(nodeMask.begin(), nodeMask.end(), 0u);
    bfsLevelNodeOffsets.clear();
    srcOffset = 0u;
    numThreadsActiveOnMorsel = 0u;
    nextDstScanStartIdx = 0u;
    inputFTableTupleIdx = 0u;
    lvlStartTimeInMillis = 0u;
    startTimeInMillis = 0u;
    distWriteStartTimeInMillis = 0u;
}

/*
 * Returning the state here because if SSSPMorsel is complete / in distance writing stage then
 * depending on state we need to take next step. If MORSEL_COMPLETE then proceed to get a
 * new SSSPMorsel & if MORSEL_DISTANCE_WRITING_IN_PROGRESS then help in this task.
 */
SSSPComputationState SSSPMorsel::getBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {
    mutex.lock();
    if (ssspLocalState == MORSEL_COMPLETE || ssspLocalState == MORSEL_DISTANCE_WRITE_IN_PROGRESS) {
        bfsMorsel->threadCheckSSSPState = true;
        mutex.unlock();
        return ssspLocalState;
    }
    if (nextScanStartIdx < bfsLevelNodeOffsets.size()) {
        numThreadsActiveOnMorsel++;
        auto bfsMorselSize = std::min(
            common::DEFAULT_VECTOR_CAPACITY, bfsLevelNodeOffsets.size() - nextScanStartIdx);
        auto morselScanEndIdx = nextScanStartIdx + bfsMorselSize;
        bfsMorsel->reset(nextScanStartIdx, morselScanEndIdx, this);
        nextScanStartIdx += bfsMorselSize;
        mutex.unlock();
        return ssspLocalState;
    }
    bfsMorsel->threadCheckSSSPState = true;
    mutex.unlock();
    return ssspLocalState;
}

bool SSSPMorsel::finishBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {
    mutex.lock();
    numThreadsActiveOnMorsel--;
    if (ssspLocalState != MORSEL_EXTEND_IN_PROGRESS) {
        mutex.unlock();
        return true;
    }
    // Update the destinations visited, used to check for termination condition.
    numVisitedNodes += bfsMorsel->localVisitedDstNodes;
    if (numThreadsActiveOnMorsel == 0 && nextScanStartIdx == bfsLevelNodeOffsets.size()) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        auto size = bfsLevelNodeOffsets.size();
        moveNextLevelAsCurrentLevel();
        lvlStartTimeInMillis = millis;
        if (isComplete(bfsMorsel->getNumDstNodeOffsets())) {
            ssspLocalState = MORSEL_DISTANCE_WRITE_IN_PROGRESS;
            mutex.unlock();
            return true;
        }
    } else if (isComplete(bfsMorsel->getNumDstNodeOffsets())) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        ssspLocalState = MORSEL_DISTANCE_WRITE_IN_PROGRESS;
        mutex.unlock();
        return true;
    }
    mutex.unlock();
    return false;
}

bool SSSPMorsel::isComplete(uint64_t numDstNodesToVisit) {
    if (bfsLevelNodeOffsets.empty()) { // no more to extend.
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
    bfsLevelNodeOffsets.push_back(srcOffset);
}

void SSSPMorsel::moveNextLevelAsCurrentLevel() {
    currentLevel++;
    nextScanStartIdx = 0u;
    auto duration2 = std::chrono::system_clock::now().time_since_epoch();
    auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
    bfsLevelNodeOffsets.clear();
    if (currentLevel < upperBound) { // No need to prepare this vector if we won't extend.
        for (auto i = 0u; i < nodeMask.size(); i++) {
            if (nodeMask[i]) {
                bfsLevelNodeOffsets.push_back(i);
            }
        }
    }
    std::fill(nodeMask.begin(), nodeMask.end(), 0u);
    auto duration3 = std::chrono::system_clock::now().time_since_epoch();
    auto millis3 = std::chrono::duration_cast<std::chrono::milliseconds>(duration3).count();
    printf("Time taken to prepare: %lu nodes is: %lu\n", bfsLevelNodeOffsets.size(),
        millis3 - millis2);
}

std::pair<uint64_t, int64_t> SSSPMorsel::getDstDistanceMorsel() {
    mutex.lock();
    if (ssspLocalState != MORSEL_DISTANCE_WRITE_IN_PROGRESS) {
        mutex.unlock();
        return {UINT64_MAX, INT64_MAX};
    } else if (nextDstScanStartIdx == visitedNodes.size()) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        printf("SSSP with source: %lu took: %lu ms to write distances\n", srcOffset,
            millis - distWriteStartTimeInMillis);
        ssspLocalState = MORSEL_COMPLETE;
        mutex.unlock();
        return {0, -1};
    }
    if (nextDstScanStartIdx == 0u) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        distWriteStartTimeInMillis =
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }
    auto sizeToScan =
        std::min(common::DEFAULT_VECTOR_CAPACITY, visitedNodes.size() - nextDstScanStartIdx);
    std::pair<uint64_t, uint32_t> startScanIdxAndSize = {nextDstScanStartIdx, sizeToScan};
    nextDstScanStartIdx += sizeToScan;
    mutex.unlock();
    return startScanIdxAndSize;
}

common::offset_t BaseBFSMorsel::getNextNodeOffset() {
    if (startScanIdx == endScanIdx) {
        return common::INVALID_OFFSET;
    }
    return ssspMorsel->bfsLevelNodeOffsets[startScanIdx++];
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
                ssspMorsel->nodeMask[nodeID.offset] = 1;
                localVisitedDstNodes++;
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(
                    &ssspMorsel->visitedNodes[nodeID.offset], state, VISITED)) {
                ssspMorsel->nodeMask[nodeID.offset] = 1;
            }
        }
    }
}

uint32_t MorselDispatcher::getThreadIdx() {
    std::unique_lock lck{mutex};
    activeSSSPMorsel[threadIdxCounter] =
        std::make_shared<SSSPMorsel>(upperBound, lowerBound, maxOffset);
    return threadIdxCounter++;
}

// Not thread safe, called only for initialization of BFSMorsel. ThreadIdx position is fixed.
SSSPMorsel* MorselDispatcher::getSSSPMorsel(uint32_t threadIdx) {
    return activeSSSPMorsel[threadIdx].get();
}

SSSPComputationState MorselDispatcher::getBFSMorsel(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
    std::unique_ptr<BaseBFSMorsel>& bfsMorsel, uint32_t threadIdx) {
    std::unique_lock lck{mutex};
    switch (globalState) {
    case COMPLETE:
        bfsMorsel->threadCheckSSSPState = true;
        return globalState;
    case IN_PROGRESS_ALL_SRC_SCANNED: {
        auto ssspMorsel = activeSSSPMorsel[threadIdx];
        auto ret = ssspMorsel->getBFSMorsel(bfsMorsel);
        if (bfsMorsel->threadCheckSSSPState) {
            switch (ret) {
                // try to get the next available SSSPMorsel for work
                // same for both states, try to get next available SSSPMorsel for work
            case MORSEL_COMPLETE:
            case MORSEL_EXTEND_IN_PROGRESS: {
                auto nextAvailableSSSPIdx = getNextAvailableSSSPWork(threadIdx);
                if (nextAvailableSSSPIdx == -1) {
                    globalState = (numActiveSSSP == 0) ? COMPLETE : IN_PROGRESS_ALL_SRC_SCANNED;
                    bfsMorsel->threadCheckSSSPState = true;
                    return globalState;
                }
                if (activeSSSPMorsel[nextAvailableSSSPIdx]->getBFSMorsel(bfsMorsel)) {
                    ssspMorsel->mutex.lock();
                    activeSSSPMorsel[threadIdx] = activeSSSPMorsel[nextAvailableSSSPIdx];
                    printf("thread: %d working on SSSP src %lu now \n", threadIdx, ssspMorsel->srcOffset);
                    ssspMorsel->mutex.unlock();
                    return globalState;
                }
                bfsMorsel->threadCheckSSSPState = true;
                return globalState;
            }
            case MORSEL_DISTANCE_WRITE_IN_PROGRESS:
                // TODO: try to help the distance writing for SSSPMorsel (need to check this state)
                bfsMorsel->threadCheckSSSPState = true;
                return globalState;
            default:
                assert(false);
            }
        } else {
            return globalState;
        }
    }
    case IN_PROGRESS: {
        auto ssspMorsel = activeSSSPMorsel[threadIdx];
        auto ret = ssspMorsel->getBFSMorsel(bfsMorsel);
        if (bfsMorsel->threadCheckSSSPState) {
            switch (ret) {
                // try to get the next available SSSPMorsel for work
                // same for both states, try to get next available SSSPMorsel for work
            case MORSEL_COMPLETE:
            case MORSEL_EXTEND_IN_PROGRESS: {
                if (numActiveSSSP < activeSSSPMorsel.size()) {
                    auto inputFTableMorsel = inputFTableSharedState->getMorsel(1);
                    if (inputFTableMorsel->numTuples == 0) {
                        globalState = (numActiveSSSP == 0) ? COMPLETE : IN_PROGRESS_ALL_SRC_SCANNED;
                        bfsMorsel->threadCheckSSSPState = true;
                        printf("changed state to: %d\n", globalState);
                        return globalState;
                    }
                    numActiveSSSP++;
                    auto newSSSPMorsel =
                        std::make_shared<SSSPMorsel>(upperBound, lowerBound, maxOffset);
                    auto duration = std::chrono::system_clock::now().time_since_epoch();
                    auto millis =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    inputFTableSharedState->getTable()->scan(vectorsToScan,
                        inputFTableMorsel->startTupleIdx, inputFTableMorsel->numTuples,
                        colIndicesToScan);
                    newSSSPMorsel->reset(bfsMorsel->targetDstNodeOffsets);
                    newSSSPMorsel->startTimeInMillis = millis;
                    newSSSPMorsel->lvlStartTimeInMillis = millis;
                    newSSSPMorsel->inputFTableTupleIdx = inputFTableMorsel->startTupleIdx;
                    auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
                        srcNodeIDVector->state->selVector->selectedPositions[0]);
                    newSSSPMorsel->srcOffset = nodeID.offset;
                    newSSSPMorsel->markSrc(bfsMorsel->targetDstNodeOffsets);
                    newSSSPMorsel->getBFSMorsel(bfsMorsel);
                    printf("no. of SSSP increased by 1: %lu, src offset: %lu started by thread: %d\n", numActiveSSSP, nodeID.offset, threadIdx);
                    ssspMorsel->mutex.lock();
                    activeSSSPMorsel[threadIdx] = newSSSPMorsel;
                    ssspMorsel->mutex.unlock();
                    return globalState;
                } else {
                    auto nextAvailableSSSPIdx = getNextAvailableSSSPWork(threadIdx);
                    if (nextAvailableSSSPIdx == -1) {
                        bfsMorsel->threadCheckSSSPState = true;
                        return globalState;
                    }
                    if (activeSSSPMorsel[nextAvailableSSSPIdx]->getBFSMorsel(bfsMorsel)) {
                        activeSSSPMorsel[threadIdx] = activeSSSPMorsel[nextAvailableSSSPIdx];
                        return globalState;
                    }
                    bfsMorsel->threadCheckSSSPState = true;
                    return globalState;
                }
            }
            case MORSEL_DISTANCE_WRITE_IN_PROGRESS:
                // TODO: try to help the distance writing for SSSPMorsel (need to check this state)
                bfsMorsel->threadCheckSSSPState = true;
                return globalState;
            default:
                assert(false);
            }
        } else {
            return globalState;
        }
    }
    default:
        assert(false);
    }
    return globalState;
}

int64_t MorselDispatcher::getNextAvailableSSSPWork(uint32_t threadIdx) {
    auto nextAvailableSSSPIdx = -1;
    for (int i = 0; i < activeSSSPMorsel.size(); i++) {
        if (activeSSSPMorsel[i]) {
            activeSSSPMorsel[i]->mutex.lock();
            if (activeSSSPMorsel[i]->nextScanStartIdx <
                    activeSSSPMorsel[i]->bfsLevelNodeOffsets.size() &&
                activeSSSPMorsel[i]->ssspLocalState == MORSEL_EXTEND_IN_PROGRESS) {
                nextAvailableSSSPIdx = i;
                activeSSSPMorsel[i]->mutex.unlock();
                break;
            }
            activeSSSPMorsel[i]->mutex.unlock();
        }
    }
    return nextAvailableSSSPIdx;
}

/*
 * If return value = -1, it indicates new SSSPMorsel can be started.
 * If return value = 0, indicates no more distances to write BUT cannot start new SSSPMorsel.
 * If return value > 0, indicates distances were written to distanceVector.
 */
int64_t MorselDispatcher::writeDstNodeIDAndDistance(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    const std::shared_ptr<common::ValueVector>& dstNodeIDVector,
    const std::shared_ptr<common::ValueVector>& distanceVector, common::table_id_t tableID,
    uint32_t threadIdx) {
    mutex.lock();
    auto& ssspMorsel = activeSSSPMorsel[threadIdx];
    auto startScanIdxAndSize = ssspMorsel->getDstDistanceMorsel();
    if (startScanIdxAndSize.first == UINT64_MAX && startScanIdxAndSize.second == INT64_MAX) {
        mutex.unlock();
        return -1;
    } else if (startScanIdxAndSize.second == -1) {
        numActiveSSSP--;
        if (numActiveSSSP == 0 && globalState == IN_PROGRESS_ALL_SRC_SCANNED) {
            globalState = COMPLETE;
        }
        mutex.unlock();
        return 0;
    }
    mutex.unlock();
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    while (startScanIdxAndSize.first < endIdx) {
        if (ssspMorsel->visitedNodes[startScanIdxAndSize.first] == VISITED_DST &&
            ssspMorsel->distance[startScanIdxAndSize.first] >= ssspMorsel->lowerBound) {
            dstNodeIDVector->setValue<common::nodeID_t>(
                size, common::nodeID_t{startScanIdxAndSize.first, tableID});
            distanceVector->setValue<int64_t>(
                size, ssspMorsel->distance[startScanIdxAndSize.first]);
            size++;
        }
        startScanIdxAndSize.first++;
    }
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        inputFTableSharedState->getTable()->scan(
            vectorsToScan, ssspMorsel->inputFTableTupleIdx, 1 /* numTuples */, colIndicesToScan);
        return size;
    }
    return 0;
}

} // namespace processor
} // namespace kuzu
