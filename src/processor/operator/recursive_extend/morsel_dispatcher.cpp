#include "processor/operator/recursive_extend/morsel_dispatcher.h"

#include "chrono"
#include <iostream>
#include <utility>

namespace kuzu {
namespace processor {

/**
 * Main function of the scheduler that dispatches morsels from the FTable between threads. Or it can
 * be called if a thread could not find a morsel from it's local BFSSharedState.
 *
 * @return - TRUE indicates check the combination value of the state pair, FALSE indicates some
 * work was found hence the state's values can be ignored.
 */

std::pair<GlobalSSSPState, SSSPLocalState> MorselDispatcher::getBFSMorsel(
    const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* srcNodeIDVector, BaseBFSState* bfsMorsel,
    common::QueryRelType queryRelType, planner::RecursiveJoinType recursiveJoinType) {
    switch (schedulerType) {
    case common::SchedulerType::OneThreadOneMorsel: {
        mutex.lock();
        auto inputFTableMorsel = inputFTableSharedState->getMorsel();
        if (inputFTableMorsel.numTuples == 0) {
            return {COMPLETE, NO_WORK_TO_SHARE};
        }
        inputFTableSharedState->table->scan(vectorsToScan, inputFTableMorsel.startTupleIdx,
            inputFTableMorsel.numTuples, colIndicesToScan);
        srcNodeIDVector->state->setToFlat();
        bfsMorsel->resetState();
        auto selectedPos = srcNodeIDVector->state->getSelVector().getSelectedPositions();
        auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(selectedPos[0]);
        bfsMorsel->markSrc(nodeID);
        mutex.unlock();
        return {IN_PROGRESS, EXTEND_IN_PROGRESS};
    }
    case common::SchedulerType::nThreadkMorsel: {
        switch (globalState) {
        case COMPLETE: {
            return {COMPLETE, NO_WORK_TO_SHARE};
        }
        case IN_PROGRESS_ALL_SRC_SCANNED: {
            return findAvailableSSSP(bfsMorsel);
        }
        case IN_PROGRESS: {
            if (numActiveBFSSharedState < activeBFSSharedState.size()) {
                mutex.lock();
                if (numActiveBFSSharedState >= activeBFSSharedState.size()) {
                    mutex.unlock();
                    return findAvailableSSSP(bfsMorsel);
                }
                auto inputFTableMorsel = inputFTableSharedState->getMorsel();
                if (inputFTableMorsel.numTuples == 0) {
                    globalState =
                        (numActiveBFSSharedState == 0) ? COMPLETE : IN_PROGRESS_ALL_SRC_SCANNED;
                    mutex.unlock();
                    return {globalState, NO_WORK_TO_SHARE};
                }
                numActiveBFSSharedState++;
                auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                inputFTableSharedState->table->scan(vectorsToScan, inputFTableMorsel.startTupleIdx,
                    inputFTableMorsel.numTuples, colIndicesToScan);
                srcNodeIDVector->state->setToFlat();
                auto selectedPos = srcNodeIDVector->state->getSelVector().getSelectedPositions();
                auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(selectedPos[0]);
                printf("starting a new bfs source, total: %d | src: %lu\n", numActiveBFSSharedState,
                    nodeID.offset);
                uint32_t newSharedStateIdx = UINT32_MAX;
                // Find a position for the new SSSP in the list, there are 2 candidates:
                // 1) the position has a nullptr, add the shared state there
                // 2) the SSSP is marked MORSEL_COMPLETE, it is complete and can be replaced
                for (auto i = 0u; i < activeBFSSharedState.size(); i++) {
                    if (!activeBFSSharedState[i]) {
                        newSharedStateIdx = i;
                        break;
                    }
                    activeBFSSharedState[i]->mutex.lock();
                    if (activeBFSSharedState[i]->isComplete()) {
                        newSharedStateIdx = i;
                        activeBFSSharedState[i]->mutex.unlock();
                        break;
                    }
                    activeBFSSharedState[i]->mutex.unlock();
                }
                if (newSharedStateIdx == UINT32_MAX || !activeBFSSharedState[newSharedStateIdx]) {
                    auto newBFSSharedState =
                        std::make_shared<BFSSharedState>(upperBound, lowerBound, maxOffset);
                    setUpNewBFSSharedState(newBFSSharedState, bfsMorsel, &inputFTableMorsel, nodeID,
                        queryRelType, recursiveJoinType);
                    newBFSSharedState->startTimeInMillis1 = millis;
                    if (newSharedStateIdx != UINT32_MAX) {
                        activeBFSSharedState[newSharedStateIdx] = newBFSSharedState;
                    }
                } else {
                    /// HOLD LOCK here for BFSSharedState being reused. While resetting an existing
                    /// BFSSharedState, only this current Thread should have exclusive access to it.
                    activeBFSSharedState[newSharedStateIdx]->mutex.lock();
                    setUpNewBFSSharedState(activeBFSSharedState[newSharedStateIdx], bfsMorsel,
                        &inputFTableMorsel, nodeID, queryRelType, recursiveJoinType);
                    activeBFSSharedState[newSharedStateIdx]->startTimeInMillis1 = millis;
                    activeBFSSharedState[newSharedStateIdx]->mutex.unlock();
                }
                mutex.unlock();
                return {IN_PROGRESS, EXTEND_IN_PROGRESS};
            } else {
                return findAvailableSSSP(bfsMorsel);
            }
        }
        }
    }
    case common::SchedulerType::nThreadkMorselAdaptive: {
        switch (globalState) {
        case COMPLETE: {
            return {COMPLETE, NO_WORK_TO_SHARE};
        }
        case IN_PROGRESS_ALL_SRC_SCANNED: {
            return findAvailableSSSP(bfsMorsel);
        }
        case IN_PROGRESS: {
            auto state = findAvailableSSSP(bfsMorsel);
            if (state.second != NO_WORK_TO_SHARE) {
                return state;
            }
            if (numActiveBFSSharedState < activeBFSSharedState.size()) {
                mutex.lock();
                if (numActiveBFSSharedState >= activeBFSSharedState.size()) {
                    mutex.unlock();
                    return findAvailableSSSP(bfsMorsel);
                }
                auto inputFTableMorsel = inputFTableSharedState->getMorsel();
                if (inputFTableMorsel.numTuples == 0) {
                    globalState =
                        (numActiveBFSSharedState == 0) ? COMPLETE : IN_PROGRESS_ALL_SRC_SCANNED;
                    mutex.unlock();
                    return {globalState, NO_WORK_TO_SHARE};
                }
                numActiveBFSSharedState++;
                auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                inputFTableSharedState->table->scan(vectorsToScan, inputFTableMorsel.startTupleIdx,
                    inputFTableMorsel.numTuples, colIndicesToScan);
                srcNodeIDVector->state->setToFlat();
                auto selectedPos = srcNodeIDVector->state->getSelVector().getSelectedPositions();
                auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(selectedPos[0]);
                printf("starting a new bfs source, total: %d | src: %lu\n", numActiveBFSSharedState,
                    nodeID.offset);
                uint32_t newSharedStateIdx = UINT32_MAX;
                // Find a position for the new SSSP in the list, there are 2 candidates:
                // 1) the position has a nullptr, add the shared state there
                // 2) the SSSP is marked MORSEL_COMPLETE, it is complete and can be replaced
                for (auto i = 0u; i < activeBFSSharedState.size(); i++) {
                    if (!activeBFSSharedState[i]) {
                        newSharedStateIdx = i;
                        break;
                    }
                    activeBFSSharedState[i]->mutex.lock();
                    if (activeBFSSharedState[i]->isComplete()) {
                        newSharedStateIdx = i;
                        activeBFSSharedState[i]->mutex.unlock();
                        break;
                    }
                    activeBFSSharedState[i]->mutex.unlock();
                }
                if (newSharedStateIdx == UINT32_MAX || !activeBFSSharedState[newSharedStateIdx]) {
                    auto newBFSSharedState =
                        std::make_shared<BFSSharedState>(upperBound, lowerBound, maxOffset);
                    setUpNewBFSSharedState(newBFSSharedState, bfsMorsel, &inputFTableMorsel, nodeID,
                        queryRelType, recursiveJoinType);
                    newBFSSharedState->startTimeInMillis1 = millis;
                    if (newSharedStateIdx != UINT32_MAX) {
                        activeBFSSharedState[newSharedStateIdx] = newBFSSharedState;
                    }
                } else {
                    /// HOLD LOCK here for BFSSharedState being reused. While resetting an existing
                    /// BFSSharedState, only this current Thread should have exclusive access to it.
                    activeBFSSharedState[newSharedStateIdx]->mutex.lock();
                    setUpNewBFSSharedState(activeBFSSharedState[newSharedStateIdx], bfsMorsel,
                        &inputFTableMorsel, nodeID, queryRelType, recursiveJoinType);
                    activeBFSSharedState[newSharedStateIdx]->startTimeInMillis1 = millis;
                    activeBFSSharedState[newSharedStateIdx]->mutex.unlock();
                }
                mutex.unlock();
                return {IN_PROGRESS, EXTEND_IN_PROGRESS};
            } else {
                return findAvailableSSSP(bfsMorsel);
            }
        }
        }
    }
    }
}

void MorselDispatcher::setUpNewBFSSharedState(std::shared_ptr<BFSSharedState>& newBFSSharedState,
    BaseBFSState* bfsMorsel, FTableScanMorsel* inputFTableMorsel, common::nodeID_t nodeID,
    common::QueryRelType queryRelType, planner::RecursiveJoinType recursiveJoinType) {
    newBFSSharedState->reset(bfsMorsel->targetDstNodes, queryRelType, recursiveJoinType);
    newBFSSharedState->inputFTableTupleIdx = inputFTableMorsel->startTupleIdx;
    newBFSSharedState->srcOffset = nodeID.offset;
    newBFSSharedState->markSrc(bfsMorsel->targetDstNodes->contains(nodeID), queryRelType);
    newBFSSharedState->numThreadsBFSActive++;
    auto bfsMorselSize = std::min(common::DEFAULT_VECTOR_CAPACITY,
        newBFSSharedState->bfsLevelNodeOffsets.size() - newBFSSharedState->nextScanStartIdx);
    auto morselScanEndIdx = newBFSSharedState->nextScanStartIdx + bfsMorselSize;
    bfsMorsel->reset(newBFSSharedState->nextScanStartIdx, morselScanEndIdx,
        newBFSSharedState.get());
    newBFSSharedState->nextScanStartIdx += bfsMorselSize;
}

uint32_t MorselDispatcher::getNextAvailableSSSPWork() const {
    uint64_t maxBFSWork = 0u, maxBfsID = UINT64_MAX, maxPathLenWriteWork = 0,
             maxPathLenWriteID = UINT64_MAX;
    for (auto i = 0u; i < activeBFSSharedState.size(); i++) {
        if (activeBFSSharedState[i]) {
            activeBFSSharedState[i]->mutex.lock();
            auto bfsSharedState = activeBFSSharedState[i];
            if (bfsSharedState->ssspLocalState == EXTEND_IN_PROGRESS &&
                bfsSharedState->nextScanStartIdx < bfsSharedState->bfsLevelNodeOffsets.size()) {
                auto frontierSize =
                    bfsSharedState->bfsLevelNodeOffsets.size() - bfsSharedState->nextScanStartIdx;
                auto threadsActive =
                    bfsSharedState->numThreadsBFSActive ? bfsSharedState->numThreadsBFSActive : 1;
                auto work = frontierSize / threadsActive;
                if (work > maxBFSWork) {
                    maxBFSWork = work;
                    maxBfsID = i;
                }
                activeBFSSharedState[i]->mutex.unlock();
                continue;
            } else if (bfsSharedState->ssspLocalState == PATH_LENGTH_WRITE_IN_PROGRESS &&
                       bfsSharedState->nextDstScanStartIdx < bfsSharedState->visitedNodes.size()) {
                auto totalOffsets =
                    bfsSharedState->visitedNodes.size() - bfsSharedState->nextDstScanStartIdx;
                auto threadsActive = !bfsSharedState->pathLengthThreadWriters.empty() ?
                                         bfsSharedState->pathLengthThreadWriters.size() :
                                         1;
                auto work = totalOffsets / threadsActive;
                if (work > maxPathLenWriteWork) {
                    maxPathLenWriteWork = work;
                    maxPathLenWriteID = i;
                }
                activeBFSSharedState[i]->mutex.unlock();
                continue;
            }
            activeBFSSharedState[i]->mutex.unlock();
        }
    }
    if (maxBFSWork > 0) {
        return maxBfsID;
    }
    if (maxPathLenWriteWork > 0) {
        return maxPathLenWriteID;
    }
    return UINT32_MAX;
}

/**
 * Try to find an available BFSSharedState for work and then try to get a morsel. If the work is
 * writing path lengths then the localState value will be PATH_LENGTH_WRITE_IN_PROGRESS.
 */
std::pair<GlobalSSSPState, SSSPLocalState> MorselDispatcher::findAvailableSSSP(
    BaseBFSState* bfsMorsel) {
    std::pair<GlobalSSSPState, SSSPLocalState> state{globalState, NO_WORK_TO_SHARE};
    auto availableSSSPIdx = getNextAvailableSSSPWork();
    if (availableSSSPIdx == UINT32_MAX) {
        return state;
    }
    state.second = activeBFSSharedState[availableSSSPIdx]->getBFSMorsel(bfsMorsel);
    return state;
}

/**
 * Return value = -1: New BFSSharedState can be started.
 * Return value = 0, The pathLengthMorsel received did not yield any value to write to the
 * pathLengthVector.
 * Return value > 0, indicates pathLengths were written to pathLengthVector.
 */
int64_t MorselDispatcher::writeDstNodeIDAndPathLength(
    const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID, std::unique_ptr<BaseBFSState>& baseBfsMorsel,
    RecursiveJoinVectors* vectors) {
    if (baseBfsMorsel->hasMoreToWrite()) {
        auto startScanIdxAndSize = baseBfsMorsel->getPrevDistStartScanIdxAndSize();
        return baseBfsMorsel->writeToVector(inputFTableSharedState, std::move(vectorsToScan),
            std::move(colIndicesToScan), tableID, startScanIdxAndSize, vectors);
    }
    auto bfsSharedState = baseBfsMorsel->bfsSharedState;
    auto startScanIdxAndSize = bfsSharedState->getDstPathLengthMorsel();
    /**
     * [startScanIdx, Size] = [UINT64_MAX, INT64_MAX] = no work in this morsel, exit & return -1
     * [startScanIdx, Size] = [0, -1] = BFSSharedState was just completed (marked MORSEL_COMPLETE)
     * Else a pathLengthMorsel has been allotted to the thread and it can proceed to write the
     * path lengths to the pathLengthVector.
     * For the [0, -1] case, the thread will mark the BFSSharedState as MORSEL_COMPLETE.
     */
    if (startScanIdxAndSize.first == UINT64_MAX && startScanIdxAndSize.second == INT64_MAX) {
        // Don't hold the reference if no work present in this BFS Shared State.
        baseBfsMorsel->bfsSharedState = nullptr;
        return -1;
    }
    if (startScanIdxAndSize.second == -1) {
        /// HOLD LOCKS in this relative order ALWAYS. First the global mutex of morsel dispatcher
        /// and then the local BFSSharedState being marked as MORSEL_COMPLETE. There will be a
        /// deadlock situation if it is not followed since some thread might be trying find work.
        bfsSharedState->freeIntermediatePathData();
        mutex.lock();
        bfsSharedState->mutex.lock();
        numActiveBFSSharedState--;
        bfsSharedState->ssspLocalState = MORSEL_COMPLETE;
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        printf("Source: %lu | BFS time taken: %lu millis | Path / Path-Len write time taken: %lu "
               "millis | Total Active BFS: %d\n",
            bfsSharedState->srcOffset,
            bfsSharedState->startTimeInMillis2 - bfsSharedState->startTimeInMillis1,
            millis - bfsSharedState->startTimeInMillis2, numActiveBFSSharedState);
        bfsSharedState->mutex.unlock();
        // If all SSSP have been completed indicated by count (numActiveBFSSharedState == 0) and no
        // more SSSP are available indicated by state IN_PROGRESS_ALL_SRC_SCANNED then global state
        // can be successfully changed to COMPLETE to indicate completion of SSSP Computation.
        if (numActiveBFSSharedState == 0 && globalState == IN_PROGRESS_ALL_SRC_SCANNED) {
            globalState = COMPLETE;
        }
        mutex.unlock();
        return -1;
    }
    return baseBfsMorsel->writeToVector(inputFTableSharedState, std::move(vectorsToScan),
        std::move(colIndicesToScan), tableID, startScanIdxAndSize, vectors);
}

} // namespace processor
} // namespace kuzu
