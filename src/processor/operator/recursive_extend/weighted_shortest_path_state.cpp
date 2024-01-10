#include "processor/operator/recursive_extend/weighted_shortest_path_state.h"

namespace kuzu {
namespace processor {

template<>
void WeightedShortestPathMorsel<false>::markVisited(common::nodeID_t boundNodeID,
    common::nodeID_t nbrNodeID, common::relID_t relID, uint64_t multiplicity) {
    auto boundNodePathCost = offsetPrevPathCost[nextNodeIdxToExtend - 1];
    for (auto i = 0u; i < vectors->recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = vectors->recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto edgeWeight = vectors->recursiveEdgePropertyVector->getValue<int64_t>(pos);
        auto state = visitedNodes[nbrNodeID.offset];
        if ((boundNodePathCost + edgeWeight) < pathCost[nbrNodeID.offset]) {
            if (state == NOT_VISITED_DST || state == VISITED_DST) {
                visitedNodes[nbrNodeID.offset] = VISITED_DST_NEW;
            } else if (state == NOT_VISITED || state == VISITED) {
                visitedNodes[nbrNodeID.offset] = VISITED_NEW;
            }
            pathCost[nbrNodeID.offset] = boundNodePathCost + edgeWeight;
            pathLength[nbrNodeID.offset] = currentLevel + 1;
        }
    }
}

template<>
void WeightedShortestPathMorsel<true>::markVisited(common::nodeID_t boundNodeID,
    common::nodeID_t nbrNodeID, common::relID_t relID, uint64_t multiplicity) {
    // TODO: This will be done later.
}

template<>
void WeightedShortestPathMorsel<false>::addToLocalNextBFSLevel(
    kuzu::processor::RecursiveJoinVectors* vectors_, uint64_t boundNodeMultiplicity,
    unsigned long boundNodeOffset) {
    auto boundNodePathCost = bfsSharedState->offsetPrevPathCost[startScanIdx - 1];
    for (auto i = 0u; i < vectors->recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = vectors->recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto edgeWeight = vectors->recursiveEdgePropertyVector->getValue<int64_t>(pos);
        auto state = bfsSharedState->visitedNodes[nbrNodeID.offset];
        auto nbrNodePathCost = bfsSharedState->pathCost[nbrNodeID.offset];
        if ((boundNodePathCost + edgeWeight) < nbrNodePathCost) {
            /**
             * Main Logic for parallel Bellman Ford:
             * ------------------------------------
             *
             * 3 things need to be atomically updated -
             *
             * (1) if state is VISITED_DST / NOT_VISITED_DST -> update it to VISITED_DST_NEW
             *     if state is VISITED / NOT_VISITED -> update it to VISITED_DST
             *
             * (2) update pathCost[nbrNodeID] to (boundNodePathCost + edgeWeight)
             *
             * (3) update pathLength[nbrNodeID] to (currentLevel + 1)
             *
             * (1) and (3) can be done using ATOMIC_STORE since there is no retrying required.
             * (2) has to be done using a CAS-loop since if a thread failed with the CAS, we need
             * to reread the pathCost[nbrNodeID] again and check if the cost is lesser or not.
             *
             */
            if (state == NOT_VISITED_DST || state == VISITED_DST) {
                uint8_t newState = VISITED_DST_NEW;
                __atomic_store(
                    &bfsSharedState->visitedNodes[nbrNodeID.offset], &newState, __ATOMIC_SEQ_CST);
            } else if (state == NOT_VISITED || state == VISITED) {
                uint8_t newState = VISITED_NEW;
                __atomic_store(
                    &bfsSharedState->visitedNodes[nbrNodeID.offset], &newState, __ATOMIC_SEQ_CST);
            }
            uint8_t newLength = bfsSharedState->currentLevel + 1;
            __atomic_store(
                &bfsSharedState->pathLength[nbrNodeID.offset], &newLength, __ATOMIC_SEQ_CST);
            /**
             * First, confirm that new path cost is lower than already existing path cost.
             * If yes, try to do the CAS operation. If successful, then exit from infinite loop.
             * If CAS is NOT successful, do a read of the path cost that was JUST updated and retry.
             */
            do {
                nbrNodePathCost = bfsSharedState->pathCost[nbrNodeID.offset];
                if (nbrNodePathCost < (boundNodePathCost + edgeWeight)) {
                    break;
                }
            } while (!__sync_bool_compare_and_swap(&bfsSharedState->pathCost[nbrNodeID.offset],
                nbrNodePathCost, (boundNodePathCost + edgeWeight)));
        }
    }
}

template<>
void WeightedShortestPathMorsel<true>::addToLocalNextBFSLevel(
    kuzu::processor::RecursiveJoinVectors* vectors_, uint64_t boundNodeMultiplicity,
    unsigned long boundNodeOffset) {}

template<>
int64_t WeightedShortestPathMorsel<false>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID_, std::pair<uint64_t, int64_t> startScanIdxAndSize,
    kuzu::processor::RecursiveJoinVectors* vectors_) {
    if (isSingleThread) {
        if (prevWriteEndIdx == UINT64_MAX || prevWriteEndIdx == (maxOffset + 1)) {
            return 0;
        }
        auto offset = prevWriteEndIdx;
        auto size = 0u;
        auto dstNodeIDVector = vectors->dstNodeIDVector;
        auto pathLengthVector = vectors->pathLengthVector;
        auto pathCostVector = vectors->pathCostVector;
        while (offset < (maxOffset + 1) && size <= common::DEFAULT_VECTOR_CAPACITY) {
            if ((visitedNodes[offset] == VISITED_DST || visitedNodes[offset] == VISITED_DST_NEW) &&
                pathLength[offset] >= lowerBound) {
                dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{offset, tableID});
                pathLengthVector->setValue<int64_t>(size, pathLength[offset]);
                pathCostVector->setValue<int64_t>(size, pathCost[offset]);
                size++;
            }
            offset++;
        }
        dstNodeIDVector->state->selVector->resetSelectorToUnselectedWithSize(size);
        prevWriteEndIdx = offset;
        return (size > 0);
    } else {
        auto size = 0u;
        auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
        auto dstNodeIDVector = vectors_->dstNodeIDVector;
        auto pathLengthVector = vectors_->pathLengthVector;
        auto pathCostVector = vectors_->pathCostVector;
        while (startScanIdxAndSize.first < endIdx) {
            if ((bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST ||
                    bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW) &&
                bfsSharedState->pathLength[startScanIdxAndSize.first] >=
                    bfsSharedState->lowerBound) {
                dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{startScanIdxAndSize.first, tableID_});
                pathLengthVector->setValue<int64_t>(
                    size, bfsSharedState->pathLength[startScanIdxAndSize.first]);
                pathCostVector->setValue<int64_t>(
                    size, bfsSharedState->pathCost[startScanIdxAndSize.first]);
                size++;
            }
            startScanIdxAndSize.first++;
        }
        if (size > 0) {
            dstNodeIDVector->state->initOriginalAndSelectedSize(size);
            // We need to rescan the FTable to get the source for which the pathLengths were
            // computed. This is because the thread that scanned FTable initially might not be the
            // thread writing the pathLengths to its vector.
            inputFTableSharedState->getTable()->scan(vectorsToScan,
                bfsSharedState->inputFTableTupleIdx, 1 /* numTuples */, colIndicesToScan);
            if (!vectorsToScan[0]->state->isFlat()) {
                vectorsToScan[0]->state->setToFlat();
            }
            return size;
        }
        return 0;
    }
}

template<>
int64_t WeightedShortestPathMorsel<true>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID_, std::pair<uint64_t, int64_t> startScanIdxAndSize,
    kuzu::processor::RecursiveJoinVectors* vectors_) {}

} // namespace processor
} // namespace kuzu
