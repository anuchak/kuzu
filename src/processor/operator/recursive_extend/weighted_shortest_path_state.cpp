#include "processor/operator/recursive_extend/weighted_shortest_path_state.h"

#pragma clang diagnostic push
#pragma ide diagnostic ignored "UnreachableCode"
namespace kuzu {
namespace processor {

template<>
void WeightedShortestPathMorsel<false>::markVisited(common::nodeID_t boundNodeID,
    common::nodeID_t nbrNodeID, common::relID_t relID, uint64_t multiplicity) {
    auto boundNodePathCost = pathCost[boundNodeID.offset];
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
            wpriority_queue.push(
                std::pair<uint64_t, uint64_t>(boundNodePathCost + edgeWeight, nbrNodeID.offset));
            pathCost[nbrNodeID.offset] = boundNodePathCost + edgeWeight;
            pathLength[nbrNodeID.offset] = pathLength[boundNodeID.offset] + 1;
        }
    }
}

template<>
void WeightedShortestPathMorsel<true>::markVisited(common::nodeID_t boundNodeID,
    common::nodeID_t nbrNodeID, common::relID_t relID, uint64_t multiplicity) {
    auto boundNodePathCost = offsetPrevPathCost[nextNodeIdxToExtend - 1];
    for (auto i = 0u; i < vectors->recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = vectors->recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        relID = vectors->recursiveEdgeIDVector->getValue<common::nodeID_t>(pos);
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
            srcAndEdgeOffset[nbrNodeID.offset] = {boundNodeID.offset, relID.offset};
        }
        if (edgeTableID == UINT64_MAX) {
            edgeTableID = relID.tableID;
        }
    }
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
                    &bfsSharedState->visitedNodes[nbrNodeID.offset], &newState, __ATOMIC_RELAXED);
            } else if (state == NOT_VISITED || state == VISITED) {
                uint8_t newState = VISITED_NEW;
                __atomic_store(
                    &bfsSharedState->visitedNodes[nbrNodeID.offset], &newState, __ATOMIC_RELAXED);
            }
            uint8_t newLength = bfsSharedState->currentLevel + 1;
            __atomic_store(
                &bfsSharedState->pathLength[nbrNodeID.offset], &newLength, __ATOMIC_RELAXED);
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
    unsigned long boundNodeOffset) {
    auto boundNodePathCost = bfsSharedState->offsetPrevPathCost[startScanIdx - 1];
    for (auto i = 0u; i < vectors->recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = vectors->recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto edgeWeight = vectors->recursiveEdgePropertyVector->getValue<int64_t>(pos);
        auto relID = vectors->recursiveEdgeIDVector->getValue<common::relID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nbrNodeID.offset];
        auto nbrNodeVal = bfsSharedState->pathCostAndSrc[nbrNodeID.offset];
        auto nbrNodePathCost = (int64_t)nbrNodeVal;
        if ((boundNodePathCost + edgeWeight) < nbrNodePathCost) {
            /**
             * Main Logic for parallel Bellman Ford:
             * ------------------------------------
             *
             * 4 things need to be atomically updated -
             *
             * (1) if state is VISITED_DST / NOT_VISITED_DST -> update it to VISITED_DST_NEW
             *     if state is VISITED / NOT_VISITED -> update it to VISITED_DST
             *
             * (2) update path cost of nbrNodeID to (boundNodePathCost + edgeWeight)
             *
             * (3) update pathLength[nbrNodeID] to (currentLevel + 1)
             *
             * (4) update nbrNode's parent and edge offset for cheapest path
             *
             * (1) and (3) can be done using ATOMIC_STORE since there is no retrying required.
             *
             * (2) & (4) have to be done atomically, the cost associated along with the parent &
             * edge offset associated - all have to change atomically *at the same time*.
             *
             * To do this the __sync_bool_compare_and_swap_16 primitive is being used which is
             * Double Width Compare & Swap (DWCAS)
             *
             */
            if (state == NOT_VISITED_DST || state == VISITED_DST) {
                uint8_t newState = VISITED_DST_NEW;
                __atomic_store(
                    &bfsSharedState->visitedNodes[nbrNodeID.offset], &newState, __ATOMIC_RELAXED);
            } else if (state == NOT_VISITED || state == VISITED) {
                uint8_t newState = VISITED_NEW;
                __atomic_store(
                    &bfsSharedState->visitedNodes[nbrNodeID.offset], &newState, __ATOMIC_RELAXED);
            }
            uint8_t newLength = bfsSharedState->currentLevel + 1;
            __atomic_store(
                &bfsSharedState->pathLength[nbrNodeID.offset], &newLength, __ATOMIC_RELAXED);
            /**
             * First, confirm that new path cost is lower than already existing path cost.
             * If yes, try to do the CAS operation. If successful, then exit from infinite loop.
             * If CAS is NOT successful, do a read of the path cost that was JUST updated and retry.
             */
            auto newSrc =
                new std::pair<common::offset_t, common::offset_t>(boundNodeOffset, relID.offset);
            while (true) {
                nbrNodeVal = bfsSharedState->pathCostAndSrc[nbrNodeID.offset];
                nbrNodePathCost = (int64_t)(nbrNodeVal);
                if (nbrNodePathCost < (boundNodePathCost + edgeWeight)) {
                    delete newSrc;
                    break;
                }
                // VERY IMP: Cast the path cost to unsigned int64_t first, prevents setting the
                // 128th bit as `1` if the path cost is -ve.
                uint64_t bottom64Bits = (boundNodePathCost + edgeWeight);
                unsigned __int128 top64Bits = ((unsigned __int128)newSrc) << 64;
                unsigned __int128 newNbrNodeVal = top64Bits | bottom64Bits;
                if (__sync_bool_compare_and_swap_16(
                        &bfsSharedState->pathCostAndSrc[nbrNodeID.offset], nbrNodeVal,
                        newNbrNodeVal)) {
                    intermediateSrcEdgeOffset.push_back(newSrc);
                    break;
                }
            }
        }
        if (bfsSharedState->edgeTableID == UINT64_MAX) {
            bfsSharedState->edgeTableID = relID.tableID;
        }
    }
}

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
    kuzu::processor::RecursiveJoinVectors* vectors_) {
    if (isSingleThread) {
        if (prevWriteEndIdx == UINT64_MAX || prevWriteEndIdx == (maxOffset + 1)) {
            return 0;
        }
        auto offset = prevWriteEndIdx;
        auto size = 0u, nodeIDDataVectorPos = 0u, relIDDataVectorPos = 0u;
        auto dstNodeIDVector = vectors->dstNodeIDVector;
        auto pathLengthVector = vectors->pathLengthVector;
        auto pathCostVector = vectors->pathCostVector;
        if (vectors->pathVector != nullptr) {
            vectors->pathVector->resetAuxiliaryBuffer();
        }
        uint8_t length;
        auto nodeBuffer = std::vector<common::offset_t>(31u);
        auto relBuffer = std::vector<common::offset_t>(31u);
        while (offset < (maxOffset + 1) && size <= common::DEFAULT_VECTOR_CAPACITY) {
            if ((visitedNodes[offset] == VISITED_DST || visitedNodes[offset] == VISITED_DST_NEW) &&
                pathLength[offset] >= lowerBound) {
                length = pathLength[offset];
                auto nodeEntry = common::ListVector::addList(vectors->pathNodesVector, length - 1);
                auto relEntry = common::ListVector::addList(vectors->pathRelsVector, length);
                vectors->pathNodesVector->setValue(size, nodeEntry);
                vectors->pathRelsVector->setValue(size, relEntry);
                vectors->dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{offset, tableID});
                vectors->pathLengthVector->setValue<int64_t>(size, length);
                vectors->pathCostVector->setValue<int64_t>(size, pathCost[offset]);
                size++;
                auto entry = srcAndEdgeOffset[offset];
                auto idx = 0u;
                nodeBuffer[length - idx] = offset;
                while (entry.first != UINT64_MAX && entry.second != UINT64_MAX && (length > idx)) {
                    relBuffer[length - idx - 1] = entry.second;
                    nodeBuffer[length - ++idx] = entry.first;
                    entry = srcAndEdgeOffset[entry.first];
                }
                for (auto i = 1u; i < length; i++) {
                    vectors->pathNodesIDDataVector->setValue<common::nodeID_t>(
                        nodeIDDataVectorPos++, common::nodeID_t{nodeBuffer[i], tableID});
                }
                for (auto i = 0u; i < length; i++) {
                    vectors->pathRelsSrcIDDataVector->setValue<common::nodeID_t>(
                        relIDDataVectorPos, common::nodeID_t{nodeBuffer[i], tableID});
                    vectors->pathRelsIDDataVector->setValue<common::relID_t>(
                        relIDDataVectorPos, common::relID_t{relBuffer[i], edgeTableID});
                    vectors->pathRelsDstIDDataVector->setValue<common::nodeID_t>(
                        relIDDataVectorPos++, common::nodeID_t{nodeBuffer[i + 1], tableID});
                }
            }
            offset++;
        }
        dstNodeIDVector->state->selVector->resetSelectorToUnselectedWithSize(size);
        prevWriteEndIdx = offset;
        return (size > 0);
    } else {
        auto size = 0u, nodeIDDataVectorPos = 0u, relIDDataVectorPos = 0u;
        auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
        if (vectors->pathVector != nullptr) {
            vectors->pathVector->resetAuxiliaryBuffer();
        }
        auto nodeBuffer = std::vector<common::offset_t>(31u);
        auto relBuffer = std::vector<common::offset_t>(31u);
        while (startScanIdxAndSize.first < endIdx) {
            if ((bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST ||
                    bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW) &&
                bfsSharedState->pathLength[startScanIdxAndSize.first] >=
                    bfsSharedState->lowerBound) {
                auto length = bfsSharedState->pathLength[startScanIdxAndSize.first];
                auto nodeEntry = common::ListVector::addList(vectors->pathNodesVector, length - 1);
                auto relEntry = common::ListVector::addList(vectors->pathRelsVector, length);
                vectors->pathNodesVector->setValue(size, nodeEntry);
                vectors->pathRelsVector->setValue(size, relEntry);
                vectors->dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{startScanIdxAndSize.first, tableID_});
                vectors->pathLengthVector->setValue<int64_t>(size, length);
                auto entry = bfsSharedState->pathCostAndSrc[startScanIdxAndSize.first];
                auto pathCostVal = (int64_t)entry;
                vectors->pathCostVector->setValue<int64_t>(size, pathCostVal);
                auto offsetSrc = (std::pair<common::offset_t, common::offset_t>*)(entry >> 64);
                auto idx = 0u;
                nodeBuffer[length - idx] = startScanIdxAndSize.first;
                while (offsetSrc->first != UINT64_MAX && offsetSrc->second != UINT64_MAX &&
                       (length > idx)) {
                    relBuffer[length - idx - 1] = offsetSrc->second;
                    nodeBuffer[length - ++idx] = offsetSrc->first;
                    entry = bfsSharedState->pathCostAndSrc[offsetSrc->first];
                    offsetSrc = (std::pair<common::offset_t, common::offset_t>*)(entry >> 64);
                }
                for (auto i = 1u; i < length; i++) {
                    vectors->pathNodesIDDataVector->setValue<common::nodeID_t>(
                        nodeIDDataVectorPos++, common::nodeID_t{nodeBuffer[i], tableID_});
                }
                for (auto i = 0u; i < length; i++) {
                    vectors->pathRelsSrcIDDataVector->setValue<common::nodeID_t>(
                        relIDDataVectorPos, common::nodeID_t{nodeBuffer[i], tableID_});
                    vectors->pathRelsIDDataVector->setValue<common::relID_t>(relIDDataVectorPos,
                        common::relID_t{relBuffer[i], bfsSharedState->edgeTableID});
                    vectors->pathRelsDstIDDataVector->setValue<common::nodeID_t>(
                        relIDDataVectorPos++, common::nodeID_t{nodeBuffer[i + 1], tableID_});
                }
                size++;
            }
            startScanIdxAndSize.first++;
        }
        if (size > 0) {
            vectors->dstNodeIDVector->state->initOriginalAndSelectedSize(size);
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

} // namespace processor
} // namespace kuzu

#pragma clang diagnostic pop