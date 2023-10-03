#include "processor/operator/recursive_extend/variable_length_state.h"

namespace kuzu {
namespace processor {

template<>
void VariableLengthMorsel<false>::addToLocalNextBFSLevel(
    RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity, unsigned long boundNodeOffset) {
    auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
    for (auto i = 0u; i < recursiveDstNodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST || state == VISITED_DST) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_DST_NEW);
        } else if (state == NOT_VISITED || state == VISITED) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_NEW);
        }
        auto member = bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset];
        if (!member) {
            auto entry = new multiplicityAndLevel(
                boundNodeMultiplicity, bfsSharedState->currentLevel + 1, nullptr);
            if (__sync_bool_compare_and_swap(
                    &bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset], member, entry)) {
                // no need to do anything, current thread was successful
            } else {
                delete entry;
                member = bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset];
                member->multiplicity.fetch_add(boundNodeMultiplicity, std::memory_order_relaxed);
            }
        } else {
            if (member->bfsLevel != bfsSharedState->currentLevel + 1) {
                auto entry = new multiplicityAndLevel(
                    boundNodeMultiplicity, bfsSharedState->currentLevel + 1, member);
                if (__sync_bool_compare_and_swap(
                        &bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset], member, entry)) {
                    // no need to do anything, current thread was successful
                } else {
                    delete entry;
                    member = bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset];
                    member->multiplicity.fetch_add(
                        boundNodeMultiplicity, std::memory_order_relaxed);
                }
            } else {
                member->multiplicity.fetch_add(boundNodeMultiplicity, std::memory_order_relaxed);
            }
        }
    }
}

template<>
void VariableLengthMorsel<true>::addToLocalNextBFSLevel(
    RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity, unsigned long boundNodeOffset) {
    auto recursiveDstNodeIDVector = vectors->recursiveEdgeIDVector;
    auto recursiveEdgeIDVector = vectors->recursiveEdgeIDVector;
    auto totalEdgeListSize = recursiveDstNodeIDVector->state->selVector->selectedSize;
    // TODO: These newEdgeListSegment need to be maintained somewhere, the memory needs to be freed.
    auto newEdgeListSegment = new edgeListSegment(totalEdgeListSize);
    localEdgeListSegment.push_back(newEdgeListSegment);
    auto srcNodeEdgeListAndLevel = bfsSharedState->nodeIDEdgeListAndLevel[boundNodeOffset];
    for (auto i = 0u; i < totalEdgeListSize; i++) {
        auto pos = recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST || state == VISITED_DST) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_DST_NEW);
        } else if (state == NOT_VISITED || state == VISITED) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_NEW);
        }
        auto entry = bfsSharedState->nodeIDEdgeListAndLevel[nodeID.offset];
        if (entry->bfsLevel < bfsSharedState->currentLevel) {
            auto newEntry =
                new edgeListAndLevel(bfsSharedState->currentLevel + 1, nodeID.offset, entry);
            if (__sync_bool_compare_and_swap(
                    &bfsSharedState->nodeIDEdgeListAndLevel[nodeID.offset], entry, newEntry)) {
                // This thread was successful in doing the CAS operation at the top.
                newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
            } else {
                // This thread was NOT successful in doing the CAS operation, hence free the memory
                // right here since it has no use.
                delete newEntry;
            }
        }
        auto edgeID = recursiveEdgeIDVector->getValue<common::relID_t>(pos);
        newEdgeListSegment->edgeListBlockPtr[i].edgeOffset = edgeID.offset;
        newEdgeListSegment->edgeListBlockPtr[i].src = srcNodeEdgeListAndLevel;
        auto currTopEdgeList = bfsSharedState->nodeIDEdgeListAndLevel[nodeID.offset]->top;
        newEdgeListSegment->edgeListBlockPtr[i].next = currTopEdgeList;
        // Keep trying to add until successful, if failed then read the new value.
        while (!__sync_bool_compare_and_swap(
            &bfsSharedState->nodeIDEdgeListAndLevel[nodeID.offset]->top, currTopEdgeList,
            &newEdgeListSegment->edgeListBlockPtr[i])) {
            // Failed to do the CAS operation, reread the top pointer and retry.
            currTopEdgeList = bfsSharedState->nodeIDEdgeListAndLevel[nodeID.offset]->top;
            newEdgeListSegment->edgeListBlockPtr[i].next = currTopEdgeList;
        }
    }
}

template<>
int64_t VariableLengthMorsel<false>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
    RecursiveJoinVectors* vectors) {
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    bool exitOuterLoop = false;
    auto dstNodeIDVector = vectors->dstNodeIDVector;
    auto pathLengthVector = vectors->pathLengthVector;
    while (startScanIdxAndSize.first < endIdx && size < common::DEFAULT_VECTOR_CAPACITY) {
        if (bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST ||
            bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW) {
            auto entry = bfsSharedState->nodeIDMultiplicityToLevel[startScanIdxAndSize.first];
            while (entry && entry->bfsLevel >= lowerBound) {
                auto multiplicity = entry->multiplicity.load(std::memory_order_relaxed);
                do {
                    dstNodeIDVector->setValue<common::nodeID_t>(
                        size, common::nodeID_t{startScanIdxAndSize.first, tableID});
                    pathLengthVector->setValue<int64_t>(size, entry->bfsLevel);
                    size++;
                } while (--multiplicity && size < common::DEFAULT_VECTOR_CAPACITY);
                /// ValueVector capacity was reached, keep morsel state saved & exit from loop.
                if (multiplicity > 0) {
                    entry->multiplicity.store(multiplicity, std::memory_order_relaxed);
                    bfsSharedState->nodeIDMultiplicityToLevel[startScanIdxAndSize.first] = entry;
                    exitOuterLoop = true;
                    break;
                }
                if (size == common::DEFAULT_VECTOR_CAPACITY) {
                    auto temp = entry;
                    bfsSharedState->nodeIDMultiplicityToLevel[startScanIdxAndSize.first] =
                        entry->next;
                    delete temp;
                    exitOuterLoop = true;
                    break;
                }
                auto temp = entry;
                entry = entry->next;
                delete temp;
            }
            if (entry && entry->bfsLevel < lowerBound) {
                multiplicityAndLevel* temp;
                while (entry) {
                    temp = entry;
                    entry = entry->next;
                    delete temp;
                }
            }
        }
        if (!exitOuterLoop) {
            startScanIdxAndSize.first++;
        }
    }
    prevDistMorselStartEndIdx = {startScanIdxAndSize.first, endIdx};
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->getTable()->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        return size;
    }
    return 0;
}

template<>
int64_t VariableLengthMorsel<true>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
    RecursiveJoinVectors* vectors) {
    auto size = 0u, nodeIDDataVectorPos = 0u, relIDDataVectorPos = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    if (vectors->pathVector != nullptr) {
        vectors->pathVector->resetAuxiliaryBuffer();
    }
    uint8_t pathLength;
    auto nodeBuffer = std::vector<common::offset_t>(30u);
    auto relBuffer = std::vector<common::offset_t>(30u);
    while (startScanIdxAndSize.first < endIdx) {
        if ((bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST ||
                bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW) &&
            bfsSharedState->pathLength[startScanIdxAndSize.first] >= bfsSharedState->lowerBound) {
            pathLength = bfsSharedState->pathLength[startScanIdxAndSize.first];
            auto nodeEntry = common::ListVector::addList(vectors->pathNodesVector, pathLength - 1);
            auto relEntry = common::ListVector::addList(vectors->pathRelsVector, pathLength);
            vectors->pathNodesVector->setValue(size, nodeEntry);
            vectors->pathRelsVector->setValue(size, relEntry);
            vectors->dstNodeIDVector->setValue<common::nodeID_t>(
                size, common::nodeID_t{startScanIdxAndSize.first, tableID});
            vectors->pathLengthVector->setValue<int64_t>(size, pathLength);
            size++;
            // FROM HERE START WRITING THE PATH
        }
        startScanIdxAndSize.first++;
    }
    if (size > 0) {
        vectors->dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->getTable()->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        return size;
    }
    return 0;
}

} // namespace processor
} // namespace kuzu
