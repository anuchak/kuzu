#include "processor/operator/recursive_extend/variable_length_state.h"

namespace kuzu {
namespace processor {

template<>
void VariableLengthState<false>::addToLocalNextBFSLevel(
    RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity, unsigned long boundNodeOffset) {
    auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
    auto selectedSize = recursiveDstNodeIDVector->state->getSelVector().getSelSize();
    auto selPos = recursiveDstNodeIDVector->state->getSelVector().getSelectedPositions();
    for (auto i = 0u; i < selectedSize; i++) {
        auto pos = selPos[i];
        auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST || state == VISITED_DST) {
            __atomic_store_n(
                &bfsSharedState->visitedNodes[nodeID.offset], VISITED_DST_NEW, __ATOMIC_RELAXED);
        } else if (state == NOT_VISITED || state == VISITED) {
            __atomic_store_n(
                &bfsSharedState->visitedNodes[nodeID.offset], VISITED_NEW, __ATOMIC_RELAXED);
        }
        auto entry = bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset];
        if (!entry || (entry->bfsLevel <= bfsSharedState->currentLevel)) {
            auto newEntry = new multiplicityAndLevel(
                boundNodeMultiplicity, bfsSharedState->currentLevel + 1, entry);
            if (__sync_bool_compare_and_swap(
                    &bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset], entry, newEntry)) {
                // no need to do anything, current thread was successful
            } else {
                delete newEntry;
                entry = bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset];
                entry->multiplicity.fetch_add(boundNodeMultiplicity, std::memory_order_relaxed);
            }
        } else {
            entry->multiplicity.fetch_add(boundNodeMultiplicity, std::memory_order_relaxed);
        }
    }
}

template<>
void VariableLengthState<true>::addToLocalNextBFSLevel(
    RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity, unsigned long boundNodeOffset) {
    auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
    auto recursiveEdgeIDVector = vectors->recursiveEdgeIDVector;
    auto selectedSize = recursiveDstNodeIDVector->state->getSelVector().getSelSize();
    auto selPos = recursiveDstNodeIDVector->state->getSelVector().getSelectedPositions();
    auto totalEdgeListSize = selectedSize;
    auto newEdgeListSegment = new edgeListSegment(totalEdgeListSize);
    localEdgeListSegment.push_back(newEdgeListSegment);
    auto srcNodeEdgeListAndLevel = bfsSharedState->nodeIDEdgeListAndLevel[boundNodeOffset];
    while (srcNodeEdgeListAndLevel->bfsLevel != bfsSharedState->currentLevel) {
        srcNodeEdgeListAndLevel = srcNodeEdgeListAndLevel->next;
    }
    for (auto i = 0u; i < totalEdgeListSize; i++) {
        auto pos = selPos[i];
        auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST || state == VISITED_DST) {
            __atomic_store_n(
                &bfsSharedState->visitedNodes[nodeID.offset], VISITED_DST_NEW, __ATOMIC_RELAXED);
        } else if (state == NOT_VISITED || state == VISITED) {
            __atomic_store_n(
                &bfsSharedState->visitedNodes[nodeID.offset], VISITED_NEW, __ATOMIC_RELAXED);
        }
        auto entry = bfsSharedState->nodeIDEdgeListAndLevel[nodeID.offset];
        if (!entry || (entry->bfsLevel <= bfsSharedState->currentLevel)) {
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
        /// TEMP - to keep the edge table ID saved later for writing to the ValueVector
        if (bfsSharedState->edgeTableID == UINT64_MAX) {
            bfsSharedState->edgeTableID = edgeID.tableID;
        }
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
int64_t VariableLengthState<false>::writeToVector(
    const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
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
        } else {
            auto entry = bfsSharedState->nodeIDMultiplicityToLevel[startScanIdxAndSize.first];
            multiplicityAndLevel* temp;
            while (entry) {
                temp = entry;
                entry = entry->next;
                delete temp;
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
        inputFTableSharedState->table->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        if (!vectorsToScan[0]->state->isFlat()) {
            vectorsToScan[0]->state->setToFlat();
        }
        return size;
    }
    return 0;
}

// TODO (Anurag): This function can be refactored because a huge part is common (looping to
// enumerate all paths).
template<>
int64_t VariableLengthState<true>::writeToVector(
    const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
    RecursiveJoinVectors* vectors) {
    auto size = 0u, nodeIDDataVectorPos = 0u, relIDDataVectorPos = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    if (vectors->pathVector != nullptr) {
        vectors->pathVector->resetAuxiliaryBuffer();
    }
    if (nodeBuffer.empty()) {
        nodeBuffer = std::vector<edgeListAndLevel*>(31u, nullptr);
        relBuffer = std::vector<edgeList*>(31u, nullptr);
    }
    if (hasMorePathToWrite) {
        bool exitLoop = true;
        auto edgeListAndLevel = bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first];
        auto pathLength = edgeListAndLevel->bfsLevel;
        do {
            exitLoop = true;
            auto nodeEntry = common::ListVector::addList(vectors->pathNodesVector, pathLength - 1);
            auto relEntry = common::ListVector::addList(vectors->pathRelsVector, pathLength);
            vectors->pathNodesVector->setValue(size, nodeEntry);
            vectors->pathRelsVector->setValue(size, relEntry);
            vectors->dstNodeIDVector->setValue<common::nodeID_t>(
                size, common::nodeID_t{startScanIdxAndSize.first, tableID});
            vectors->pathLengthVector->setValue<int64_t>(size, pathLength);
            for (auto i = 1u; i < pathLength; i++) {
                vectors->pathNodesIDDataVector->setValue<common::nodeID_t>(
                    nodeIDDataVectorPos++, common::nodeID_t{nodeBuffer[i]->nodeOffset, tableID});
            }
            for (auto i = 0u; i < pathLength; i++) {
                vectors->pathRelsSrcIDDataVector->setValue<common::nodeID_t>(
                    relIDDataVectorPos, common::nodeID_t{nodeBuffer[i]->nodeOffset, tableID});
                vectors->pathRelsIDDataVector->setValue<common::relID_t>(relIDDataVectorPos,
                    common::relID_t{relBuffer[i]->edgeOffset, bfsSharedState->edgeTableID});
                vectors->pathRelsDstIDDataVector->setValue<common::nodeID_t>(
                    relIDDataVectorPos++, common::nodeID_t{nodeBuffer[i + 1]->nodeOffset, tableID});
            }
            for (auto i = 0u; i < pathLength; i++) {
                if (relBuffer[i]->next) {
                    auto j = i;
                    auto temp_ = relBuffer[j]->next;
                    while (temp_->edgeOffset != UINT64_MAX) {
                        relBuffer[j] = temp_;
                        nodeBuffer[j] = temp_->src;
                        temp_ = nodeBuffer[j]->top;
                        j--;
                    }
                    exitLoop = false;
                    break;
                }
            }
            size++;
        } while (size < common::DEFAULT_VECTOR_CAPACITY && !exitLoop);
        if (size == common::DEFAULT_VECTOR_CAPACITY && !exitLoop) {
            hasMorePathToWrite = true;
        } else {
            hasMorePathToWrite = false;
            bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first] =
                bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first]->next;
        }
    } else {
        while (startScanIdxAndSize.first < endIdx && size < common::DEFAULT_VECTOR_CAPACITY) {
            if ((bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW ||
                    bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST) &&
                bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first] &&
                bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first]->bfsLevel >=
                    lowerBound) {
                auto edgeListAndLevel =
                    bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first];
                auto pathLength = edgeListAndLevel->bfsLevel;
                auto idx = 0u;
                nodeBuffer[pathLength - idx] = edgeListAndLevel;
                auto temp = edgeListAndLevel;
                while (pathLength > idx) {
                    relBuffer[pathLength - idx - 1] = temp->top;
                    temp = temp->top->src;
                    nodeBuffer[pathLength - ++idx] = temp;
                }
                bool exitLoop = true;
                do {
                    exitLoop = true;
                    auto nodeEntry =
                        common::ListVector::addList(vectors->pathNodesVector, pathLength - 1);
                    auto relEntry =
                        common::ListVector::addList(vectors->pathRelsVector, pathLength);
                    vectors->pathNodesVector->setValue(size, nodeEntry);
                    vectors->pathRelsVector->setValue(size, relEntry);
                    vectors->dstNodeIDVector->setValue<common::nodeID_t>(
                        size, common::nodeID_t{startScanIdxAndSize.first, tableID});
                    vectors->pathLengthVector->setValue<int64_t>(size, pathLength);
                    for (auto i = 1u; i < pathLength; i++) {
                        vectors->pathNodesIDDataVector->setValue<common::nodeID_t>(
                            nodeIDDataVectorPos++,
                            common::nodeID_t{nodeBuffer[i]->nodeOffset, tableID});
                    }
                    for (auto i = 0u; i < pathLength; i++) {
                        vectors->pathRelsSrcIDDataVector->setValue<common::nodeID_t>(
                            relIDDataVectorPos,
                            common::nodeID_t{nodeBuffer[i]->nodeOffset, tableID});
                        vectors->pathRelsIDDataVector->setValue<common::relID_t>(relIDDataVectorPos,
                            common::relID_t{relBuffer[i]->edgeOffset, bfsSharedState->edgeTableID});
                        vectors->pathRelsDstIDDataVector->setValue<common::nodeID_t>(
                            relIDDataVectorPos++,
                            common::nodeID_t{nodeBuffer[i + 1]->nodeOffset, tableID});
                    }
                    for (auto i = 0u; i < pathLength; i++) {
                        if (relBuffer[i]->next) {
                            auto j = i;
                            auto temp_ = relBuffer[j]->next;
                            while (temp_->edgeOffset != UINT64_MAX) {
                                relBuffer[j] = temp_;
                                nodeBuffer[j] = temp_->src;
                                temp_ = nodeBuffer[j]->top;
                                j--;
                            }
                            exitLoop = false;
                            break;
                        }
                    }
                    size++;
                } while (size < common::DEFAULT_VECTOR_CAPACITY && !exitLoop);
                if (size == common::DEFAULT_VECTOR_CAPACITY && !exitLoop) {
                    hasMorePathToWrite = true;
                    break;
                } else if (size == common::DEFAULT_VECTOR_CAPACITY && exitLoop) {
                    hasMorePathToWrite = false;
                    bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first] =
                        bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first]->next;
                    break;
                } else {
                    hasMorePathToWrite = false;
                    bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first] =
                        bfsSharedState->nodeIDEdgeListAndLevel[startScanIdxAndSize.first]->next;
                    continue;
                }
            }
            startScanIdxAndSize.first++;
        }
    }
    prevDistMorselStartEndIdx = {startScanIdxAndSize.first, endIdx};
    if (size > 0) {
        vectors->dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->table->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        if (!vectorsToScan[0]->state->isFlat()) {
            vectorsToScan[0]->state->setToFlat();
        }
        return size;
    }
    return 0;
}

} // namespace processor
} // namespace kuzu
