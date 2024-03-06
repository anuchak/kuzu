#include "processor/operator/recursive_extend/bfs_state.h"

#include "common/exception.h"
#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/variable_length_state.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

void BFSSharedState::reset(TargetDstNodes* targetDstNodes, common::QueryRelType queryRelType,
    planner::RecursiveJoinType joinType) {
    ssspLocalState = EXTEND_IN_PROGRESS;
    currentLevel = 0u;
    nextScanStartIdx = 0u;
    numVisitedNodes = 0u;
    auto totalDestinations = targetDstNodes->getNumNodes();
    if (totalDestinations == (maxOffset + 1) || totalDestinations == 0u) {
        // All node offsets are destinations hence mark all as not visited destinations.
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
    } else {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED);
        for (auto& dstOffset : targetDstNodes->getNodeIDs()) {
            visitedNodes[dstOffset.offset] = NOT_VISITED_DST;
        }
    }
    std::fill(pathLength.begin(), pathLength.end(), 0u);
    bfsLevelNodeOffsets.clear();
    srcOffset = 0u;
    numThreadsBFSActive = 0u;
    nextDstScanStartIdx = 0u;
    inputFTableTupleIdx = 0u;
    pathLengthThreadWriters = std::unordered_set<std::thread::id>();
    if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        minDistance = 0u;
        if (joinType == planner::RecursiveJoinType::TRACK_NONE) {
            // nodeIDToMultiplicity is not defined in the constructor directly, only for all
            // shortest recursive join it is required. If it is empty then assign a vector of size
            // maxNodeOffset to it (same size as visitedNodes).
            if (nodeIDToMultiplicity.empty()) {
                nodeIDToMultiplicity = std::vector<uint64_t>(visitedNodes.size(), 0u);
            } else {
                std::fill(nodeIDToMultiplicity.begin(), nodeIDToMultiplicity.end(), 0u);
            }
        } else {
            edgeTableID = UINT64_MAX;
            if (nodeIDEdgeListAndLevel.empty()) {
                nodeIDEdgeListAndLevel =
                    std::vector<edgeListAndLevel*>(visitedNodes.size(), nullptr);
                allEdgeListSegments = std::vector<edgeListSegment*>();
            } else {
                std::fill(nodeIDEdgeListAndLevel.begin(), nodeIDEdgeListAndLevel.end(), nullptr);
                allEdgeListSegments.clear();
            }
        }
    }
    if (queryRelType == common::QueryRelType::VARIABLE_LENGTH) {
        if (joinType == planner::RecursiveJoinType::TRACK_NONE) {
            if (nodeIDMultiplicityToLevel.empty()) {
                nodeIDMultiplicityToLevel =
                    std::vector<multiplicityAndLevel*>(visitedNodes.size(), nullptr);
            } else {
                std::fill(
                    nodeIDMultiplicityToLevel.begin(), nodeIDMultiplicityToLevel.end(), nullptr);
            }
        } else {
            edgeTableID = UINT64_MAX;
            if (nodeIDEdgeListAndLevel.empty()) {
                nodeIDEdgeListAndLevel =
                    std::vector<edgeListAndLevel*>(visitedNodes.size(), nullptr);
            } else {
                std::fill(nodeIDEdgeListAndLevel.begin(), nodeIDEdgeListAndLevel.end(), nullptr);
                allEdgeListSegments.clear();
            }
        }
    }
    if (joinType == planner::RecursiveJoinType::TRACK_PATH &&
        queryRelType == common::QueryRelType::SHORTEST) {
        // was not initialized at the constructor, being used for the 1st time
        edgeTableID = UINT64_MAX;
        if (srcNodeOffsetAndEdgeOffset.empty()) {
            srcNodeOffsetAndEdgeOffset =
                std::vector<std::pair<uint64_t, uint64_t>>(visitedNodes.size());
        }
    }
}

/*
 * Returning the state here because if BFSSharedState is complete / in pathLength writing stage
 * then depending on state we need to take next step. If MORSEL_COMPLETE then proceed to get a new
 * BFSSharedState & if MORSEL_pathLength_WRITING_IN_PROGRESS then help in this task.
 */
SSSPLocalState BFSSharedState::getBFSMorsel(BaseBFSMorsel* bfsMorsel) {
    std::unique_lock lck{mutex};
    switch (ssspLocalState) {
    case MORSEL_COMPLETE: {
        return NO_WORK_TO_SHARE;
    }
    case PATH_LENGTH_WRITE_IN_PROGRESS: {
        bfsMorsel->bfsSharedState = this;
        return PATH_LENGTH_WRITE_IN_PROGRESS;
    }
    case EXTEND_IN_PROGRESS: {
        if (nextScanStartIdx < bfsLevelNodeOffsets.size()) {
            numThreadsBFSActive++;
            auto bfsMorselSize = std::min(bfsMorsel->bfsMorselSize, bfsLevelNodeOffsets.size() - nextScanStartIdx);
            auto morselScanEndIdx = nextScanStartIdx + bfsMorselSize;
            bfsMorsel->reset(nextScanStartIdx, morselScanEndIdx, this);
            nextScanStartIdx += bfsMorselSize;
            return EXTEND_IN_PROGRESS;
        } else {
            return NO_WORK_TO_SHARE;
        }
    }
    default:
        throw common::RuntimeException(
            &"Unknown local state encountered inside BFSSharedState: "[ssspLocalState]);
    }
}

bool BFSSharedState::hasWork() const {
    if (ssspLocalState == EXTEND_IN_PROGRESS && nextScanStartIdx < bfsLevelNodeOffsets.size()) {
        return true;
    }
    if (ssspLocalState == PATH_LENGTH_WRITE_IN_PROGRESS &&
        nextDstScanStartIdx < visitedNodes.size()) {
        return true;
    }
    return false;
}

bool BFSSharedState::finishBFSMorsel(BaseBFSMorsel* bfsMorsel, common::QueryRelType queryRelType) {
    std::unique_lock lck{mutex};
    numThreadsBFSActive--;
    if (ssspLocalState != EXTEND_IN_PROGRESS) {
        return true;
    }
    // Update the destinations visited, used to check for termination condition.
    // ONLY for shortest path and all shortest path recursive join.
    if (queryRelType == common::QueryRelType::SHORTEST) {
        auto shortestPathMorsel = (reinterpret_cast<ShortestPathMorsel<false>*>(bfsMorsel));
        numVisitedNodes += shortestPathMorsel->getNumVisitedDstNodes();
    } else if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        auto allShortestPathMorsel = (reinterpret_cast<AllShortestPathMorsel<false>*>(bfsMorsel));
        numVisitedNodes += allShortestPathMorsel->getNumVisitedDstNodes();
        if (!allShortestPathMorsel->getLocalEdgeListSegments().empty()) {
            auto& localEdgeListSegment = allShortestPathMorsel->getLocalEdgeListSegments();
            allEdgeListSegments.insert(allEdgeListSegments.end(), localEdgeListSegment.begin(),
                localEdgeListSegment.end());
            localEdgeListSegment.resize(0);
        }
    } else {
        auto varLenPathMorsel = (reinterpret_cast<VariableLengthMorsel<false>*>(bfsMorsel));
        if (!varLenPathMorsel->getLocalEdgeListSegments().empty()) {
            auto& localEdgeListSegment = varLenPathMorsel->getLocalEdgeListSegments();
            allEdgeListSegments.insert(allEdgeListSegments.end(), localEdgeListSegment.begin(),
                localEdgeListSegment.end());
            localEdgeListSegment.resize(0);
        }
    }
    if (numThreadsBFSActive == 0 && nextScanStartIdx == bfsLevelNodeOffsets.size()) {
        auto duration0 = std::chrono::system_clock::now().time_since_epoch();
        auto millis0 = std::chrono::duration_cast<std::chrono::milliseconds>(duration0).count();
        printf("%lu ms is level %d end time \n", millis0, currentLevel);
        moveNextLevelAsCurrentLevel();
        duration0 = std::chrono::system_clock::now().time_since_epoch();
        millis0 = std::chrono::duration_cast<std::chrono::milliseconds>(duration0).count();
        printf("%lu ms is level %d start time, total nodes: %d\n", millis0, currentLevel, bfsLevelNodeOffsets.size());
        if (isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
            auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            printf("%lu ms is end time \n", millis);
            ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
            return true;
        }
    } else if (isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
        auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        printf("%lu ms is end time \n", millis);
        ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
        return true;
    }
    return false;
}

bool BFSSharedState::isBFSComplete(uint64_t numDstNodesToVisit, common::QueryRelType queryRelType) {
    if (bfsLevelNodeOffsets.empty()) { // no more to extend.
        return true;
    }
    if (currentLevel == upperBound) { // upper limit reached.
        return true;
    }
    if (queryRelType == common::QueryRelType::SHORTEST) {
        return numVisitedNodes == numDstNodesToVisit;
    }
    if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        return (numVisitedNodes == numDstNodesToVisit) && (currentLevel > minDistance);
    }
    return false;
}

void BFSSharedState::markSrc(bool isSrcDestination, common::QueryRelType queryRelType) {
    if (isSrcDestination) {
        visitedNodes[srcOffset] = VISITED_DST;
        numVisitedNodes++;
        pathLength[srcOffset] = 0;
    } else {
        visitedNodes[srcOffset] = VISITED;
    }
    bfsLevelNodeOffsets.push_back(srcOffset);
    if (queryRelType == common::QueryRelType::SHORTEST && !srcNodeOffsetAndEdgeOffset.empty()) {
        srcNodeOffsetAndEdgeOffset[srcOffset] = {UINT64_MAX, UINT64_MAX};
    }
    if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        if (nodeIDEdgeListAndLevel.empty()) {
            nodeIDToMultiplicity[srcOffset] = 1;
        } else {
            auto newEdgeListSegment = new edgeListSegment(1);
            newEdgeListSegment->edgeListBlockPtr[0].edgeOffset = UINT64_MAX;
            newEdgeListSegment->edgeListBlockPtr[0].next = nullptr;
            newEdgeListSegment->edgeListBlockPtr[0].src = nullptr;
            newEdgeListSegment->edgeListAndLevelBlock.push_back(
                new edgeListAndLevel(0, srcOffset, nullptr));
            nodeIDEdgeListAndLevel[srcOffset] = newEdgeListSegment->edgeListAndLevelBlock[0];
            nodeIDEdgeListAndLevel[srcOffset]->top = &newEdgeListSegment->edgeListBlockPtr[0];
            allEdgeListSegments.push_back(newEdgeListSegment);
        }
    }
    if (queryRelType == common::QueryRelType::VARIABLE_LENGTH) {
        if (nodeIDEdgeListAndLevel.empty()) {
            auto entry = new multiplicityAndLevel(1 /* multiplicity */, 0 /* bfs level */,
                nullptr /* next multiplicityAndLevel ptr */);
            nodeIDMultiplicityToLevel[srcOffset] = entry;
        } else {
            auto newEdgeListSegment = new edgeListSegment(1);
            newEdgeListSegment->edgeListBlockPtr[0].edgeOffset = UINT64_MAX;
            newEdgeListSegment->edgeListBlockPtr[0].next = nullptr;
            newEdgeListSegment->edgeListBlockPtr[0].src = nullptr;
            newEdgeListSegment->edgeListAndLevelBlock.push_back(
                new edgeListAndLevel(0, srcOffset, nullptr));
            nodeIDEdgeListAndLevel[srcOffset] = newEdgeListSegment->edgeListAndLevelBlock[0];
            nodeIDEdgeListAndLevel[srcOffset]->top = &newEdgeListSegment->edgeListBlockPtr[0];
            allEdgeListSegments.push_back(newEdgeListSegment);
        }
    }
}

void BFSSharedState::moveNextLevelAsCurrentLevel() {
    currentLevel++;
    nextScanStartIdx = 0u;
    if (currentLevel < upperBound) { // No need to prepare this vector if we won't extend.
        /// TODO: This is a bottleneck, optimize this by directly giving out morsels from
        /// visitedNodes instead of putting it into bfsLevelNodeOffsets.
        bfsLevelNodeOffsets.clear();
        for (auto i = 0u; i < visitedNodes.size(); i++) {
            if (visitedNodes[i] == VISITED_NEW) {
                visitedNodes[i] = VISITED;
                bfsLevelNodeOffsets.push_back(i);
            } else if (visitedNodes[i] == VISITED_DST_NEW) {
                visitedNodes[i] = VISITED_DST;
                bfsLevelNodeOffsets.push_back(i);
            }
        }
    }
}

std::pair<uint64_t, int64_t> BFSSharedState::getDstPathLengthMorsel() {
    std::unique_lock lck{mutex};
    if (ssspLocalState != PATH_LENGTH_WRITE_IN_PROGRESS) {
        return {UINT64_MAX, INT64_MAX};
    }
    auto threadID = std::this_thread::get_id();
    if (nextDstScanStartIdx == visitedNodes.size()) {
        if (!canThreadCompleteSharedState(threadID)) {
            return {UINT64_MAX, INT64_MAX};
        }
        pathLengthThreadWriters.erase(threadID);
        /// Last Thread to exit will be responsible for doing state change to MORSEL_COMPLETE
        /// Along with state change it will also decrement numActiveBFSSharedState by 1
        if (pathLengthThreadWriters.empty()) {
            return {0, -1};
        }
        return {UINT64_MAX, INT64_MAX};
    }
    auto sizeToScan =
        std::min(common::DEFAULT_VECTOR_CAPACITY, visitedNodes.size() - nextDstScanStartIdx);
    std::pair<uint64_t, uint32_t> startScanIdxAndSize = {nextDstScanStartIdx, sizeToScan};
    nextDstScanStartIdx += sizeToScan;
    pathLengthThreadWriters.insert(threadID);
    return startScanIdxAndSize;
}

template<>
void ShortestPathMorsel<false>::addToLocalNextBFSLevel(
    RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity, unsigned long boundNodeOffset) {
    auto startIdx = vectors->inMemCsr->csr_v[boundNodeOffset];
    auto endIdx = (boundNodeOffset == vectors->inMemCsr->csr_v.size() - 1) ?
                      vectors->inMemCsr->csr_e.size() :
                      vectors->inMemCsr->csr_v[boundNodeOffset + 1];
    uint64_t nbrOffset;
    for (auto i = startIdx; i < endIdx; i++) {
        nbrOffset = vectors->inMemCsr->csr_e[i];
        auto state = bfsSharedState->visitedNodes[nbrOffset];
        if (state == NOT_VISITED_DST) {
            __atomic_store_n(&bfsSharedState->visitedNodes[nbrOffset], VISITED_DST_NEW, __ATOMIC_SEQ_CST);
        }
        if (state == NOT_VISITED) {
            __atomic_store_n(&bfsSharedState->visitedNodes[nbrOffset], VISITED_NEW, __ATOMIC_SEQ_CST);
        }
        /*if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(
                    &bfsSharedState->visitedNodes[nbrOffset], state, VISITED_DST_NEW)) {
                bfsSharedState->pathLength[nbrOffset] = bfsSharedState->currentLevel + 1;
                numVisitedDstNodes++;
            }
        } else if (state == NOT_VISITED) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nbrOffset], state, VISITED_NEW);
        }*/
    }
    /*auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
    for (auto i = 0u; i < recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(
                    &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_DST_NEW)) {
                /// NOTE: This write is safe to do here without a CAS, because we have a full
                /// memory barrier once each thread merges its results (they have to hold a lock).
                /// A CAS would be required if a read had occurred before this full memory barrier
                /// - such as visitedNodes state. Those states are being written to and read from
                /// even before each thread holds the lock.
                bfsSharedState->pathLength[nodeID.offset] = bfsSharedState->currentLevel + 1;
                numVisitedDstNodes++;
            }
        } else if (state == NOT_VISITED) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_NEW);
        }
    }*/
}

template<>
void ShortestPathMorsel<true>::addToLocalNextBFSLevel(RecursiveJoinVectors* vectors,
    uint64_t boundNodeMultiplicity, common::offset_t boundNodeOffset) {
    auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
    auto recursiveEdgeIDVector = vectors->recursiveEdgeIDVector;
    for (auto i = 0u; i < recursiveDstNodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(
                    &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_DST_NEW)) {
                numVisitedDstNodes++;
                auto edgeID = recursiveEdgeIDVector->getValue<common::relID_t>(pos);
                // TODO: Do we even need this ? Because once we track back the path to the source
                // we know the length of the path, so a separate vector to have the bfsLevel is not
                // needed. Remove this later if speed is slow.
                bfsSharedState->pathLength[nodeID.offset] = bfsSharedState->currentLevel + 1;
                bfsSharedState->srcNodeOffsetAndEdgeOffset[nodeID.offset] = {
                    boundNodeOffset, edgeID.offset};
                /// TEMP - to keep the edge table ID saved later for writing to the ValueVector
                if (bfsSharedState->edgeTableID == UINT64_MAX) {
                    bfsSharedState->edgeTableID = edgeID.tableID;
                }
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(
                    &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_NEW)) {
                auto edgeID = recursiveEdgeIDVector->getValue<common::relID_t>(pos);
                bfsSharedState->srcNodeOffsetAndEdgeOffset[nodeID.offset] = {
                    boundNodeOffset, edgeID.offset};
            }
        }
    }
}

template<>
int64_t ShortestPathMorsel<false>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
    RecursiveJoinVectors* vectors) {
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    auto dstNodeIDVector = vectors->dstNodeIDVector;
    auto pathLengthVector = vectors->pathLengthVector;
    while (startScanIdxAndSize.first < endIdx) {
        if ((bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST ||
                bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW) &&
            bfsSharedState->pathLength[startScanIdxAndSize.first] >= bfsSharedState->lowerBound) {
            dstNodeIDVector->setValue<common::nodeID_t>(
                size, common::nodeID_t{startScanIdxAndSize.first, tableID});
            pathLengthVector->setValue<int64_t>(
                size, bfsSharedState->pathLength[startScanIdxAndSize.first]);
            size++;
        }
        startScanIdxAndSize.first++;
    }
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->getTable()->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        if (!vectorsToScan[0]->state->isFlat()) {
            vectorsToScan[0]->state->setToFlat();
        }
        return size;
    }
    return 0;
}

template<>
int64_t ShortestPathMorsel<true>::writeToVector(
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
    auto nodeBuffer = std::vector<common::offset_t>(31u);
    auto relBuffer = std::vector<common::offset_t>(31u);
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
            auto entry = bfsSharedState->srcNodeOffsetAndEdgeOffset[startScanIdxAndSize.first];
            auto idx = 0u;
            nodeBuffer[pathLength - idx] = startScanIdxAndSize.first;
            while (entry.first != UINT64_MAX && entry.second != UINT64_MAX) {
                relBuffer[pathLength - idx - 1] = entry.second;
                nodeBuffer[pathLength - ++idx] = entry.first;
                entry = bfsSharedState->srcNodeOffsetAndEdgeOffset[entry.first];
            }
            for (auto i = 1u; i < pathLength; i++) {
                vectors->pathNodesIDDataVector->setValue<common::nodeID_t>(
                    nodeIDDataVectorPos++, common::nodeID_t{nodeBuffer[i], tableID});
            }
            for (auto i = 0u; i < pathLength; i++) {
                vectors->pathRelsSrcIDDataVector->setValue<common::nodeID_t>(
                    relIDDataVectorPos, common::nodeID_t{nodeBuffer[i], tableID});
                vectors->pathRelsIDDataVector->setValue<common::relID_t>(
                    relIDDataVectorPos, common::relID_t{relBuffer[i], bfsSharedState->edgeTableID});
                vectors->pathRelsDstIDDataVector->setValue<common::nodeID_t>(
                    relIDDataVectorPos++, common::nodeID_t{nodeBuffer[i + 1], tableID});
            }
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
        if (!vectorsToScan[0]->state->isFlat()) {
            vectorsToScan[0]->state->setToFlat();
        }
        return size;
    }
    return 0;
}

} // namespace processor
} // namespace kuzu
