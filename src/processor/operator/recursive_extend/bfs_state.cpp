#include "processor/operator/recursive_extend/bfs_state.h"

#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/variable_length_state.h"
#include "processor/operator/table_scan/ftable_scan_function.h"
#include <processor/operator/recursive_extend/shortest_path_state.h>

namespace kuzu {
namespace processor {

void BFSSharedState::reset(TargetDstNodes* targetDstNodes, common::QueryRelType queryRelType,
    planner::RecursiveJoinType joinType) {
    ssspLocalState = EXTEND_IN_PROGRESS;
    currentLevel = 0u;
    startTimeInMillis1 = 0u;
    startTimeInMillis2 = 0u;
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
SSSPLocalState BFSSharedState::getBFSMorsel(BaseBFSState* bfsMorsel) {
    std::unique_lock<std::mutex> lck{mutex, std::defer_lock};
    while (true) {
        switch (ssspLocalState) {
        case MORSEL_COMPLETE: {
            return NO_WORK_TO_SHARE;
        }
        case PATH_LENGTH_WRITE_IN_PROGRESS: {
            if (nextDstScanStartIdx < visitedNodes.size()) {
                lck.lock();
                bfsMorsel->bfsSharedState = this;
                lck.unlock();
                return PATH_LENGTH_WRITE_IN_PROGRESS;
            }
            cv.wait(lck);
            lck.unlock();
        } break;
        case EXTEND_IN_PROGRESS: {
            lck.lock();
            if (nextScanStartIdx < bfsLevelNodeOffsets.size()) {
                numThreadsBFSActive++;
                auto bfsMorselSize =
                    std::min(bfsMorsel->bfsMorselSize, bfsLevelNodeOffsets.size() - nextScanStartIdx);
                auto morselScanEndIdx = nextScanStartIdx + bfsMorselSize;
                bfsMorsel->reset(nextScanStartIdx, morselScanEndIdx, this);
                nextScanStartIdx += bfsMorselSize;
                lck.unlock();
                return EXTEND_IN_PROGRESS;
            }
            cv.wait(lck);
            lck.unlock();
        } break;
        default:
            throw common::RuntimeException(
                &"Unknown local state encountered inside BFSSharedState: "[ssspLocalState]);
        }
    }
}

bool BFSSharedState::finishBFSMorsel(BaseBFSState* bfsMorsel, common::QueryRelType queryRelType) {
    std::unique_lock lck{mutex};
    numThreadsBFSActive--;
    if (ssspLocalState != EXTEND_IN_PROGRESS) {
        return true;
    }
    // Update the destinations visited, used to check for termination condition.
    // ONLY for shortest path and all shortest path recursive join.
    if (queryRelType == common::QueryRelType::SHORTEST) {
        auto shortestPathMorsel = (reinterpret_cast<ShortestPathState<false>*>(bfsMorsel));
        numVisitedNodes += shortestPathMorsel->getNumVisitedDstNodes();
    } else if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        auto allShortestPathMorsel = (reinterpret_cast<AllShortestPathState<false>*>(bfsMorsel));
        numVisitedNodes += allShortestPathMorsel->getNumVisitedDstNodes();
        if (!allShortestPathMorsel->getLocalEdgeListSegments().empty()) {
            auto& localEdgeListSegment = allShortestPathMorsel->getLocalEdgeListSegments();
            allEdgeListSegments.insert(allEdgeListSegments.end(), localEdgeListSegment.begin(),
                localEdgeListSegment.end());
            localEdgeListSegment.resize(0);
        }
    } else {
        auto varLenPathMorsel = (reinterpret_cast<VariableLengthState<false>*>(bfsMorsel));
        if (!varLenPathMorsel->getLocalEdgeListSegments().empty()) {
            auto& localEdgeListSegment = varLenPathMorsel->getLocalEdgeListSegments();
            allEdgeListSegments.insert(allEdgeListSegments.end(), localEdgeListSegment.begin(),
                localEdgeListSegment.end());
            localEdgeListSegment.resize(0);
        }
    }
    if (numThreadsBFSActive == 0 && nextScanStartIdx == bfsLevelNodeOffsets.size()) {
        moveNextLevelAsCurrentLevel();
        if (isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
            auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            startTimeInMillis2 = millis;
            ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
            cv.notify_all();
            return true;
        }
    } else if (isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
        ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
        cv.notify_all();
        return true;
    }
    cv.notify_all();
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

/**
 * All the plausible cases:
 *
 * (1) thread comes in, gets a morsel
 * (2) thread comes in, doesn't get a morsel, deletes its thread id, exits <--- // thread should NOT come back again
 * (3) thread comes in, doesn't get a morsel, thread id already deleted, exits <--- // this should NOT happen at all
 * (4) thread comes in, doesn't get a morsel, delete its thread id, marks it as complete, exits
 */
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

} // namespace processor
} // namespace kuzu
