#include "processor/operator/recursive_extend/bfs_state.h"

#include <cmath>

#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/variable_length_state.h"
#include "processor/operator/table_scan/ftable_scan_function.h"
#include <immintrin.h>
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
    // bfsLevelNodeOffsets.clear();
    isSparseFrontier = true;
    nextFrontierSize = 0u;
    if (!nextFrontier) {
        sparseFrontier = std::vector<common::offset_t>();
        sparseFrontier.reserve(maxOffset + 1);
        // skip allocating dense frontier unless required
        nextFrontier = new uint8_t[maxOffset + 1];
        std::fill(nextFrontier, nextFrontier + maxOffset + 1, 0u);
    } else {
        sparseFrontier.clear();
        if (denseFrontier) {
            std::fill(denseFrontier, denseFrontier + maxOffset + 1, 0u);
        }
        std::fill(nextFrontier, nextFrontier + maxOffset + 1, 0u);
    }
    srcOffset = 0u;
    numThreadsBFSRegistered = 0u;
    numThreadsOutputRegistered = 0u;
    numThreadsBFSFinished = 0u;
    numThreadsOutputFinished = 0u;
    nextDstScanStartIdx = 0u;
    inputFTableTupleIdx = 0u;
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
                std::fill(nodeIDMultiplicityToLevel.begin(), nodeIDMultiplicityToLevel.end(),
                    nullptr);
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

SSSPLocalState BFSSharedState::getBFSMorsel(BaseBFSState* bfsMorsel,
    common::QueryRelType queryRelType) {
    auto morselStartIdx =
        nextScanStartIdx.fetch_add(bfsMorsel->bfsMorselSize, std::memory_order_acq_rel);
    if (morselStartIdx >= currentFrontierSize ||
        isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
        mutex.lock();
        numThreadsBFSFinished++;
        if (numThreadsBFSRegistered != numThreadsBFSFinished) {
            mutex.unlock();
            bfsMorsel->bfsSharedState = nullptr;
            return NO_WORK_TO_SHARE;
        }
        if (isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
            auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            startTimeInMillis2 = millis;
            ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
            numThreadsOutputRegistered++;
            mutex.unlock();
            return PATH_LENGTH_WRITE_IN_PROGRESS;
        }
        moveNextLevelAsCurrentLevel();
        numThreadsBFSRegistered = 0, numThreadsBFSFinished = 0;
        if (isBFSComplete(bfsMorsel->targetDstNodes->getNumNodes(), queryRelType)) {
            auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            startTimeInMillis2 = millis;
            ssspLocalState = PATH_LENGTH_WRITE_IN_PROGRESS;
            numThreadsOutputRegistered++;
            mutex.unlock();
            return PATH_LENGTH_WRITE_IN_PROGRESS;
        }
        numThreadsBFSRegistered++;
        morselStartIdx =
            nextScanStartIdx.fetch_add(bfsMorsel->bfsMorselSize, std::memory_order_acq_rel);
        mutex.unlock();
    }
    uint64_t morselEndIdx =
        std::min(morselStartIdx + bfsMorsel->bfsMorselSize, currentFrontierSize);
    bfsMorsel->reset(morselStartIdx, morselEndIdx, this);
    return EXTEND_IN_PROGRESS;
}

void BFSSharedState::finishBFSMorsel(BaseBFSState* bfsMorsel, common::QueryRelType queryRelType) {
    if (queryRelType == common::QueryRelType::SHORTEST) {
        auto shortestPathMorsel = reinterpret_cast<ShortestPathState<false>*>(bfsMorsel);
        numVisitedNodes.fetch_add(shortestPathMorsel->getNumVisitedDstNodes(),
            std::memory_order_acq_rel);
        nextFrontierSize.fetch_add(shortestPathMorsel->getNumVisitedDstNodes() +
                                   shortestPathMorsel->getNumVisitedNonDstNodes());
    } else if (queryRelType == common::QueryRelType::ALL_SHORTEST) {
        auto allShortestPathMorsel = (reinterpret_cast<AllShortestPathState<false>*>(bfsMorsel));
        numVisitedNodes.fetch_add(allShortestPathMorsel->getNumVisitedDstNodes());
        nextFrontierSize.fetch_add(allShortestPathMorsel->getNumVisitedDstNodes() +
                                   allShortestPathMorsel->getNumVisitedNonDstNodes());
        if (!allShortestPathMorsel->getLocalEdgeListSegments().empty()) {
            mutex.lock();
            auto& localEdgeListSegment = allShortestPathMorsel->getLocalEdgeListSegments();
            allEdgeListSegments.insert(allEdgeListSegments.end(), localEdgeListSegment.begin(),
                localEdgeListSegment.end());
            localEdgeListSegment.resize(0);
            mutex.unlock();
        }
    } else {
        auto varLenPathMorsel = (reinterpret_cast<VariableLengthState<false>*>(bfsMorsel));
        nextFrontierSize.fetch_add(varLenPathMorsel->getNumVisitedDstNodes() +
                                   varLenPathMorsel->getNumVisitedNonDstNodes());
        if (!varLenPathMorsel->getLocalEdgeListSegments().empty()) {
            mutex.lock();
            auto& localEdgeListSegment = varLenPathMorsel->getLocalEdgeListSegments();
            allEdgeListSegments.insert(allEdgeListSegments.end(), localEdgeListSegment.begin(),
                localEdgeListSegment.end());
            localEdgeListSegment.resize(0);
            mutex.unlock();
        }
    }
}

bool BFSSharedState::isBFSComplete(uint64_t numDstNodesToVisit, common::QueryRelType queryRelType) {
    if (currentFrontierSize == 0u) { // no more to extend.
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
    currentFrontierSize = 1u;
    if (isSrcDestination) {
        visitedNodes[srcOffset] = VISITED_DST;
        numVisitedNodes++;
        pathLength[srcOffset] = 0;
    } else {
        visitedNodes[srcOffset] = VISITED;
    }
    isSparseFrontier = true;
    sparseFrontier.push_back(srcOffset);
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
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
    currentLevel++;
    nextScanStartIdx = 0u;
    currentFrontierSize = 0u;
    if (currentLevel < upperBound) {
        currentFrontierSize = nextFrontierSize;
        nextFrontierSize = 0u;
        if (currentFrontierSize < (uint64_t)std::ceil(maxOffset / 8)) {
            isSparseFrontier = true;
            sparseFrontier.clear();
            auto simdWidth = 16u, i = 0u, pos = 0u;
            // SSE2 vector with all elements set to 1
            __m128i ones = _mm_set1_epi8(1);
            for (; i + simdWidth < maxOffset + 1 && pos < currentFrontierSize; i += simdWidth) {
                __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&nextFrontier[i]));
                __m128i cmp = _mm_cmpeq_epi8(vec, ones);
                int mask = _mm_movemask_epi8(cmp);
                while (mask != 0) {
                    int index = __builtin_ctz(mask);
                    sparseFrontier[pos++] = i + index;
                    mask &= ~(1 << index);
                }
            }

            // Process any remaining elements
            for (; i < maxOffset + 1 && pos < currentFrontierSize; ++i) {
                if (nextFrontier[i]) {
                    sparseFrontier[pos++] = i;
                }
            }
        } else {
            currentFrontierSize = maxOffset + 1;
            isSparseFrontier = false;
            if (!denseFrontier) {
                denseFrontier = new uint8_t[currentFrontierSize];
                std::fill(denseFrontier, denseFrontier + maxOffset + 1, 0u);
            }
            auto temp = denseFrontier;
            denseFrontier = nextFrontier;
            nextFrontier = temp;
        }
        std::fill(nextFrontier, nextFrontier + maxOffset + 1, 0u);
    }
    /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
    printf("time taken to move level %d is %lu ms\n", currentLevel, millis1 - millis);*/
}

std::pair<uint64_t, int64_t> BFSSharedState::getDstPathLengthMorsel() {
    auto morselStartIdx =
        nextDstScanStartIdx.fetch_add(common::DEFAULT_VECTOR_CAPACITY, std::memory_order_acq_rel);
    if (morselStartIdx >= visitedNodes.size()) {
        return {UINT64_MAX, INT64_MAX};
    }
    uint64_t morselSize =
        std::min(common::DEFAULT_VECTOR_CAPACITY, visitedNodes.size() - morselStartIdx);
    return {morselStartIdx, morselSize};
}

} // namespace processor
} // namespace kuzu
