#include "function/gds/nTkS_parallel_varlen_msbfs_path.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/msbfs_varlen_path_ife_morsel.h"
#include "function/gds/parallel_msbfs_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/parallel_var_len_commons.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

static uint64_t extendFrontierLane8Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelVarLenLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSVarlenPathIFEMorsel<uint8_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0;
    }
    auto parentNodeListAndLevels = std::vector<edgeListAndLevel*>();
    auto activeLanes = std::vector<int>();
    parentNodeListAndLevels.reserve(8);
    activeLanes.reserve(8);
    if (graph->isInMemory) {
        // TODO: Leaving in memory version not complete for now
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        auto& localEdgeListSegment = shortestPathLocalState->localEdgeListSegment;
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    activeLanes.clear();
                    parentNodeListAndLevels.clear();
                    auto currentFrontierVal = msbfsIFEMorsel->current[offset];
                    while (currentFrontierVal) {
                        int index = __builtin_ctz(currentFrontierVal);
                        activeLanes.push_back(index);
                        auto exactPos = offset * 8 + (7 - index);
                        auto nodeEdgeListAndLevel = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos];
                        while (nodeEdgeListAndLevel->bfsLevel != msbfsIFEMorsel->currentLevel) {
                            nodeEdgeListAndLevel = nodeEdgeListAndLevel->next;
                        }
                        parentNodeListAndLevels.push_back(nodeEdgeListAndLevel);
                        currentFrontierVal = currentFrontierVal ^ ((uint8_t)1 << index);
                    }
                    auto totalActiveLanes = activeLanes.size();
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto newEdgeListSegment = new edgeListSegment(size * activeLanes.size());
                        localEdgeListSegment.push_back(newEdgeListSegment);
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        auto relIDs = (common::relID_t*)nbrScanState->relIDVector->getData();
                        common::nodeID_t dstNodeID;
                        common::relID_t edgeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint8_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->next[dstNodeID.offset];
                            if (shouldBeActive) {
                                uint8_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                uint8_t newVal = oldVal | msbfsIFEMorsel->current[dstNodeID.offset];
                                while (!__sync_bool_compare_and_swap_1(
                                    &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    newVal = oldVal | msbfsIFEMorsel->current[dstNodeID.offset];
                                }
                            }
                            edgeID = relIDs[j];
                            for (auto i = 0u; i < totalActiveLanes; i++) {
                                auto exactPos = dstNodeID.offset * 8 + (7 - activeLanes[i]);
                                auto topEntry = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos];
                                if (!topEntry ||
                                    (topEntry->bfsLevel <= msbfsIFEMorsel->currentLevel)) {
                                    auto newEntry = new edgeListAndLevel(msbfsIFEMorsel->currentLevel + 1,
                                        dstNodeID.offset, topEntry);
                                    if (__sync_bool_compare_and_swap(&msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos],
                                            topEntry, newEntry)) {
                                        newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
                                    } else {
                                        delete newEntry;
                                    }
                                }
                                auto edgeListBlockPos = j * totalActiveLanes + i;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].edgeOffset = edgeID.offset;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].src = parentNodeListAndLevels[i];
                                auto currTopEdgeList = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].next = currTopEdgeList;
                                while (!__sync_bool_compare_and_swap(&msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top,
                                    currTopEdgeList, &newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos])) {
                                    currTopEdgeList = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top;
                                    newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].next = currTopEdgeList;
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
                if (!localEdgeListSegment.empty()) {
                    msbfsIFEMorsel->mergeResults(localEdgeListSegment);
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    }
    return 0u;
}

static uint64_t extendFrontierLane16Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelVarLenLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSVarlenPathIFEMorsel<uint16_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0;
    }
    auto parentNodeListAndLevels = std::vector<edgeListAndLevel*>();
    auto activeLanes = std::vector<int>();
    parentNodeListAndLevels.reserve(16);
    activeLanes.reserve(16);
    if (graph->isInMemory) {
        // TODO: Leaving in memory version not complete for now
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        auto& localEdgeListSegment = shortestPathLocalState->localEdgeListSegment;
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    activeLanes.clear();
                    parentNodeListAndLevels.clear();
                    auto currentFrontierVal = msbfsIFEMorsel->current[offset];
                    while (currentFrontierVal) {
                        int index = __builtin_ctz(currentFrontierVal);
                        activeLanes.push_back(index);
                        auto exactPos = offset * 16 + (15 - index);
                        auto nodeEdgeListAndLevel = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos];
                        while (nodeEdgeListAndLevel->bfsLevel != msbfsIFEMorsel->currentLevel) {
                            nodeEdgeListAndLevel = nodeEdgeListAndLevel->next;
                        }
                        parentNodeListAndLevels.push_back(nodeEdgeListAndLevel);
                        currentFrontierVal = currentFrontierVal ^ ((uint16_t)1 << index);
                    }
                    auto totalActiveLanes = activeLanes.size();
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto newEdgeListSegment = new edgeListSegment(size * activeLanes.size());
                        localEdgeListSegment.push_back(newEdgeListSegment);
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        auto relIDs = (common::relID_t*)nbrScanState->relIDVector->getData();
                        common::nodeID_t dstNodeID;
                        common::relID_t edgeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint16_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->next[dstNodeID.offset];
                            if (shouldBeActive) {
                                uint16_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                uint16_t newVal = oldVal | msbfsIFEMorsel->current[dstNodeID.offset];
                                while (!__sync_bool_compare_and_swap_2(
                                    &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    newVal = oldVal | msbfsIFEMorsel->current[dstNodeID.offset];
                                }
                            }
                            edgeID = relIDs[j];
                            for (auto i = 0u; i < totalActiveLanes; i++) {
                                auto exactPos = dstNodeID.offset * 16 + (15 - activeLanes[i]);
                                auto topEntry = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos];
                                if (!topEntry ||
                                    (topEntry->bfsLevel <= msbfsIFEMorsel->currentLevel)) {
                                    auto newEntry = new edgeListAndLevel(msbfsIFEMorsel->currentLevel + 1,
                                        dstNodeID.offset, topEntry);
                                    if (__sync_bool_compare_and_swap(&msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos],
                                            topEntry, newEntry)) {
                                        newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
                                    } else {
                                        delete newEntry;
                                    }
                                }
                                auto edgeListBlockPos = j * totalActiveLanes + i;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].edgeOffset = edgeID.offset;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].src = parentNodeListAndLevels[i];
                                auto currTopEdgeList = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].next = currTopEdgeList;
                                while (!__sync_bool_compare_and_swap(&msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top,
                                    currTopEdgeList, &newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos])) {
                                    currTopEdgeList = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top;
                                    newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].next = currTopEdgeList;
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
                if (!localEdgeListSegment.empty()) {
                    msbfsIFEMorsel->mergeResults(localEdgeListSegment);
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    }
    return 0u;
}

static uint64_t extendFrontierLane32Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelVarLenLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSVarlenPathIFEMorsel<uint32_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0;
    }
    auto parentNodeListAndLevels = std::vector<edgeListAndLevel*>();
    auto activeLanes = std::vector<int>();
    parentNodeListAndLevels.reserve(32);
    activeLanes.reserve(32);
    if (graph->isInMemory) {
        // TODO: Leaving in memory version not complete for now
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        auto& localEdgeListSegment = shortestPathLocalState->localEdgeListSegment;
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    activeLanes.clear();
                    parentNodeListAndLevels.clear();
                    auto currentFrontierVal = msbfsIFEMorsel->current[offset];
                    while (currentFrontierVal) {
                        int index = __builtin_ctz(currentFrontierVal);
                        activeLanes.push_back(index);
                        auto exactPos = offset * 32 + (31 - index);
                        auto nodeEdgeListAndLevel = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos];
                        while (nodeEdgeListAndLevel->bfsLevel != msbfsIFEMorsel->currentLevel) {
                            nodeEdgeListAndLevel = nodeEdgeListAndLevel->next;
                        }
                        parentNodeListAndLevels.push_back(nodeEdgeListAndLevel);
                        currentFrontierVal = currentFrontierVal ^ ((uint32_t)1 << index);
                    }
                    auto totalActiveLanes = activeLanes.size();
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto newEdgeListSegment = new edgeListSegment(size * activeLanes.size());
                        localEdgeListSegment.push_back(newEdgeListSegment);
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        auto relIDs = (common::relID_t*)nbrScanState->relIDVector->getData();
                        common::nodeID_t dstNodeID;
                        common::relID_t edgeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint32_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->next[dstNodeID.offset];
                            if (shouldBeActive) {
                                uint32_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                uint32_t newVal = oldVal | msbfsIFEMorsel->current[dstNodeID.offset];
                                while (!__sync_bool_compare_and_swap_4(
                                    &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    newVal = oldVal | msbfsIFEMorsel->current[dstNodeID.offset];
                                }
                            }
                            edgeID = relIDs[j];
                            for (auto i = 0u; i < totalActiveLanes; i++) {
                                auto exactPos = dstNodeID.offset * 32 + (31 - activeLanes[i]);
                                auto topEntry = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos];
                                if (!topEntry ||
                                    (topEntry->bfsLevel <= msbfsIFEMorsel->currentLevel)) {
                                    auto newEntry = new edgeListAndLevel(msbfsIFEMorsel->currentLevel + 1,
                                        dstNodeID.offset, topEntry);
                                    if (__sync_bool_compare_and_swap(&msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos],
                                            topEntry, newEntry)) {
                                        newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
                                    } else {
                                        delete newEntry;
                                    }
                                }
                                auto edgeListBlockPos = j * totalActiveLanes + i;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].edgeOffset = edgeID.offset;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].src = parentNodeListAndLevels[i];
                                auto currTopEdgeList = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].next = currTopEdgeList;
                                while (!__sync_bool_compare_and_swap(&msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top,
                                    currTopEdgeList, &newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos])) {
                                    currTopEdgeList = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top;
                                    newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].next = currTopEdgeList;
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
                if (!localEdgeListSegment.empty()) {
                    msbfsIFEMorsel->mergeResults(localEdgeListSegment);
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    }
    return 0u;
}

static uint64_t extendFrontierLane64Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelVarLenLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSVarlenPathIFEMorsel<uint64_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0;
    }
    auto parentNodeListAndLevels = std::vector<edgeListAndLevel*>();
    auto activeLanes = std::vector<int>();
    parentNodeListAndLevels.reserve(64);
    activeLanes.reserve(64);
    if (graph->isInMemory) {
        // TODO: Leaving in memory version not complete for now
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        auto& localEdgeListSegment = shortestPathLocalState->localEdgeListSegment;
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    activeLanes.clear();
                    parentNodeListAndLevels.clear();
                    auto currentFrontierVal = msbfsIFEMorsel->current[offset];
                    while (currentFrontierVal) {
                        int index = __builtin_ctzll(currentFrontierVal);
                        activeLanes.push_back(index);
                        auto exactPos = offset * 64 + (63 - index);
                        auto nodeEdgeListAndLevel = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos];
                        while (nodeEdgeListAndLevel->bfsLevel != msbfsIFEMorsel->currentLevel) {
                            nodeEdgeListAndLevel = nodeEdgeListAndLevel->next;
                        }
                        parentNodeListAndLevels.push_back(nodeEdgeListAndLevel);
                        currentFrontierVal = currentFrontierVal ^ ((uint64_t)1 << index);
                    }
                    auto totalActiveLanes = activeLanes.size();
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto newEdgeListSegment = new edgeListSegment(size * activeLanes.size());
                        localEdgeListSegment.push_back(newEdgeListSegment);
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        auto relIDs = (common::relID_t*)nbrScanState->relIDVector->getData();
                        common::nodeID_t dstNodeID;
                        common::relID_t edgeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint64_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->next[dstNodeID.offset];
                            if (shouldBeActive) {
                                uint64_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                uint64_t newVal = oldVal | msbfsIFEMorsel->current[dstNodeID.offset];
                                while (!__sync_bool_compare_and_swap_8(
                                    &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    newVal = oldVal | msbfsIFEMorsel->current[dstNodeID.offset];
                                }
                            }
                            edgeID = relIDs[j];
                            for (auto i = 0u; i < totalActiveLanes; i++) {
                                auto exactPos = dstNodeID.offset * 64 + (63 - activeLanes[i]);
                                auto topEntry = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos];
                                if (!topEntry ||
                                    (topEntry->bfsLevel <= msbfsIFEMorsel->currentLevel)) {
                                    auto newEntry = new edgeListAndLevel(msbfsIFEMorsel->currentLevel + 1,
                                        dstNodeID.offset, topEntry);
                                    if (__sync_bool_compare_and_swap(&msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos],
                                            topEntry, newEntry)) {
                                        newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
                                    } else {
                                        delete newEntry;
                                    }
                                }
                                auto edgeListBlockPos = j * totalActiveLanes + i;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].edgeOffset = edgeID.offset;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].src = parentNodeListAndLevels[i];
                                auto currTopEdgeList = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top;
                                newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].next = currTopEdgeList;
                                while (!__sync_bool_compare_and_swap(&msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top,
                                    currTopEdgeList, &newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos])) {
                                    currTopEdgeList = msbfsIFEMorsel->nodeIDEdgeListAndLevel[exactPos]->top;
                                    newEdgeListSegment->edgeListBlockPtr[edgeListBlockPos].next = currTopEdgeList;
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
                if (!localEdgeListSegment.empty()) {
                    msbfsIFEMorsel->mergeResults(localEdgeListSegment);
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    }
    return 0u;
}

static uint64_t shortestPathOutputLane8Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {

}

static uint64_t shortestPathOutputLane16Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {

}

static uint64_t shortestPathOutputLane32Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {

}

static uint64_t shortestPathOutputLane64Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {

}

void nTkSParallelVarlenMSBFSPath::exec() {
    auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
    auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
    auto laneWidth = extraData->laneWidth;
    auto numNodes = sharedState->graph->getNumNodes();
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    if (laneWidth == 8) {
        auto ifeMorsel =
            std::make_unique<MSBFSVarlenPathIFEMorsel<uint8_t>>(extraData->upperBound, 1, numNodes - 1);
        auto count = 0;
        for (auto offset = 0u; offset < numNodes; offset++) {
            if (!inputMask->isMasked(offset)) {
                continue;
            }
            ifeMorsel->srcOffsets.push_back(offset);
            count++;
            if (count == laneWidth) {
                /*auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                while (!ifeMorsel->isBFSCompleteNoLock()) {
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane8Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                        millis1 - millis);
                    millis = millis1;*/
                    ifeMorsel->initializeNextFrontierNoLock();
                }
                /*duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    shortestPathOutputLane8Func, maxThreads};
                parallelUtils->submitParallelTaskAndWait(job);
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("output writing completed in %lu ms\n", millis1 - millis);
                printf("sources: [");
                for (auto& offset_ : ifeMorsel->srcOffsets) {
                    printf("%lu, ", offset_);
                }
                printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);*/
                ifeMorsel->srcOffsets.clear();
                count = 0;
            }
        }
        // There are some BFS sources still remaining, full 8-lane not utilized.
        // But the remaining sources need to be completed.
        if (count > 0) {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            while (!ifeMorsel->isBFSCompleteNoLock()) {
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    extendFrontierLane8Func, maxThreads};
                parallelUtils->submitParallelTaskAndWait(job);
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                    millis1 - millis);*/
                ifeMorsel->initializeNextFrontierNoLock();
                // millis = millis1;
            }
            /*duration = std::chrono::system_clock::now().time_since_epoch();
            millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                shortestPathOutputLane8Func, maxThreads};
            parallelUtils->submitParallelTaskAndWait(job);
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("output writing completed in %lu ms\n", millis1 - millis);
            printf("sources: [");
            for (auto& offset_ : ifeMorsel->srcOffsets) {
                printf("%lu, ", offset_);
            }
            printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);*/
            ifeMorsel->srcOffsets.clear();
        }
    } else if (laneWidth == 16) {
        auto ifeMorsel =
            std::make_unique<MSBFSVarlenPathIFEMorsel<uint16_t>>(extraData->upperBound, 1, numNodes - 1);
        auto count = 0;
        for (auto offset = 0u; offset < numNodes; offset++) {
            if (!inputMask->isMasked(offset)) {
                continue;
            }
            ifeMorsel->srcOffsets.push_back(offset);
            count++;
            if (count == laneWidth) {
                /*auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                while (!ifeMorsel->isBFSCompleteNoLock()) {
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane16Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                        millis1 - millis);*/
                    ifeMorsel->initializeNextFrontierNoLock();
                    // millis = millis1;
                }
                /*duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    shortestPathOutputLane16Func, maxThreads};
                parallelUtils->submitParallelTaskAndWait(job);
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("output writing completed in %lu ms\n", millis1 - millis);
                printf("sources: [");
                for (auto& offset_ : ifeMorsel->srcOffsets) {
                    printf("%lu, ", offset_);
                }
                printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);*/
                ifeMorsel->srcOffsets.clear();
                count = 0;
            }
        }
        // There are some BFS sources still remaining, full 8-lane not utilized.
        // But the remaining sources need to be completed.
        if (count > 0) {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            while (!ifeMorsel->isBFSCompleteNoLock()) {
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    extendFrontierLane16Func, maxThreads};
                parallelUtils->submitParallelTaskAndWait(job);
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                    millis1 - millis);*/
                ifeMorsel->initializeNextFrontierNoLock();
                // millis = millis1;
            }
            /*duration = std::chrono::system_clock::now().time_since_epoch();
            millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                shortestPathOutputLane16Func, maxThreads};
            parallelUtils->submitParallelTaskAndWait(job);
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("output writing completed in %lu ms\n", millis1 - millis);
            printf("sources: [");
            for (auto& offset_ : ifeMorsel->srcOffsets) {
                printf("%lu, ", offset_);
            }
            printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);*/
            ifeMorsel->srcOffsets.clear();
        }
    } else if (laneWidth == 32) {
        auto ifeMorsel =
            std::make_unique<MSBFSVarlenPathIFEMorsel<uint32_t>>(extraData->upperBound, 1, numNodes - 1);
        auto count = 0;
        for (auto offset = 0u; offset < numNodes; offset++) {
            if (!inputMask->isMasked(offset)) {
                continue;
            }
            ifeMorsel->srcOffsets.push_back(offset);
            count++;
            if (count == laneWidth) {
                /*auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                while (!ifeMorsel->isBFSCompleteNoLock()) {
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane32Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                        millis1 - millis);*/
                    ifeMorsel->initializeNextFrontierNoLock();
                    // millis = millis1;
                }
                /*duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    shortestPathOutputLane32Func, maxThreads};
                parallelUtils->submitParallelTaskAndWait(job);
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("output writing completed in %lu ms\n", millis1 - millis);
                printf("sources: [");
                for (auto& offset_ : ifeMorsel->srcOffsets) {
                    printf("%lu, ", offset_);
                }
                printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);*/
                ifeMorsel->srcOffsets.clear();
                count = 0;
            }
        }
        // There are some BFS sources still remaining, full 8-lane not utilized.
        // But the remaining sources need to be completed.
        if (count > 0) {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            while (!ifeMorsel->isBFSCompleteNoLock()) {
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    extendFrontierLane32Func, maxThreads};
                parallelUtils->submitParallelTaskAndWait(job);
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                    millis1 - millis);*/
                ifeMorsel->initializeNextFrontierNoLock();
                // millis = millis1;
            }
            /*duration = std::chrono::system_clock::now().time_since_epoch();
            millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                shortestPathOutputLane32Func, maxThreads};
            parallelUtils->submitParallelTaskAndWait(job);
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("output writing completed in %lu ms\n", millis1 - millis);
            printf("sources: [");
            for (auto& offset_ : ifeMorsel->srcOffsets) {
                printf("%lu, ", offset_);
            }
            printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);*/
            ifeMorsel->srcOffsets.clear();
        }
    } else {
        auto ifeMorsel =
            std::make_unique<MSBFSVarlenPathIFEMorsel<uint64_t>>(extraData->upperBound, 1, numNodes - 1);
        auto count = 0;
        for (auto offset = 0u; offset < numNodes; offset++) {
            if (!inputMask->isMasked(offset)) {
                continue;
            }
            ifeMorsel->srcOffsets.push_back(offset);
            count++;
            if (count == laneWidth) {
                /*auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                while (!ifeMorsel->isBFSCompleteNoLock()) {
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane64Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                        millis1 - millis);*/
                    ifeMorsel->initializeNextFrontierNoLock();
                    // millis = millis1;
                }
                /*duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    shortestPathOutputLane64Func, maxThreads};
                parallelUtils->submitParallelTaskAndWait(job);
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("output writing completed in %lu ms\n", millis1 - millis);
                printf("sources: [");
                for (auto& offset_ : ifeMorsel->srcOffsets) {
                    printf("%lu, ", offset_);
                }
                printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);*/
                ifeMorsel->srcOffsets.clear();
                count = 0;
            }
        }
        // There are some BFS sources still remaining, full 8-lane not utilized.
        // But the remaining sources need to be completed.
        if (count > 0) {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            while (!ifeMorsel->isBFSCompleteNoLock()) {
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    extendFrontierLane64Func, maxThreads};
                parallelUtils->submitParallelTaskAndWait(job);
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                    millis1 - millis);*/
                ifeMorsel->initializeNextFrontierNoLock();
                // millis = millis1;
            }
            /*duration = std::chrono::system_clock::now().time_since_epoch();
            millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                shortestPathOutputLane64Func, maxThreads};
            parallelUtils->submitParallelTaskAndWait(job);
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("output writing completed in %lu ms\n", millis1 - millis);
            printf("sources: [");
            for (auto& offset_ : ifeMorsel->srcOffsets) {
                printf("%lu, ", offset_);
            }
            printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);*/
            ifeMorsel->srcOffsets.clear();
        }
    }
}

} // namespace function
} // namespace kuzu
