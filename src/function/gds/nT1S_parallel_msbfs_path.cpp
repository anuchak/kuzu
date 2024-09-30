#include "function/gds/nT1S_parallel_msbfs_path.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/msbfs_path_ife_morsel.h"
#include "function/gds/parallel_msbfs_commons.h"
#include "function/gds/parallel_utils.h"
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
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSPathIFEMorsel<uint8_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0;
    }
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint8_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            uint8_t oldVal = msbfsIFEMorsel->next[nbrOffset];
                            uint8_t newVal = oldVal | shouldBeActive;
                            while (!__sync_bool_compare_and_swap_1(&msbfsIFEMorsel->next[nbrOffset],
                                oldVal, newVal)) {
                                oldVal = msbfsIFEMorsel->next[nbrOffset];
                                newVal = oldVal | shouldBeActive;
                            }
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exactPos = nbrOffset * 8 + (8 - index);
                                newVal = msbfsIFEMorsel->currentLevel + 1;
                                __atomic_store_n(&msbfsIFEMorsel->pathLength[exactPos], newVal,
                                    __ATOMIC_RELEASE);
                                __atomic_store_n(&msbfsIFEMorsel->parentOffset[exactPos],
                                    offset, __ATOMIC_RELEASE);
                                __atomic_store_n(&msbfsIFEMorsel->edgeOffset[exactPos],
                                    csrEntry->relOffsets[nbrIdx], __ATOMIC_RELEASE);
                                shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index - 1));
                            }
                        }
                    }
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size =
                            nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        auto relIDs = (common::relID_t*)nbrScanState->relIDVector->getData();
                        common::nodeID_t dstNodeID;
                        common::relID_t edgeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint8_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                     ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                uint8_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                uint8_t newVal = oldVal | shouldBeActive;
                                while (!__sync_bool_compare_and_swap_1(
                                    &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    newVal = oldVal | shouldBeActive;
                                }
                                edgeID = relIDs[j];
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exactPos = dstNodeID.offset * 8 + (8 - index);
                                    newVal = msbfsIFEMorsel->currentLevel + 1;
                                    __atomic_store_n(&msbfsIFEMorsel->pathLength[exactPos],
                                        newVal, __ATOMIC_RELEASE);
                                    __atomic_store_n(&msbfsIFEMorsel->parentOffset[exactPos],
                                        offset, __ATOMIC_RELEASE);
                                    __atomic_store_n(&msbfsIFEMorsel->edgeOffset[exactPos],
                                        edgeID.offset, __ATOMIC_RELEASE);
                                    shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
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
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSPathIFEMorsel<uint16_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0;
    }
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint16_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            uint16_t oldVal = msbfsIFEMorsel->next[nbrOffset];
                            uint16_t newVal = oldVal | shouldBeActive;
                            while (!__sync_bool_compare_and_swap_2(&msbfsIFEMorsel->next[nbrOffset],
                                oldVal, newVal)) {
                                oldVal = msbfsIFEMorsel->next[nbrOffset];
                                newVal = oldVal | shouldBeActive;
                            }
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exactPos = nbrOffset * 16 + (16 - index);
                                newVal = msbfsIFEMorsel->currentLevel + 1;
                                __atomic_store_n(&msbfsIFEMorsel->pathLength[exactPos], newVal,
                                    __ATOMIC_RELEASE);
                                __atomic_store_n(&msbfsIFEMorsel->parentOffset[exactPos],
                                    offset, __ATOMIC_RELEASE);
                                __atomic_store_n(&msbfsIFEMorsel->edgeOffset[exactPos],
                                    csrEntry->relOffsets[nbrIdx], __ATOMIC_RELEASE);
                                shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index - 1));
                            }
                        }
                    }
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size =
                            nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        auto relIDs = (common::relID_t *)nbrScanState->relIDVector->getData();
                        common::nodeID_t dstNodeID;
                        common::relID_t edgeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint16_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                      ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                uint16_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                uint16_t newVal = oldVal | shouldBeActive;
                                while (!__sync_bool_compare_and_swap_2(
                                    &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    newVal = oldVal | shouldBeActive;
                                }
                                edgeID = relIDs[j];
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exactPos = dstNodeID.offset * 16 + (16 - index);
                                    newVal = msbfsIFEMorsel->currentLevel + 1;
                                    __atomic_store_n(&msbfsIFEMorsel->pathLength[exactPos],
                                        newVal, __ATOMIC_RELEASE);
                                    __atomic_store_n(&msbfsIFEMorsel->parentOffset[exactPos],
                                        offset, __ATOMIC_RELEASE);
                                    __atomic_store_n(&msbfsIFEMorsel->edgeOffset[exactPos],
                                        edgeID.offset, __ATOMIC_RELEASE);
                                    shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
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
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSPathIFEMorsel<uint32_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0;
    }
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint32_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            uint32_t oldVal = msbfsIFEMorsel->next[nbrOffset];
                            uint32_t newVal = oldVal | shouldBeActive;
                            while (!__sync_bool_compare_and_swap_4(&msbfsIFEMorsel->next[nbrOffset],
                                oldVal, newVal)) {
                                oldVal = msbfsIFEMorsel->next[nbrOffset];
                                newVal = oldVal | shouldBeActive;
                            }
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exactPos = nbrOffset * 32 + (32 - index);
                                newVal = msbfsIFEMorsel->currentLevel + 1;
                                __atomic_store_n(&msbfsIFEMorsel->pathLength[exactPos], newVal,
                                    __ATOMIC_RELEASE);
                                __atomic_store_n(&msbfsIFEMorsel->parentOffset[exactPos],
                                    offset, __ATOMIC_RELEASE);
                                __atomic_store_n(&msbfsIFEMorsel->edgeOffset[exactPos],
                                    csrEntry->relOffsets[nbrIdx], __ATOMIC_RELEASE);
                                shouldBeActive = shouldBeActive ^ ((uint32_t)1 << (index - 1));
                            }
                        }
                    }
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size =
                            nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        auto relIDs = (common::relID_t*)nbrScanState->relIDVector->getData();
                        common::nodeID_t dstNodeID;
                        common::relID_t relID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint32_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                      ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                uint32_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                uint32_t newVal = oldVal | shouldBeActive;
                                while (!__sync_bool_compare_and_swap_4(
                                    &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    newVal = oldVal | shouldBeActive;
                                }
                                relID = relIDs[j];
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exactPos = dstNodeID.offset * 32 + (32 - index);
                                    newVal = msbfsIFEMorsel->currentLevel + 1;
                                    __atomic_store_n(&msbfsIFEMorsel->pathLength[exactPos],
                                        newVal, __ATOMIC_RELEASE);
                                    __atomic_store_n(&msbfsIFEMorsel->parentOffset[exactPos],
                                        offset, __ATOMIC_RELEASE);
                                    __atomic_store_n(&msbfsIFEMorsel->edgeOffset[exactPos],
                                        relID.offset, __ATOMIC_RELEASE);
                                    shouldBeActive = shouldBeActive ^ ((uint32_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
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
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSPathIFEMorsel<uint64_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0;
    }
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint64_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            uint64_t oldVal = msbfsIFEMorsel->next[nbrOffset];
                            uint64_t newVal = oldVal | shouldBeActive;
                            while (!__sync_bool_compare_and_swap_8(&msbfsIFEMorsel->next[nbrOffset],
                                oldVal, newVal)) {
                                oldVal = msbfsIFEMorsel->next[nbrOffset];
                                newVal = oldVal | shouldBeActive;
                            }
                            while (shouldBeActive) {
                                int index = __builtin_ctzll(shouldBeActive) + 1;
                                auto exactPos = nbrOffset * 64 + (64 - index);
                                newVal = msbfsIFEMorsel->currentLevel + 1;
                                __atomic_store_n(&msbfsIFEMorsel->pathLength[exactPos], newVal,
                                    __ATOMIC_RELEASE);
                                __atomic_store_n(&msbfsIFEMorsel->parentOffset[exactPos],
                                    offset, __ATOMIC_RELEASE);
                                __atomic_store_n(&msbfsIFEMorsel->edgeOffset[exactPos],
                                    csrEntry->relOffsets[nbrIdx], __ATOMIC_RELEASE);
                                shouldBeActive = shouldBeActive ^ ((uint64_t)1 << (index - 1));
                            }
                        }
                    }
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size =
                            nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        auto relIDs = (common::relID_t*)nbrScanState->relIDVector->getData();
                        common::nodeID_t dstNodeID;
                        common::relID_t relID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint64_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                      ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                uint64_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                uint64_t newVal = oldVal | shouldBeActive;
                                while (!__sync_bool_compare_and_swap_8(
                                    &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    newVal = oldVal | shouldBeActive;
                                }
                                relID = relIDs[j];
                                while (shouldBeActive) {
                                    int index = __builtin_ctzll(shouldBeActive) + 1;
                                    auto exactPos = dstNodeID.offset * 64 + (64 - index);
                                    newVal = msbfsIFEMorsel->currentLevel + 1;
                                    __atomic_store_n(&msbfsIFEMorsel->pathLength[exactPos],
                                        newVal, __ATOMIC_RELEASE);
                                    __atomic_store_n(&msbfsIFEMorsel->parentOffset[exactPos],
                                        offset, __ATOMIC_RELEASE);
                                    __atomic_store_n(&msbfsIFEMorsel->edgeOffset[exactPos],
                                        relID.offset, __ATOMIC_RELEASE);
                                    shouldBeActive = shouldBeActive ^ ((uint64_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
            }
            frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        }
    }
    return 0u;
}

static uint64_t shortestPathOutputLane8Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (MSBFSPathIFEMorsel<uint8_t>*)shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane & 0x8) {
        currentDstLane = 0u;
        morsel = ifeMorsel->getDstWriteMorsel(common::DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0u;
        }
    }
    auto nodeTableID = sharedState->graph->getNodeTableID();
    auto edgeTableID = sharedState->graph->getRelTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    auto& pathVector = shortestPathLocalState->pathVector;
    auto pathDataVector = ListVector::getDataVector(pathVector.get());
    if (pathVector) {
        pathVector->resetAuxiliaryBuffer();
    }
    srcNodeVector->setValue<nodeID_t>(0,
        common::nodeID_t{ifeMorsel->srcOffsets[currentDstLane], nodeTableID});
    auto pos = 0;
    auto srcOffset = ifeMorsel->srcOffsets[currentDstLane];
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto exactPos = offset * 8 + (7 - currentDstLane);
        uint64_t pathLength = ifeMorsel->pathLength[exactPos];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, nodeTableID});
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            // Max length of intermediate path can be (2 * upper_bound - 1)
            // There can be (upper_bound) no. of edges and (upper_bound - 1) no. of nodes
            auto listEntry = ListVector::addList(pathVector.get(), 2*pathLength - 1);
            pathVector->setValue<list_entry_t>(pos, listEntry);
            pos++;
            auto parentOffset = ifeMorsel->parentOffset[exactPos],
                 edgeOffset = ifeMorsel->edgeOffset[exactPos];
            auto pathIdx = 2 * pathLength - 2;
            while (parentOffset != srcOffset) {
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                    {edgeOffset, edgeTableID});
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx - 1,
                    {parentOffset, nodeTableID});
                pathIdx -= 2;
                exactPos = parentOffset * 8 + (7 - currentDstLane);
                edgeOffset = ifeMorsel->edgeOffset[exactPos];
                parentOffset = ifeMorsel->parentOffset[exactPos];
            }
            pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                {edgeOffset, edgeTableID});
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos;
}

static uint64_t shortestPathOutputLane16Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (MSBFSPathIFEMorsel<uint16_t>*)shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane & 0x10) {
        currentDstLane = 0u;
        morsel = ifeMorsel->getDstWriteMorsel(common::DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0u;
        }
    }
    auto nodeTableID = sharedState->graph->getNodeTableID();
    auto edgeTableID = sharedState->graph->getRelTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    auto& pathVector = shortestPathLocalState->pathVector;
    auto pathDataVector = ListVector::getDataVector(pathVector.get());
    if (pathVector) {
        pathVector->resetAuxiliaryBuffer();
    }
    srcNodeVector->setValue<nodeID_t>(0,
        common::nodeID_t{ifeMorsel->srcOffsets[currentDstLane], nodeTableID});
    auto pos = 0;
    auto srcOffset = ifeMorsel->srcOffsets[currentDstLane];
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto exactPos = offset * 16 + (15 - currentDstLane);
        uint64_t pathLength = ifeMorsel->pathLength[exactPos];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, nodeTableID});
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            // Max length of intermediate path can be (2 * upper_bound - 1)
            // There can be (upper_bound) no. of edges and (upper_bound - 1) no. of nodes
            auto listEntry = ListVector::addList(pathVector.get(), 2*pathLength - 1);
            pathVector->setValue<list_entry_t>(pos, listEntry);
            pos++;
            auto parentOffset = ifeMorsel->parentOffset[exactPos],
                 edgeOffset = ifeMorsel->edgeOffset[exactPos];
            auto pathIdx = 2 * pathLength - 2;
            while (parentOffset != srcOffset) {
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                    {edgeOffset, edgeTableID});
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx - 1,
                    {parentOffset, nodeTableID});
                pathIdx -= 2;
                exactPos = parentOffset * 16 + (15 - currentDstLane);
                edgeOffset = ifeMorsel->edgeOffset[exactPos];
                parentOffset = ifeMorsel->parentOffset[exactPos];
            }
            pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                {edgeOffset, edgeTableID});
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos;
}

static uint64_t shortestPathOutputLane32Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (MSBFSPathIFEMorsel<uint32_t>*)shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane & 0x20) {
        currentDstLane = 0u;
        morsel = ifeMorsel->getDstWriteMorsel(common::DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0u;
        }
    }
    auto nodeTableID = sharedState->graph->getNodeTableID();
    auto edgeTableID = sharedState->graph->getRelTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    auto& pathVector = shortestPathLocalState->pathVector;
    auto pathDataVector = ListVector::getDataVector(pathVector.get());
    if (pathVector) {
        pathVector->resetAuxiliaryBuffer();
    }
    srcNodeVector->setValue<nodeID_t>(0,
        common::nodeID_t{ifeMorsel->srcOffsets[currentDstLane], nodeTableID});
    auto pos = 0;
    auto srcOffset = ifeMorsel->srcOffsets[currentDstLane];
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto exactPos = offset * 32 + (31 - currentDstLane);
        uint64_t pathLength = ifeMorsel->pathLength[exactPos];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, nodeTableID});
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            // Max length of intermediate path can be (2 * upper_bound - 1)
            // There can be (upper_bound) no. of edges and (upper_bound - 1) no. of nodes
            auto listEntry = ListVector::addList(pathVector.get(), 2*pathLength - 1);
            pathVector->setValue<list_entry_t>(pos, listEntry);
            pos++;
            auto parentOffset = ifeMorsel->parentOffset[exactPos],
                 edgeOffset = ifeMorsel->edgeOffset[exactPos];
            auto pathIdx = 2 * pathLength - 2;
            while (parentOffset != srcOffset) {
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                    {edgeOffset, edgeTableID});
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx - 1,
                    {parentOffset, nodeTableID});
                pathIdx -= 2;
                exactPos = parentOffset * 32 + (31 - currentDstLane);
                edgeOffset = ifeMorsel->edgeOffset[exactPos];
                parentOffset = ifeMorsel->parentOffset[exactPos];
            }
            pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                {edgeOffset, edgeTableID});
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos;
}

static uint64_t shortestPathOutputLane64Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (MSBFSPathIFEMorsel<uint64_t>*)shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane & 0x40) {
        currentDstLane = 0u;
        morsel = ifeMorsel->getDstWriteMorsel(common::DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0u;
        }
    }
    auto nodeTableID = sharedState->graph->getNodeTableID();
    auto edgeTableID = sharedState->graph->getRelTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    auto& pathVector = shortestPathLocalState->pathVector;
    auto pathDataVector = ListVector::getDataVector(pathVector.get());
    if (pathVector) {
        pathVector->resetAuxiliaryBuffer();
    }
    srcNodeVector->setValue<nodeID_t>(0,
        common::nodeID_t{ifeMorsel->srcOffsets[currentDstLane], nodeTableID});
    auto pos = 0;
    auto srcOffset = ifeMorsel->srcOffsets[currentDstLane];
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto exactPos = offset * 64 + (63 - currentDstLane);
        uint64_t pathLength = ifeMorsel->pathLength[exactPos];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, nodeTableID});
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            // Max length of intermediate path can be (2 * upper_bound - 1)
            // There can be (upper_bound) no. of edges and (upper_bound - 1) no. of nodes
            auto listEntry = ListVector::addList(pathVector.get(), 2*pathLength - 1);
            pathVector->setValue<list_entry_t>(pos, listEntry);
            pos++;
            auto parentOffset = ifeMorsel->parentOffset[exactPos],
                 edgeOffset = ifeMorsel->edgeOffset[exactPos];
            auto pathIdx = 2 * pathLength - 2;
            while (parentOffset != srcOffset) {
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                    {edgeOffset, edgeTableID});
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx - 1,
                    {parentOffset, nodeTableID});
                exactPos = parentOffset * 64 + (63 - currentDstLane);
                pathIdx -= 2;
                edgeOffset = ifeMorsel->edgeOffset[exactPos];
                parentOffset = ifeMorsel->parentOffset[exactPos];
            }
            pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                {edgeOffset, edgeTableID});
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos;
}

void nT1SParallelMSBFSPath::exec() {
    auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
    auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
    auto laneWidth = extraData->laneWidth;
    auto numNodes = sharedState->graph->getNumNodes();
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    if (laneWidth == 8) {
        auto ifeMorsel =
            std::make_unique<MSBFSPathIFEMorsel<uint8_t>>(extraData->upperBound, 1, numNodes - 1);
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
            std::make_unique<MSBFSPathIFEMorsel<uint16_t>>(extraData->upperBound, 1, numNodes - 1);
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
            std::make_unique<MSBFSPathIFEMorsel<uint32_t>>(extraData->upperBound, 1, numNodes - 1);
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
            std::make_unique<MSBFSPathIFEMorsel<uint64_t>>(extraData->upperBound, 1, numNodes - 1);
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
