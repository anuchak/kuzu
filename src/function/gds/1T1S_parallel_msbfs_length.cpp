#include "function/gds/1T1S_parallel_msbfs_length.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/msbfs_ife_morsel.h"
#include "function/gds/parallel_msbfs_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

static uint64_t extendFrontierLane8SingleFunc(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSIFEMorsel<uint8_t>*>(
        shortestPathLocalState->ifeMorsel);
    if (!msbfsIFEMorsel->initializedIFEMorsel) {
        msbfsIFEMorsel->init();
    }
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint8_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            msbfsIFEMorsel->next[nbrOffset] |= shouldBeActive;
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exact1BytePos = nbrOffset * 8 + (8 - index);
                                msbfsIFEMorsel->pathLength[exact1BytePos] =
                                    msbfsIFEMorsel->currentLevel + 1;
                                shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index - 1));
                            }
                        }
                    }
                }
            }
            msbfsIFEMorsel->initializeNextFrontierNoLock();
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size =
                            nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        common::nodeID_t dstNodeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint8_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                     ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = dstNodeID.offset * 8 + (8 - index);
                                    msbfsIFEMorsel->pathLength[exact1BytePos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
            }
            msbfsIFEMorsel->initializeNextFrontierNoLock();
        }
    }
    return 0u;
}

static uint64_t extendFrontierLane16SingleFunc(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSIFEMorsel<uint16_t>*>(
        shortestPathLocalState->ifeMorsel);
    if (!msbfsIFEMorsel->initializedIFEMorsel) {
        msbfsIFEMorsel->init();
    }
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint16_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            msbfsIFEMorsel->next[nbrOffset] |= shouldBeActive;
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exact1BytePos = nbrOffset * 16 + (16 - index);
                                msbfsIFEMorsel->pathLength[exact1BytePos] =
                                    msbfsIFEMorsel->currentLevel + 1;
                                shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index - 1));
                            }
                        }
                    }
                }
            }
            msbfsIFEMorsel->initializeNextFrontierNoLock();
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size =
                            nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        common::nodeID_t dstNodeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint16_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                      ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = dstNodeID.offset * 16 + (16 - index);
                                    msbfsIFEMorsel->pathLength[exact1BytePos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
            }
            msbfsIFEMorsel->initializeNextFrontierNoLock();
        }
    }
    return 0u;
}

static uint64_t extendFrontierLane32SingleFunc(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSIFEMorsel<uint32_t>*>(
        shortestPathLocalState->ifeMorsel);
    if (!msbfsIFEMorsel->initializedIFEMorsel) {
        msbfsIFEMorsel->init();
    }
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint32_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            msbfsIFEMorsel->next[nbrOffset] |= shouldBeActive;
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exact1BytePos = nbrOffset * 32 + (32 - index);
                                msbfsIFEMorsel->pathLength[exact1BytePos] =
                                    msbfsIFEMorsel->currentLevel + 1;
                                shouldBeActive = shouldBeActive ^ ((uint32_t)1 << (index - 1));
                            }
                        }
                    }
                }
            }
            msbfsIFEMorsel->initializeNextFrontierNoLock();
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size =
                            nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        common::nodeID_t dstNodeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint32_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                      ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = dstNodeID.offset * 32 + (32 - index);
                                    msbfsIFEMorsel->pathLength[exact1BytePos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    shouldBeActive = shouldBeActive ^ ((uint32_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
            }
            msbfsIFEMorsel->initializeNextFrontierNoLock();
        }
    }
    return 0u;
}

static uint64_t extendFrontierLane64SingleFunc(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSIFEMorsel<uint64_t>*>(
        shortestPathLocalState->ifeMorsel);
    if (!msbfsIFEMorsel->initializedIFEMorsel) {
        msbfsIFEMorsel->init();
    }
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint64_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            msbfsIFEMorsel->next[nbrOffset] |= shouldBeActive;
                            while (shouldBeActive) {
                                int index = __builtin_ctzll(shouldBeActive) + 1;
                                auto exact1BytePos = nbrOffset * 64 + (64 - index);
                                msbfsIFEMorsel->pathLength[exact1BytePos] =
                                    msbfsIFEMorsel->currentLevel + 1;
                                shouldBeActive = shouldBeActive ^ ((uint64_t)1 << (index - 1));
                            }
                        }
                    }
                }
            }
            msbfsIFEMorsel->initializeNextFrontierNoLock();
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                    do {
                        graph->getFwdNbrs(nbrScanState.get());
                        auto size =
                            nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                        auto nbrNodes = (common::nodeID_t*)nbrScanState->dstNodeIDVector->getData();
                        common::nodeID_t dstNodeID;
                        for (auto j = 0u; j < size; j++) {
                            dstNodeID = nbrNodes[j];
                            uint64_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                      ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                while (shouldBeActive) {
                                    int index = __builtin_ctzll(shouldBeActive) + 1;
                                    auto exact1BytePos = dstNodeID.offset * 64 + (64 - index);
                                    msbfsIFEMorsel->pathLength[exact1BytePos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    shouldBeActive = shouldBeActive ^ ((uint64_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
            }
            msbfsIFEMorsel->initializeNextFrontierNoLock();
        }
    }
    return 0u;
}

static uint64_t shortestPathOutputLane8FuncSingle(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (MSBFSIFEMorsel<uint8_t>*)shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane & 0x8) {
        currentDstLane = 0u;
        morsel = ifeMorsel->getDstWriteMorsel(common::DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0u;
        }
    }
    auto tableID = sharedState->graph->getNodeTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    srcNodeVector->setValue<nodeID_t>(0,
        common::nodeID_t{ifeMorsel->srcOffsets[currentDstLane], tableID});
    auto pos = 0;
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto exactPathLengthPos = offset * 8 + (7 - currentDstLane);
        uint64_t pathLength = ifeMorsel->pathLength[exactPathLengthPos];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, tableID});
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            pos++;
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos;
}

static uint64_t shortestPathOutputLane16FuncSingle(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (MSBFSIFEMorsel<uint16_t>*)shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane & 0x10) {
        currentDstLane = 0u;
        morsel = ifeMorsel->getDstWriteMorsel(common::DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0u;
        }
    }
    auto tableID = sharedState->graph->getNodeTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    srcNodeVector->setValue<nodeID_t>(0,
        common::nodeID_t{ifeMorsel->srcOffsets[currentDstLane], tableID});
    auto pos = 0;
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto exactPathLengthPos = offset * 16 + (15 - currentDstLane);
        uint64_t pathLength = ifeMorsel->pathLength[exactPathLengthPos];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, tableID});
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            pos++;
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos;
}

static uint64_t shortestPathOutputLane32FuncSingle(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (MSBFSIFEMorsel<uint32_t>*)shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane & 0x20) {
        currentDstLane = 0u;
        morsel = ifeMorsel->getDstWriteMorsel(common::DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0u;
        }
    }
    auto tableID = sharedState->graph->getNodeTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    srcNodeVector->setValue<nodeID_t>(0,
        common::nodeID_t{ifeMorsel->srcOffsets[currentDstLane], tableID});
    auto pos = 0;
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto exactPathLengthPos = offset * 32 + (31 - currentDstLane);
        uint64_t pathLength = ifeMorsel->pathLength[exactPathLengthPos];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, tableID});
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            pos++;
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos;
}

static uint64_t shortestPathOutputLane64FuncSingle(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (MSBFSIFEMorsel<uint64_t>*)shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane & 0x40) {
        currentDstLane = 0u;
        morsel = ifeMorsel->getDstWriteMorsel(common::DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0u;
        }
    }
    auto tableID = sharedState->graph->getNodeTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    srcNodeVector->setValue<nodeID_t>(0,
        common::nodeID_t{ifeMorsel->srcOffsets[currentDstLane], tableID});
    auto pos = 0;
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto exactPathLengthPos = offset * 64 + (63 - currentDstLane);
        uint64_t pathLength = ifeMorsel->pathLength[exactPathLengthPos];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, tableID});
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            pos++;
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos;
}

static uint64_t mainFuncLane8(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputLane8FuncSingle(sharedState, localState);
    }
    extendFrontierLane8SingleFunc(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputLane8FuncSingle(sharedState, localState);
}

static uint64_t mainFuncLane16(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputLane16FuncSingle(sharedState, localState);
    }
    extendFrontierLane16SingleFunc(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputLane16FuncSingle(sharedState, localState);
}

static uint64_t mainFuncLane32(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputLane32FuncSingle(sharedState, localState);
    }
    extendFrontierLane32SingleFunc(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputLane32FuncSingle(sharedState, localState);
}

static uint64_t mainFuncLane64(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputLane64FuncSingle(sharedState, localState);
    }
    extendFrontierLane64SingleFunc(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputLane64FuncSingle(sharedState, localState);
}

void _1T1SParallelMSBFSLength::exec() {
    auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
    auto laneWidth = extraData->laneWidth;
    auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
    auto maxConcurrentMorsels = std::max(1LU, concurrentBFS);
    printf("max concurrent bfs setting: %lu, launching maxConcurrentBFS as: %lu\n", concurrentBFS,
        maxConcurrentMorsels);
    auto maxNodeOffset = sharedState->graph->getNumNodes() - 1;
    auto lowerBound = 1u;
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    std::vector<ParallelUtilsJob> jobs;    // stores the next batch of jobs to submit
    std::vector<unsigned int> jobIdxInMap; // stores the scheduledTaskMap idx <-> job mapping
    auto srcOffset = 0LU;
    auto numCompletedMorsels = 0, totalMorsels = 0, countSources = 0;
    std::vector<common::offset_t> nextMSBFSBatch = std::vector<common::offset_t>();
    /*
     * We need to seed `maxConcurrentBFS` no. of tasks into the queue first. And then we reuse
     * the IFEMorsels initialized again and again for further tasks.
     * (1) Prepare at most maxConcurrentBFS no. of IFEMorsels as tasks to push into queue
     * (2) If we reach maxConcurrentBFS before reaching end of total nodes, then break.
     * (3) If we reach total nodes before we hit maxConcurrentBFS, then break.
     */
    if (laneWidth == 8) {
        auto ifeMorselTasks = scheduledTaskMapLane8();
        while (totalMorsels < maxConcurrentMorsels) {
            while (srcOffset <= maxNodeOffset) {
                if (inputMask->isMasked(srcOffset)) {
                    break;
                }
                srcOffset++;
            }
            if (srcOffset > maxNodeOffset) {
                break;
            }
            countSources++;
            nextMSBFSBatch.push_back(srcOffset);
            srcOffset++;
            if (countSources == laneWidth) {
                totalMorsels++;
                countSources = 0;
                auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint8_t>>(extraData->upperBound,
                    lowerBound, maxNodeOffset);
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                    nextMSBFSBatch.end());
                nextMSBFSBatch.clear();
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, mainFuncLane8, 1 /* maxTaskThreads */});
                ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
            }
        }
        if (countSources > 0) {
            countSources = 0;
            srcOffset++;
            totalMorsels++;
            auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint8_t>>(extraData->upperBound,
                lowerBound, maxNodeOffset);
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                nextMSBFSBatch.end());
            nextMSBFSBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                mainFuncLane8, 1 /* maxTaskThreads */});
            ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
        }
        auto scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
        // place the right scheduled task corresponding to its ife morsel
        for (auto i = 0u; i < scheduledTasks.size(); i++) {
            ifeMorselTasks[i].second = scheduledTasks[i];
            // printf("task ID: %lu was submitted for src: %lu\n", scheduledTasks[i]->ID,
            // ifeMorselTasks[i].first->srcOffset);
        }
        jobs.clear();
        jobIdxInMap.clear();
        bool runLoop = true;
        while (runLoop) {
            for (auto i = 0u; i < ifeMorselTasks.size(); i++) {
                auto& schedTask = ifeMorselTasks[i].second;
                if (!schedTask) {
                    continue;
                }
                if (!parallelUtils->taskCompletedNoError(schedTask)) {
                    continue;
                }
                if (parallelUtils->taskHasExceptionOrTimedOut(schedTask, executionContext)) {
                    // Can we exit from here ? Or should we remove all the other remaining tasks ?
                    // TODO: Handling errors is not currently fixed, all tasks should be removed
                    runLoop = false;
                    break;
                }
                auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                    ifeMorselTasks[i].second->task.get());
                delete processorTask->getSink();
                ifeMorselTasks[i].second = nullptr;
                numCompletedMorsels++;
                ifeMorselTasks[i].first->resetNoLock(common::INVALID_OFFSET);
                ifeMorselTasks[i].first->srcOffsets.clear();
                while ((srcOffset <= maxNodeOffset) && (countSources < laneWidth)) {
                    if (inputMask->isMasked(srcOffset)) {
                        ifeMorselTasks[i].first->srcOffsets.push_back(srcOffset);
                        countSources++;
                    }
                    srcOffset++;
                }
                if (countSources > 0) {
                    countSources = 0;
                    totalMorsels++;
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, mainFuncLane8, 1u /* maxTaskThreads */});
                    jobIdxInMap.push_back(i);
                    continue;
                }
                if ((srcOffset > maxNodeOffset) && (totalMorsels == numCompletedMorsels)) {
                    return; // reached termination, all bfs sources launched have finished
                } else if (srcOffset > maxNodeOffset) {
                    continue;
                }
            }
            scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
            // place the right scheduled task corresponding to its ife morsel
            for (auto i = 0u; i < jobIdxInMap.size(); i++) {
                ifeMorselTasks[jobIdxInMap[i]].second = scheduledTasks[i];
            }
            jobs.clear();
            jobIdxInMap.clear();
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        }
    } else if (laneWidth == 16) {
        auto ifeMorselTasks = scheduledTaskMapLane16();
        while (totalMorsels < maxConcurrentMorsels) {
            while (srcOffset <= maxNodeOffset) {
                if (inputMask->isMasked(srcOffset)) {
                    break;
                }
                srcOffset++;
            }
            if (srcOffset > maxNodeOffset) {
                break;
            }
            countSources++;
            nextMSBFSBatch.push_back(srcOffset);
            srcOffset++;
            if (countSources == laneWidth) {
                totalMorsels++;
                countSources = 0;
                auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint16_t>>(extraData->upperBound,
                    lowerBound, maxNodeOffset);
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                    nextMSBFSBatch.end());
                nextMSBFSBatch.clear();
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, mainFuncLane16, 1 /* maxTaskThreads */});
                ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
            }
        }
        if (countSources > 0) {
            countSources = 0;
            srcOffset++;
            totalMorsels++;
            auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint16_t>>(extraData->upperBound,
                lowerBound, maxNodeOffset);
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                nextMSBFSBatch.end());
            nextMSBFSBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                mainFuncLane16, 1 /* maxTaskThreads */});
            ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
        }
        auto scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
        // place the right scheduled task corresponding to its ife morsel
        for (auto i = 0u; i < scheduledTasks.size(); i++) {
            ifeMorselTasks[i].second = scheduledTasks[i];
            // printf("task ID: %lu was submitted for src: %lu\n", scheduledTasks[i]->ID,
            // ifeMorselTasks[i].first->srcOffset);
        }
        jobs.clear();
        jobIdxInMap.clear();
        bool runLoop = true;
        while (runLoop) {
            for (auto i = 0u; i < ifeMorselTasks.size(); i++) {
                auto& schedTask = ifeMorselTasks[i].second;
                if (!schedTask) {
                    continue;
                }
                if (!parallelUtils->taskCompletedNoError(schedTask)) {
                    continue;
                }
                if (parallelUtils->taskHasExceptionOrTimedOut(schedTask, executionContext)) {
                    // Can we exit from here ? Or should we remove all the other remaining tasks ?
                    // TODO: Handling errors is not currently fixed, all tasks should be removed
                    runLoop = false;
                    break;
                }
                auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                    ifeMorselTasks[i].second->task.get());
                delete processorTask->getSink();
                ifeMorselTasks[i].second = nullptr;
                numCompletedMorsels++;
                ifeMorselTasks[i].first->resetNoLock(common::INVALID_OFFSET);
                ifeMorselTasks[i].first->srcOffsets.clear();
                while ((srcOffset <= maxNodeOffset) && (countSources < laneWidth)) {
                    if (inputMask->isMasked(srcOffset)) {
                        ifeMorselTasks[i].first->srcOffsets.push_back(srcOffset);
                        countSources++;
                    }
                    srcOffset++;
                }
                if (countSources > 0) {
                    countSources = 0;
                    totalMorsels++;
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, mainFuncLane16, 1u /* maxTaskThreads */});
                    jobIdxInMap.push_back(i);
                    continue;
                }
                if ((srcOffset > maxNodeOffset) && (totalMorsels == numCompletedMorsels)) {
                    return; // reached termination, all bfs sources launched have finished
                } else if (srcOffset > maxNodeOffset) {
                    continue;
                }
            }
            scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
            // place the right scheduled task corresponding to its ife morsel
            for (auto i = 0u; i < jobIdxInMap.size(); i++) {
                ifeMorselTasks[jobIdxInMap[i]].second = scheduledTasks[i];
            }
            jobs.clear();
            jobIdxInMap.clear();
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        }
    } else if (laneWidth == 32) {
        auto ifeMorselTasks = scheduledTaskMapLane32();
        while (totalMorsels < maxConcurrentMorsels) {
            while (srcOffset <= maxNodeOffset) {
                if (inputMask->isMasked(srcOffset)) {
                    break;
                }
                srcOffset++;
            }
            if (srcOffset > maxNodeOffset) {
                break;
            }
            countSources++;
            nextMSBFSBatch.push_back(srcOffset);
            srcOffset++;
            if (countSources == laneWidth) {
                totalMorsels++;
                countSources = 0;
                auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint32_t>>(extraData->upperBound,
                    lowerBound, maxNodeOffset);
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                    nextMSBFSBatch.end());
                nextMSBFSBatch.clear();
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, mainFuncLane32, 1 /* maxTaskThreads */});
                ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
            }
        }
        if (countSources > 0) {
            countSources = 0;
            srcOffset++;
            totalMorsels++;
            auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint32_t>>(extraData->upperBound,
                lowerBound, maxNodeOffset);
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                nextMSBFSBatch.end());
            nextMSBFSBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                mainFuncLane32, 1 /* maxTaskThreads */});
            ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
        }
        auto scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
        // place the right scheduled task corresponding to its ife morsel
        for (auto i = 0u; i < scheduledTasks.size(); i++) {
            ifeMorselTasks[i].second = scheduledTasks[i];
            // printf("task ID: %lu was submitted for src: %lu\n", scheduledTasks[i]->ID,
            // ifeMorselTasks[i].first->srcOffset);
        }
        jobs.clear();
        jobIdxInMap.clear();
        bool runLoop = true;
        while (runLoop) {
            for (auto i = 0u; i < ifeMorselTasks.size(); i++) {
                auto& schedTask = ifeMorselTasks[i].second;
                if (!schedTask) {
                    continue;
                }
                if (!parallelUtils->taskCompletedNoError(schedTask)) {
                    continue;
                }
                if (parallelUtils->taskHasExceptionOrTimedOut(schedTask, executionContext)) {
                    // Can we exit from here ? Or should we remove all the other remaining tasks ?
                    // TODO: Handling errors is not currently fixed, all tasks should be removed
                    runLoop = false;
                    break;
                }
                auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                    ifeMorselTasks[i].second->task.get());
                delete processorTask->getSink();
                ifeMorselTasks[i].second = nullptr;
                numCompletedMorsels++;
                ifeMorselTasks[i].first->resetNoLock(common::INVALID_OFFSET);
                ifeMorselTasks[i].first->srcOffsets.clear();
                while ((srcOffset <= maxNodeOffset) && (countSources < laneWidth)) {
                    if (inputMask->isMasked(srcOffset)) {
                        ifeMorselTasks[i].first->srcOffsets.push_back(srcOffset);
                        countSources++;
                    }
                    srcOffset++;
                }
                if (countSources > 0) {
                    countSources = 0;
                    totalMorsels++;
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, mainFuncLane32, 1u /* maxTaskThreads */});
                    jobIdxInMap.push_back(i);
                    continue;
                }
                if ((srcOffset > maxNodeOffset) && (totalMorsels == numCompletedMorsels)) {
                    return; // reached termination, all bfs sources launched have finished
                } else if (srcOffset > maxNodeOffset) {
                    continue;
                }
            }
            scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
            // place the right scheduled task corresponding to its ife morsel
            for (auto i = 0u; i < jobIdxInMap.size(); i++) {
                ifeMorselTasks[jobIdxInMap[i]].second = scheduledTasks[i];
            }
            jobs.clear();
            jobIdxInMap.clear();
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        }
    } else {
        auto ifeMorselTasks = scheduledTaskMapLane64();
        while (totalMorsels < maxConcurrentMorsels) {
            while (srcOffset <= maxNodeOffset) {
                if (inputMask->isMasked(srcOffset)) {
                    break;
                }
                srcOffset++;
            }
            if (srcOffset > maxNodeOffset) {
                break;
            }
            countSources++;
            nextMSBFSBatch.push_back(srcOffset);
            srcOffset++;
            if (countSources == laneWidth) {
                totalMorsels++;
                countSources = 0;
                auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint64_t>>(extraData->upperBound,
                    lowerBound, maxNodeOffset);
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                    nextMSBFSBatch.end());
                nextMSBFSBatch.clear();
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, mainFuncLane64, 1 /* maxTaskThreads */});
                ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
            }
        }
        if (countSources > 0) {
            countSources = 0;
            srcOffset++;
            totalMorsels++;
            auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint64_t>>(extraData->upperBound,
                lowerBound, maxNodeOffset);
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                nextMSBFSBatch.end());
            nextMSBFSBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                mainFuncLane64, 1 /* maxTaskThreads */});
            ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
        }
        auto scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
        // place the right scheduled task corresponding to its ife morsel
        for (auto i = 0u; i < scheduledTasks.size(); i++) {
            ifeMorselTasks[i].second = scheduledTasks[i];
            // printf("task ID: %lu was submitted for src: %lu\n", scheduledTasks[i]->ID,
            // ifeMorselTasks[i].first->srcOffset);
        }
        jobs.clear();
        jobIdxInMap.clear();
        bool runLoop = true;
        while (runLoop) {
            for (auto i = 0u; i < ifeMorselTasks.size(); i++) {
                auto& schedTask = ifeMorselTasks[i].second;
                if (!schedTask) {
                    continue;
                }
                if (!parallelUtils->taskCompletedNoError(schedTask)) {
                    continue;
                }
                if (parallelUtils->taskHasExceptionOrTimedOut(schedTask, executionContext)) {
                    // Can we exit from here ? Or should we remove all the other remaining tasks ?
                    // TODO: Handling errors is not currently fixed, all tasks should be removed
                    runLoop = false;
                    break;
                }
                auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                    ifeMorselTasks[i].second->task.get());
                delete processorTask->getSink();
                ifeMorselTasks[i].second = nullptr;
                numCompletedMorsels++;
                ifeMorselTasks[i].first->resetNoLock(common::INVALID_OFFSET);
                ifeMorselTasks[i].first->srcOffsets.clear();
                while ((srcOffset <= maxNodeOffset) && (countSources < laneWidth)) {
                    if (inputMask->isMasked(srcOffset)) {
                        ifeMorselTasks[i].first->srcOffsets.push_back(srcOffset);
                        countSources++;
                    }
                    srcOffset++;
                }
                if (countSources > 0) {
                    countSources = 0;
                    totalMorsels++;
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, mainFuncLane64, 1u /* maxTaskThreads */});
                    jobIdxInMap.push_back(i);
                    continue;
                }
                if ((srcOffset > maxNodeOffset) && (totalMorsels == numCompletedMorsels)) {
                    return; // reached termination, all bfs sources launched have finished
                } else if (srcOffset > maxNodeOffset) {
                    continue;
                }
            }
            scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
            // place the right scheduled task corresponding to its ife morsel
            for (auto i = 0u; i < jobIdxInMap.size(); i++) {
                ifeMorselTasks[jobIdxInMap[i]].second = scheduledTasks[i];
            }
            jobs.clear();
            jobIdxInMap.clear();
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        }
    }
}

} // namespace function
} // namespace kuzu
