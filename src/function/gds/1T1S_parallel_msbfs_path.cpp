#include "function/gds/1T1S_parallel_msbfs_path.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/msbfs_path_ife_morsel.h"
#include "function/gds/parallel_msbfs_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

typedef std::vector<
    std::pair<std::unique_ptr<MSBFSPathIFEMorsel<uint8_t>>, std::shared_ptr<ScheduledTask>>>
    taskMapLane8;

typedef std::vector<
    std::pair<std::unique_ptr<MSBFSPathIFEMorsel<uint16_t>>, std::shared_ptr<ScheduledTask>>>
    taskMapLane16;

typedef std::vector<
    std::pair<std::unique_ptr<MSBFSPathIFEMorsel<uint32_t>>, std::shared_ptr<ScheduledTask>>>
    taskMapLane32;

typedef std::vector<
    std::pair<std::unique_ptr<MSBFSPathIFEMorsel<uint64_t>>, std::shared_ptr<ScheduledTask>>>
    taskMapLane64;

static uint64_t extendFrontierLane8Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSPathIFEMorsel<uint8_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset < (msbfsIFEMorsel->maxOffset + 1); offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint8_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            msbfsIFEMorsel->next[nbrOffset] =
                                msbfsIFEMorsel->next[nbrOffset] | shouldBeActive;
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exactPos = nbrOffset * 8 + (8 - index);
                                msbfsIFEMorsel->pathLength[exactPos] =
                                    msbfsIFEMorsel->currentLevel + 1;
                                msbfsIFEMorsel->parentOffset[exactPos] = offset;
                                msbfsIFEMorsel->edgeOffset[exactPos] = csrEntry->relOffsets[nbrIdx];
                                shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index - 1));
                            }
                        }
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset < (msbfsIFEMorsel->maxOffset + 1); offset++) {
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
                                msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                edgeID = relIDs[j];
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exactPos = dstNodeID.offset * 8 + (8 - index);
                                    msbfsIFEMorsel->pathLength[exactPos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    msbfsIFEMorsel->parentOffset[exactPos] = offset;
                                    msbfsIFEMorsel->edgeOffset[exactPos] = edgeID.offset;
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

static uint64_t extendFrontierLane16Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSPathIFEMorsel<uint16_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset < (msbfsIFEMorsel->maxOffset + 1); offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint16_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            msbfsIFEMorsel->next[nbrOffset] =
                                msbfsIFEMorsel->next[nbrOffset] | shouldBeActive;
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exactPos = nbrOffset * 16 + (16 - index);
                                msbfsIFEMorsel->pathLength[exactPos] =
                                    msbfsIFEMorsel->currentLevel + 1;
                                msbfsIFEMorsel->parentOffset[exactPos] = offset;
                                msbfsIFEMorsel->edgeOffset[exactPos] = csrEntry->relOffsets[nbrIdx];
                                shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index - 1));
                            }
                        }
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        }
    } else {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset < (msbfsIFEMorsel->maxOffset + 1); offset++) {
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
                            uint16_t shouldBeActive = msbfsIFEMorsel->current[offset] &
                                                      ~msbfsIFEMorsel->seen[dstNodeID.offset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[dstNodeID.offset] =
                                    msbfsIFEMorsel->next[dstNodeID.offset] | shouldBeActive;
                                relID = relIDs[j];
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exactPos = dstNodeID.offset * 16 + (16 - index);
                                    msbfsIFEMorsel->pathLength[exactPos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    msbfsIFEMorsel->parentOffset[exactPos] = offset;
                                    msbfsIFEMorsel->edgeOffset[exactPos] = relID.offset;
                                    shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index - 1));
                                }
                            }
                        }
                    } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
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
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
        for (auto offset = 0u; offset < (msbfsIFEMorsel->maxOffset + 1); offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint32_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            msbfsIFEMorsel->next[nbrOffset] = msbfsIFEMorsel->next[nbrOffset] | shouldBeActive;
                            while (shouldBeActive) {
                                int index = __builtin_ctz(shouldBeActive) + 1;
                                auto exactPos = nbrOffset * 32 + (32 - index);
                                msbfsIFEMorsel->pathLength[exactPos] = msbfsIFEMorsel->currentLevel + 1;
                                msbfsIFEMorsel->parentOffset[exactPos] = offset;
                                msbfsIFEMorsel->edgeOffset[exactPos] = csrEntry->relOffsets[nbrIdx];
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
            for (auto offset = 0u; offset < (msbfsIFEMorsel->maxOffset + 1); offset++) {
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
                                msbfsIFEMorsel->next[dstNodeID.offset] = msbfsIFEMorsel->next[dstNodeID.offset] | shouldBeActive;
                                relID = relIDs[j];
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exactPos = dstNodeID.offset * 32 + (32 - index);
                                    msbfsIFEMorsel->pathLength[exactPos] = msbfsIFEMorsel->currentLevel + 1;
                                    msbfsIFEMorsel->parentOffset[exactPos] = offset;
                                    msbfsIFEMorsel->edgeOffset[exactPos] = relID.offset;
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

static uint64_t extendFrontierLane64Func(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel*, MSBFSPathIFEMorsel<uint64_t>*>(
        shortestPathLocalState->ifeMorsel);
    msbfsIFEMorsel->init();
    if (graph->isInMemory) {
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
        auto& csr = inMemGraph->getInMemCSR();
        while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
            for (auto offset = 0u; offset < (msbfsIFEMorsel->maxOffset + 1); offset++) {
                if (msbfsIFEMorsel->current[offset]) {
                    auto csrEntry = csr[offset >> RIGHT_SHIFT];
                    auto posInCSR = offset & OFFSET_DIV;
                    for (auto nbrIdx = csrEntry->csr_v[posInCSR];
                        nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                        uint64_t shouldBeActive =
                            msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                        if (shouldBeActive) {
                            msbfsIFEMorsel->next[nbrOffset] = msbfsIFEMorsel->next[nbrOffset] | shouldBeActive;
                            while (shouldBeActive) {
                                int index = __builtin_ctzll(shouldBeActive) + 1;
                                auto exactPos = nbrOffset * 64 + (64 - index);
                                msbfsIFEMorsel->pathLength[exactPos] = msbfsIFEMorsel->currentLevel + 1;
                                msbfsIFEMorsel->parentOffset[exactPos] = offset;
                                msbfsIFEMorsel->edgeOffset[exactPos] = csrEntry->relOffsets[nbrIdx];
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
            for (auto offset = 0u; offset < msbfsIFEMorsel->maxOffset + 1; offset++) {
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
                                msbfsIFEMorsel->next[dstNodeID.offset] = msbfsIFEMorsel->next[dstNodeID.offset] | shouldBeActive;
                                relID = relIDs[j];
                                while (shouldBeActive) {
                                    int index = __builtin_ctzll(shouldBeActive) + 1;
                                    auto exactPos = dstNodeID.offset * 64 + (64 - index);
                                    msbfsIFEMorsel->pathLength[exactPos] = msbfsIFEMorsel->currentLevel + 1;
                                    msbfsIFEMorsel->parentOffset[exactPos] = offset;
                                    msbfsIFEMorsel->edgeOffset[exactPos] = relID.offset;
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


static uint64_t mainFuncLane8(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputLane8Func(sharedState, localState);
    }
    extendFrontierLane8Func(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputLane8Func(sharedState, localState);
}

static uint64_t mainFuncLane16(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputLane16Func(sharedState, localState);
    }
    extendFrontierLane16Func(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputLane16Func(sharedState, localState);
}

static uint64_t mainFuncLane32(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputLane32Func(sharedState, localState);
    }
    extendFrontierLane32Func(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputLane32Func(sharedState, localState);
}

static uint64_t mainFuncLane64(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputLane64Func(sharedState, localState);
    }
    extendFrontierLane64Func(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputLane64Func(sharedState, localState);
}

void _1T1SParallelMSBFSPath::exec() {
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
        auto ifeMorselTasks = taskMapLane8();
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
                auto ifeMorsel = std::make_unique<MSBFSPathIFEMorsel<uint8_t>>(extraData->upperBound,
                    lowerBound, maxNodeOffset);
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                    nextMSBFSBatch.end());
                nextMSBFSBatch.clear();
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
            auto ifeMorsel = std::make_unique<MSBFSPathIFEMorsel<uint8_t>>(extraData->upperBound,
                lowerBound, maxNodeOffset);
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                nextMSBFSBatch.end());
            nextMSBFSBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
                free(processorTask->getSink());
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
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
        auto ifeMorselTasks = taskMapLane16();
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
                auto ifeMorsel = std::make_unique<MSBFSPathIFEMorsel<uint16_t>>(extraData->upperBound,
                    lowerBound, maxNodeOffset);
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                    nextMSBFSBatch.end());
                nextMSBFSBatch.clear();
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
            auto ifeMorsel = std::make_unique<MSBFSPathIFEMorsel<uint16_t>>(extraData->upperBound,
                lowerBound, maxNodeOffset);
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                nextMSBFSBatch.end());
            nextMSBFSBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
                free(processorTask->getSink());
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
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
        auto ifeMorselTasks = taskMapLane32();
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
                auto ifeMorsel = std::make_unique<MSBFSPathIFEMorsel<uint32_t>>(extraData->upperBound,
                    lowerBound, maxNodeOffset);
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                    nextMSBFSBatch.end());
                nextMSBFSBatch.clear();
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
            auto ifeMorsel = std::make_unique<MSBFSPathIFEMorsel<uint32_t>>(extraData->upperBound,
                lowerBound, maxNodeOffset);
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                nextMSBFSBatch.end());
            nextMSBFSBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
                free(processorTask->getSink());
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
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
        auto ifeMorselTasks = taskMapLane64();
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
                auto ifeMorsel = std::make_unique<MSBFSPathIFEMorsel<uint64_t>>(extraData->upperBound,
                    lowerBound, maxNodeOffset);
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                    nextMSBFSBatch.end());
                nextMSBFSBatch.clear();
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
            auto ifeMorsel = std::make_unique<MSBFSPathIFEMorsel<uint64_t>>(extraData->upperBound,
                lowerBound, maxNodeOffset);
            // just pass a placeholder offset, it is not used for ms bfs ife morsel
            ifeMorsel->resetNoLock(common::INVALID_OFFSET);
            ifeMorsel->srcOffsets.insert(ifeMorsel->srcOffsets.end(), nextMSBFSBatch.begin(),
                nextMSBFSBatch.end());
            nextMSBFSBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
                free(processorTask->getSink());
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
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>(true);
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
