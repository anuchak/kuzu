#include "function/gds/nT1S_parallel_varlen_path.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/parallel_var_len_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/varlen_path_ife_morsel.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

static void visitNbrs(VarlenPathIFEMorsel* ifeMorsel,
    ValueVector& dstNodeIDVector, ValueVector& relIDVector,
    std::vector<edgeListSegment*>& localEdgeListSegment, common::offset_t parentOffset) {
    auto size = dstNodeIDVector.state->getSelVector().getSelSize();
    auto nbrNodes = (common::nodeID_t*)dstNodeIDVector.getData();
    auto relIDs = (common::relID_t*)relIDVector.getData();
    auto newEdgeListSegment = new edgeListSegment(size);
    localEdgeListSegment.push_back(newEdgeListSegment);
    auto parentNodeEdgeListAndLevel = ifeMorsel->nodeIDEdgeListAndLevel[parentOffset];
    while (parentNodeEdgeListAndLevel->bfsLevel != ifeMorsel->currentLevel) {
        parentNodeEdgeListAndLevel = parentNodeEdgeListAndLevel->next;
    }
    for (auto j = 0u; j < size; j++) {
        auto dstNodeID = nbrNodes[j];
        auto edgeID = relIDs[j];
        auto isNextFrontier = ifeMorsel->nextFrontier[dstNodeID.offset];
        auto state = ifeMorsel->visitedNodes[dstNodeID.offset];
        if (!isNextFrontier && state == NOT_VISITED_DST)  {
            __atomic_store_n(&ifeMorsel->visitedNodes[dstNodeID.offset], VISITED_DST,
                __ATOMIC_RELAXED);
            __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u, __ATOMIC_RELAXED);
        } else if (!isNextFrontier && state == NOT_VISITED) {
            __atomic_store_n(&ifeMorsel->visitedNodes[dstNodeID.offset], VISITED, __ATOMIC_RELAXED);
            __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u, __ATOMIC_RELAXED);
        } else if (!isNextFrontier) {
            __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u, __ATOMIC_RELAXED);
        }
        auto topEntry = ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset];
        if (!topEntry || (topEntry->bfsLevel <= ifeMorsel->currentLevel)) {
            auto newEntry =
                new edgeListAndLevel(ifeMorsel->currentLevel + 1, dstNodeID.offset, topEntry);
            if (__sync_bool_compare_and_swap(&ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset],
                    topEntry, newEntry)) {
                // This thread was successful in doing the CAS operation at the top.
                newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
            } else {
                // This thread was NOT successful in doing the CAS operation, hence free the memory
                // right here since it has no use.
                // TODO: There is an optimization here, of reusing memory by not deleting this
                // edgeListAndLevel object, keeping it in a (doubly-ended) local queue. This way we
                // reduce the no. of memory allocations, we queue failed CAS objects at the end and
                // dequeue objects from the front
                delete newEntry;
            }
        }
        newEdgeListSegment->edgeListBlockPtr[j].edgeOffset = edgeID.offset;
        newEdgeListSegment->edgeListBlockPtr[j].src = parentNodeEdgeListAndLevel;
        auto currTopEdgeList = ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset]->top;
        newEdgeListSegment->edgeListBlockPtr[j].next = currTopEdgeList;
        while (
            !__sync_bool_compare_and_swap(&ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset]->top,
                currTopEdgeList, &newEdgeListSegment->edgeListBlockPtr[j])) {
            // Failed to do the CAS operation, read the top edgeList of the dstNode again
            currTopEdgeList = ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset]->top;
            newEdgeListSegment->edgeListBlockPtr[j].next = currTopEdgeList;
        }
    }
}

static void visitNbrs(VarlenPathIFEMorsel* ifeMorsel, graph::Graph* graph,
    std::vector<edgeListSegment*>& localEdgeListSegment, common::offset_t parentOffset) {
    auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph);
    auto& csr = inMemGraph->getInMemCSR();
    auto csrEntry = csr[parentOffset >> RIGHT_SHIFT];
    if (!csrEntry) {
        return;
    }
    auto posInCSR = parentOffset & OFFSET_DIV;
    auto size = csrEntry->csr_v[posInCSR + 1] - csrEntry->csr_v[posInCSR] + 1;
    auto newEdgeListSegment = new edgeListSegment(size);
    localEdgeListSegment.push_back(newEdgeListSegment);
    auto parentNodeEdgeListAndLevel = ifeMorsel->nodeIDEdgeListAndLevel[parentOffset];
    while (parentNodeEdgeListAndLevel->bfsLevel != ifeMorsel->currentLevel) {
        parentNodeEdgeListAndLevel = parentNodeEdgeListAndLevel->next;
    }
    for (auto j = csrEntry->csr_v[posInCSR]; j < csrEntry->csr_v[posInCSR + 1]; j++) {
        auto nbrOffset = csrEntry->nbrNodeOffsets[j];
        auto edgeID = csrEntry->relOffsets[j];
        auto state = ifeMorsel->visitedNodes[nbrOffset];
        if (state == NOT_VISITED_DST || state == VISITED_DST) {
            __atomic_store_n(&ifeMorsel->visitedNodes[nbrOffset], VISITED_DST, __ATOMIC_RELAXED);
            __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELAXED);
        } else if (state == NOT_VISITED || state == VISITED) {
            __atomic_store_n(&ifeMorsel->visitedNodes[nbrOffset], VISITED, __ATOMIC_RELAXED);
            __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELAXED);
        }
        auto topEntry = ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset];
        if (!topEntry || (topEntry->bfsLevel <= ifeMorsel->currentLevel)) {
            auto newEntry = new edgeListAndLevel(ifeMorsel->currentLevel + 1, nbrOffset, topEntry);
            if (__sync_bool_compare_and_swap(&ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset],
                    topEntry, newEntry)) {
                // This thread was successful in doing the CAS operation at the top.
                newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
            } else {
                // This thread was NOT successful in doing the CAS operation, hence free the memory
                // right here since it has no use.
                // TODO: There is an optimization here, of reusing memory by not deleting this
                // edgeListAndLevel object, keeping it in a (doubly-ended) local queue. This way we
                // reduce the no. of memory allocations, we queue failed CAS objects at the end and
                // dequeue objects from the front
                delete newEntry;
            }
        }
        newEdgeListSegment->edgeListBlockPtr[j].edgeOffset = edgeID;
        newEdgeListSegment->edgeListBlockPtr[j].src = parentNodeEdgeListAndLevel;
        auto currTopEdgeList = ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset]->top;
        newEdgeListSegment->edgeListBlockPtr[j].next = currTopEdgeList;
        while (!__sync_bool_compare_and_swap(&ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset]->top,
            currTopEdgeList, &newEdgeListSegment->edgeListBlockPtr[j])) {
            // Failed to do the CAS operation, read the top edgeList of the dstNode again
            currTopEdgeList = ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset]->top;
            newEdgeListSegment->edgeListBlockPtr[j].next = currTopEdgeList;
        }
    }
}

static void extendNode(graph::Graph* graph, VarlenPathIFEMorsel* ifeMorsel,
    const common::offset_t offset, std::vector<edgeListSegment*>& localEdgeListSegment,
    graph::NbrScanState* nbrScanState) {
    if (graph->isInMemory) {
        visitNbrs(ifeMorsel, graph, localEdgeListSegment, offset);
    } else {
        graph->initializeStateFwdNbrs(offset, nbrScanState);
        do {
            graph->getFwdNbrs(nbrScanState);
            visitNbrs(ifeMorsel, *nbrScanState->dstNodeIDVector, *nbrScanState->relIDVector,
                localEdgeListSegment, offset);
        } while (graph->hasMoreFwdNbrs(nbrScanState));
    }
}

static uint64_t extendFrontierFunc(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto varlenLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelVarLenLocalState*>(localState);
    auto ifeMorsel = (VarlenPathIFEMorsel*)varlenLocalState->ifeMorsel;
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = ifeMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0; // return 0 to indicate to thread it can exit from operator
    }
    auto& nbrScanState = varlenLocalState->nbrScanState;
    while (!ifeMorsel->isBFSCompleteNoLock() && frontierMorsel.hasMoreToOutput()) {
        for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
            offset++) {
            if (!ifeMorsel->currentFrontier[offset]) {
                continue;
            }
            extendNode(graph.get(), ifeMorsel, offset,
                varlenLocalState->localEdgeListSegment,nbrScanState.get());
        }
        if (!varlenLocalState->localEdgeListSegment.empty()) {
            ifeMorsel->mergeResults(varlenLocalState->localEdgeListSegment);
        }
        frontierMorsel = ifeMorsel->getMorsel(morselSize);
    }
    return 0u;
}

static uint64_t shortestPathOutputFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto varlenLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelVarLenLocalState*>(localState);
    auto ifeMorsel = (VarlenPathIFEMorsel*)varlenLocalState->ifeMorsel;
    auto nodeTableID = sharedState->graph->getNodeTableID();
    auto edgeTableID = sharedState->graph->getRelTableID();
    auto& srcNodeVector = varlenLocalState->srcNodeIDVector;
    auto& dstOffsetVector = varlenLocalState->dstNodeIDVector;
    auto& pathLengthVector = varlenLocalState->lengthVector;
    auto& pathVector = varlenLocalState->pathVector;
    auto pathDataVector = ListVector::getDataVector(pathVector.get());
    if (pathVector) {
        pathVector->resetAuxiliaryBuffer();
    }
    auto& startIdx = varlenLocalState->prevDstMorselRange.first;
    auto& endIdx = varlenLocalState->prevDstMorselRange.second;
    auto pos = 0u;
    srcNodeVector->setValue<nodeID_t>(0, {ifeMorsel->srcOffset, nodeTableID});
    if (varlenLocalState->hasMorePathToWrite) {
        bool exitLoop;
        auto edgeListAndLevel = ifeMorsel->nodeIDEdgeListAndLevel[startIdx];
        auto pathLength = edgeListAndLevel->bfsLevel;
        do {
            exitLoop = true;
            dstOffsetVector->setValue<nodeID_t>(pos, {startIdx, nodeTableID});
            pathLengthVector->setValue<int64_t>(pos, pathLength);
            // Max length of intermediate path can be (2 * upper_bound - 1)
            // There can be (upper_bound) no. of edges and (upper_bound - 1) no. of nodes
            auto listEntry = ListVector::addList(pathVector.get(), 2 * pathLength - 1);
            pathVector->setValue<list_entry_t>(pos, listEntry);
            auto i = 0u;
            for (; i < pathLength - 1; i++) {
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2*i,
                    {varlenLocalState->edgeBuffer[i]->edgeOffset, edgeTableID});
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2*i + 1,
                    {varlenLocalState->nodeBuffer[i+1]->nodeOffset, nodeTableID});
            }
            pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2*i,
                {varlenLocalState->edgeBuffer[i]->edgeOffset, edgeTableID});
            for (i = 0u; i < pathLength; i++) {
                if (varlenLocalState->edgeBuffer[i]->next) {
                    auto j = i;
                    auto nextEdge = varlenLocalState->edgeBuffer[j]->next;
                    while (nextEdge) {
                        varlenLocalState->edgeBuffer[j] = nextEdge;
                        varlenLocalState->nodeBuffer[j] = nextEdge->src;
                        nextEdge = varlenLocalState->nodeBuffer[j]->top;
                        j--;
                    }
                    exitLoop = false;
                    break;
                }
            }
            pos++;
        } while (pos < common::DEFAULT_VECTOR_CAPACITY && !exitLoop);
        if (pos == common::DEFAULT_VECTOR_CAPACITY && !exitLoop) {
            varlenLocalState->hasMorePathToWrite = true;
        } else {
            varlenLocalState->hasMorePathToWrite = false;
            ifeMorsel->nodeIDEdgeListAndLevel[startIdx] =
                ifeMorsel->nodeIDEdgeListAndLevel[startIdx]->next;
        }
    } else {
        if (startIdx == endIdx) {
            auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
            if (!morsel.hasMoreToOutput()) {
                return 0u;
            }
            varlenLocalState->prevDstMorselRange.first = morsel.startOffset;
            varlenLocalState->prevDstMorselRange.second = morsel.endOffset;
        }
        while (startIdx < endIdx && pos < common::DEFAULT_VECTOR_CAPACITY) {
            if (ifeMorsel->visitedNodes[startIdx] == VISITED_DST &&
                ifeMorsel->nodeIDEdgeListAndLevel[startIdx] &&
                ifeMorsel->nodeIDEdgeListAndLevel[startIdx]->bfsLevel >= ifeMorsel->lowerBound) {
                auto edgeListAndLevel = ifeMorsel->nodeIDEdgeListAndLevel[startIdx];
                auto pathLength = edgeListAndLevel->bfsLevel;
                auto idx = 0u;
                varlenLocalState->nodeBuffer[pathLength] = edgeListAndLevel;
                auto temp = edgeListAndLevel;
                while (pathLength > idx) {
                    varlenLocalState->edgeBuffer[pathLength - idx - 1] = temp->top;
                    temp = temp->top->src;
                    varlenLocalState->nodeBuffer[pathLength - ++idx] = temp;
                }
                bool exitLoop;
                do {
                    exitLoop = true;
                    dstOffsetVector->setValue<nodeID_t>(pos, {startIdx, nodeTableID});
                    pathLengthVector->setValue<int64_t>(pos, pathLength);
                    auto listEntry = ListVector::addList(pathVector.get(), 2 * pathLength - 1);
                    pathVector->setValue<list_entry_t>(pos, listEntry);
                    auto i = 0u;
                    for (; i < pathLength - 1; i++) {
                        pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2*i,
                            {varlenLocalState->edgeBuffer[i]->edgeOffset, edgeTableID});
                        pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2*i + 1,
                            {varlenLocalState->nodeBuffer[i+1]->nodeOffset, nodeTableID});
                    }
                    pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2*i,
                        {varlenLocalState->edgeBuffer[i]->edgeOffset, edgeTableID});
                    for (i = 0u; i < pathLength; i++) {
                        if (varlenLocalState->edgeBuffer[i]->next) {
                            auto j = i;
                            auto nextEdge = varlenLocalState->edgeBuffer[j]->next;
                            while (nextEdge) {
                                varlenLocalState->edgeBuffer[j] = nextEdge;
                                varlenLocalState->nodeBuffer[j] = nextEdge->src;
                                nextEdge = varlenLocalState->nodeBuffer[j]->top;
                                j--;
                            }
                            exitLoop = false;
                            break;
                        }
                    }
                    pos++;
                } while (pos < common::DEFAULT_VECTOR_CAPACITY && !exitLoop);
                if (pos == common::DEFAULT_VECTOR_CAPACITY && !exitLoop) {
                    varlenLocalState->hasMorePathToWrite = true;
                    break;
                } else if (pos == common::DEFAULT_VECTOR_CAPACITY) {
                    ifeMorsel->nodeIDEdgeListAndLevel[startIdx] =
                        ifeMorsel->nodeIDEdgeListAndLevel[startIdx]->next;
                    break;
                } else {
                    ifeMorsel->nodeIDEdgeListAndLevel[startIdx] =
                        ifeMorsel->nodeIDEdgeListAndLevel[startIdx]->next;
                    continue;
                }
            }
            startIdx++;
        }
    }
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos; // return the no. of output values written to the value vectors
}

void nT1SParallelVarlenPath::exec() {
    auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
    auto extraData = bindData->ptrCast<ParallelVarlenBindData>();
    auto numNodes = sharedState->graph->getNumNodes();
    auto ifeMorsel = std::make_unique<VarlenPathIFEMorsel>(extraData->upperBound, 1, numNodes - 1,
        common::INVALID_OFFSET);
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    for (auto offset = 0u; offset < numNodes; offset++) {
        if (!inputMask->isMasked(offset)) {
            continue;
        }
        ifeMorsel->resetNoLock(offset);
        ifeMorsel->init();
        while (!ifeMorsel->isBFSCompleteNoLock()) {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis =
                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            // printf("starting bfs level: %d\n", ifeMorsel->currentLevel);
            auto gdsLocalState =
                std::make_unique<ParallelVarLenLocalState>(true /* is returning path */);
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                extendFrontierFunc, maxThreads};
            parallelUtils->submitParallelTaskAndWait(job);
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 =
                std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                millis1 - millis);*/
            ifeMorsel->initializeNextFrontierNoLock();
        }
        /*auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
        auto gdsLocalState =
            std::make_unique<ParallelVarLenLocalState>(true /* is returning path */);
        gdsLocalState->ifeMorsel = ifeMorsel.get();
        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
            shortestPathOutputFunc, maxThreads};
        parallelUtils->submitParallelTaskAndWait(job);
        /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
        printf("output writing completed in %lu ms\n", millis1 - millis);*/
    }
}

} // namespace function
} // namespace kuzu
