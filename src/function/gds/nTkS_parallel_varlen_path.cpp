#include "function/gds/nTkS_parallel_varlen_path.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/parallel_var_len_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/varlen_path_ife_morsel.h"
#include "function/gds_function.h"
#include "processor/processor_task.h"
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
        auto state = ifeMorsel->visitedNodes[dstNodeID.offset];
        if (state == NOT_VISITED_DST || state == VISITED_DST) {
            __atomic_store_n(&ifeMorsel->visitedNodes[dstNodeID.offset], VISITED_DST,
                __ATOMIC_RELAXED);
            __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u, __ATOMIC_RELAXED);
        } else if (state == NOT_VISITED || state == VISITED) {
            __atomic_store_n(&ifeMorsel->visitedNodes[dstNodeID.offset], VISITED, __ATOMIC_RELAXED);
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
                // dequeue objects from the
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
                // dequeue objects from the
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
    if (!ifeMorsel->initializedIFEMorsel) {
        ifeMorsel->init();
    }
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

void nTkSParallelVarlenPath::exec() {
    auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
    auto extraData = bindData->ptrCast<ParallelVarlenBindData>();
    auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
    auto maxConcurrentBFS = std::max(1LU, concurrentBFS);
    printf("max concurrent bfs setting: %lu, launching maxConcurrentBFS as: %lu\n",
        concurrentBFS, maxConcurrentBFS);
    auto maxNodeOffset = sharedState->graph->getNumNodes() - 1;
    auto lowerBound = 1u;
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    scheduledTaskMap ifeMorselTasks = scheduledTaskMap();
    std::vector<ParallelUtilsJob> jobs; // stores the next batch of jobs to submit
    std::vector<unsigned int> jobIdxInMap;       // stores the scheduledTaskMap idx <-> job mapping
    auto srcOffset = 0LU, numCompletedTasks = 0LU, totalBFSSources = 0LU;
    /*
     * We need to seed `maxConcurrentBFS` no. of tasks into the queue first. And then we reuse
     * the IFEMorsels initialized again and again for further tasks.
     * (1) Prepare at most maxConcurrentBFS no. of IFEMorsels as tasks to push into queue
     * (2) If we reach maxConcurrentBFS before reaching end of total nodes, then break.
     * (3) If we reach total nodes before we hit maxConcurrentBFS, then break.
     */
    while (totalBFSSources < maxConcurrentBFS) {
        while (srcOffset <= maxNodeOffset) {
            if (inputMask->isMasked(srcOffset)) {
                break;
            }
            srcOffset++;
        }
        if (srcOffset > maxNodeOffset) {
            break;
        }
        totalBFSSources++;
        // printf("starting bfs source: %lu\n", srcOffset);
        auto ifeMorsel = std::make_unique<VarlenPathIFEMorsel>(extraData->upperBound, lowerBound,
            maxNodeOffset, srcOffset);
        srcOffset++;
        auto gdsLocalState = std::make_unique<ParallelVarLenLocalState>(true);
        gdsLocalState->ifeMorsel = ifeMorsel.get();
        jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
            extendFrontierFunc, 1 /* maxTaskThreads */});
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
            if (ifeMorselTasks[i].first->isIFEMorselCompleteNoLock()) {
                auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                    ifeMorselTasks[i].second->task.get());
                delete processorTask->getSink();
                ifeMorselTasks[i].second = nullptr;
                numCompletedTasks++;
                // printf("bfs source: %lu is completed\n", ifeMorselTasks[i].first->srcOffset);
                while (srcOffset <= maxNodeOffset) {
                    if (inputMask->isMasked(srcOffset)) {
                        break;
                    }
                    srcOffset++;
                }
                if ((srcOffset > maxNodeOffset) && (totalBFSSources == numCompletedTasks)) {
                    return; // reached termination, all bfs sources launched have finished
                } else if (srcOffset > maxNodeOffset) {
                    continue;
                }
                totalBFSSources++;
                ifeMorselTasks[i].first->resetNoLock(srcOffset);
                srcOffset++;
                auto gdsLocalState = std::make_unique<ParallelVarLenLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, extendFrontierFunc, 1u /* maxTaskThreads */});
                jobIdxInMap.push_back(i);
                continue;
            }
            bool isBFSComplete = ifeMorselTasks[i].first->isBFSCompleteNoLock();
            if (isBFSComplete) {
                auto gdsLocalState = std::make_unique<ParallelVarLenLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, shortestPathOutputFunc, maxThreads});
                jobIdxInMap.push_back(i);
                continue;
            } else {
                ifeMorselTasks[i].first->initializeNextFrontierNoLock();
                auto gdsLocalState = std::make_unique<ParallelVarLenLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, extendFrontierFunc, maxThreads});
                jobIdxInMap.push_back(i);
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

} // namespace function
} // namespace kuzu
