#include "function/gds/1T1S_parallel_varlen_path.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/parallel_var_len_commons.h"
#include "function/gds/varlen_path_ife_morsel.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

static void visitNbrs(VarlenPathIFEMorsel* ifeMorsel, ValueVector& dstNodeIDVector,
    ValueVector& relIDVector, std::vector<edgeListSegment*>& localEdgeListSegment,
    common::offset_t parentOffset) {
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
            ifeMorsel->visitedNodes[dstNodeID.offset] = VISITED_DST;
            ifeMorsel->nextFrontier[dstNodeID.offset] = 1u;
        } else if (state == NOT_VISITED || state == VISITED) {
            ifeMorsel->visitedNodes[dstNodeID.offset] = VISITED;
            ifeMorsel->nextFrontier[dstNodeID.offset] = 1u;
        }
        auto topEntry = ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset];
        if (!topEntry || (topEntry->bfsLevel <= ifeMorsel->currentLevel)) {
            auto newEntry =
                new edgeListAndLevel(ifeMorsel->currentLevel + 1, dstNodeID.offset, topEntry);
            ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset] = newEntry;
            newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
        }
        newEdgeListSegment->edgeListBlockPtr[j].edgeOffset = edgeID.offset;
        newEdgeListSegment->edgeListBlockPtr[j].src = parentNodeEdgeListAndLevel;
        auto currTopEdgeList = ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset]->top;
        newEdgeListSegment->edgeListBlockPtr[j].next = currTopEdgeList;
        ifeMorsel->nodeIDEdgeListAndLevel[dstNodeID.offset]->top =
            &newEdgeListSegment->edgeListBlockPtr[j];
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
            ifeMorsel->visitedNodes[nbrOffset] = VISITED_DST;
            ifeMorsel->nextFrontier[nbrOffset] = 1u;
        } else if (state == NOT_VISITED || state == VISITED) {
            ifeMorsel->visitedNodes[nbrOffset] = VISITED;
            ifeMorsel->nextFrontier[nbrOffset] = 1u;
        }
        auto topEntry = ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset];
        if (!topEntry || (topEntry->bfsLevel <= ifeMorsel->currentLevel)) {
            auto newEntry = new edgeListAndLevel(ifeMorsel->currentLevel + 1, nbrOffset, topEntry);
            ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset] = newEntry;
            newEdgeListSegment->edgeListAndLevelBlock.push_back(newEntry);
        }
        newEdgeListSegment->edgeListBlockPtr[j].edgeOffset = edgeID;
        newEdgeListSegment->edgeListBlockPtr[j].src = parentNodeEdgeListAndLevel;
        auto currTopEdgeList = ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset]->top;
        newEdgeListSegment->edgeListBlockPtr[j].next = currTopEdgeList;
        ifeMorsel->nodeIDEdgeListAndLevel[nbrOffset]->top =
            &newEdgeListSegment->edgeListBlockPtr[j];
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

static uint64_t extendFrontierFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto varlenLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelVarLenLocalState*>(localState);
    auto ifeMorsel = (VarlenPathIFEMorsel*)varlenLocalState->ifeMorsel;
    auto& nbrScanState = varlenLocalState->nbrScanState;
    while (!ifeMorsel->isBFSCompleteNoLock()) {
        for (auto offset = 0u; offset < ifeMorsel->maxOffset + 1; offset++) {
            if (!ifeMorsel->currentFrontier[offset]) {
                continue;
            }
            extendNode(graph.get(), ifeMorsel, offset, varlenLocalState->localEdgeListSegment,
                nbrScanState.get());
        }
        ifeMorsel->mergeResults(varlenLocalState->localEdgeListSegment);
        ifeMorsel->initializeNextFrontierNoLock();
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
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2 * i,
                    {varlenLocalState->edgeBuffer[i]->edgeOffset, edgeTableID});
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2 * i + 1,
                    {varlenLocalState->nodeBuffer[i + 1]->nodeOffset, nodeTableID});
            }
            pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2 * i,
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
                        pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2 * i,
                            {varlenLocalState->edgeBuffer[i]->edgeOffset, edgeTableID});
                        pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2 * i + 1,
                            {varlenLocalState->nodeBuffer[i + 1]->nodeOffset, nodeTableID});
                    }
                    pathDataVector->setValue<common::internalID_t>(listEntry.offset + 2 * i,
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

static uint64_t mainFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto varlenPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelVarLenLocalState*>(localState);
    varlenPathLocalState->ifeMorsel->init();
    if (varlenPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputFunc(sharedState, localState);
    }
    extendFrontierFunc(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputFunc(sharedState, localState);
}

void _1T1SParallelVarlenPath::exec() {
    auto extraData = bindData->ptrCast<ParallelVarlenBindData>();
    // threads available will be 1 less than total (main thread makes gds call)
    auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
    // set max bfs always to min value between threads available and maxConcurrentBFS value
    auto maxConcurrentBFS = std::max(1LU, concurrentBFS);
    printf("max concurrent bfs setting: %lu, launching maxConcurrentBFS as: %lu\n", concurrentBFS,
        maxConcurrentBFS);
    auto maxNodeOffset = sharedState->graph->getNumNodes() - 1;
    auto lowerBound = 1u;
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    scheduledTaskMap ifeMorselTasks = scheduledTaskMap();
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
        /*auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        printf("starting bfs source: %lu at %lu\n", srcOffset, millis);*/
        auto ifeMorsel = std::make_unique<VarlenPathIFEMorsel>(extraData->upperBound, lowerBound,
            maxNodeOffset, srcOffset);
        srcOffset++;
        auto gdsLocalState = std::make_unique<ParallelVarLenLocalState>(true);
        gdsLocalState->ifeMorsel = ifeMorsel.get();
        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
            mainFunc, 1u /* maxTaskThreads */};
        auto scheduledTask = parallelUtils->submitTaskAndReturn(job);
        // printf("submitted job for bfs source: %lu\n", ifeMorsel->srcOffset);
        ifeMorselTasks.emplace_back(std::move(ifeMorsel), scheduledTask);
    }
    if (ifeMorselTasks.empty()) {
        return;
    }
    bool runLoop = true;
    while (runLoop) {
        for (auto & ifeMorselTask : ifeMorselTasks) {
            auto& schedTask = ifeMorselTask.second;
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
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis =
                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            printf("bfs source: %lu finished at time: %lu\n",
                ifeMorselTasks[i].first->srcOffset, millis);*/
            auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                ifeMorselTask.second->task.get());
            free(processorTask->getSink());
            ifeMorselTask.second = nullptr;
            // printf("bfs source: %lu is completed\n", ifeMorselTasks[i].first->srcOffset);
            numCompletedTasks++;
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
            /*duration = std::chrono::system_clock::now().time_since_epoch();
            millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            printf("bfs source: %lu starting at time: %lu\n", srcOffset, millis);*/
            totalBFSSources++;
            ifeMorselTask.first->resetNoLock(srcOffset);
            srcOffset++;
            auto gdsLocalState = std::make_unique<ParallelVarLenLocalState>(true);
            gdsLocalState->ifeMorsel = ifeMorselTask.first.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                mainFunc, 1u /* maxTaskThreads */};
            ifeMorselTask.second = parallelUtils->submitTaskAndReturn(job);
        }
        std::this_thread::sleep_for(
            std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
    }
}

} // namespace function
} // namespace kuzu
