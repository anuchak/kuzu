#include "function/gds/1T1S_parallel_sp_path.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/sp_path_ife_morsel.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::pair<uint64_t, uint64_t> visitNbrs(SPPathIFEMorsel* ifeMorsel,
    ValueVector& dstNodeIDVector, ValueVector& relIDVector, common::offset_t parentOffset) {
    uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
    auto size = dstNodeIDVector.state->getSelVector().getSelSize();
    auto nbrNodes = (common::nodeID_t*)dstNodeIDVector.getData();
    auto relIDs = (common::relID_t*)relIDVector.getData();
    common::nodeID_t dstNodeID;
    common::relID_t edgeID;
    uint8_t state;
    for (auto j = 0u; j < size; j++) {
        dstNodeID = nbrNodes[j];
        edgeID = relIDs[j];
        state = ifeMorsel->visitedNodes[dstNodeID.offset];
        if (state == NOT_VISITED_DST) {
            ifeMorsel->visitedNodes[dstNodeID.offset] = VISITED_DST;
            numDstVisitedLocal++;
            ifeMorsel->pathLength[dstNodeID.offset] = ifeMorsel->currentLevel + 1;
            ifeMorsel->parentOffset[dstNodeID.offset] = parentOffset;
            ifeMorsel->edgeOffset[dstNodeID.offset] = edgeID.offset;
            ifeMorsel->nextFrontier[dstNodeID.offset] = 1u;
        } else if (state == NOT_VISITED) {
            ifeMorsel->visitedNodes[dstNodeID.offset] = VISITED;
            ifeMorsel->nextFrontier[dstNodeID.offset] = 1u;
            ifeMorsel->parentOffset[dstNodeID.offset] = parentOffset;
            ifeMorsel->edgeOffset[dstNodeID.offset] = edgeID.offset;
            numNonDstVisitedLocal++;
        }
    }
    return {numDstVisitedLocal, numNonDstVisitedLocal};
}

static std::pair<uint64_t, uint64_t> visitNbrs(common::offset_t frontierOffset,
    SPPathIFEMorsel* ifeMorsel, graph::Graph* graph) {
    uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
    auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph);
    auto& csr = inMemGraph->getInMemCSR();
    auto csrEntry = csr[frontierOffset >> RIGHT_SHIFT];
    if (!csrEntry) {
        return {0, 0};
    }
    auto posInCSR = frontierOffset & OFFSET_DIV;
    for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1];
        nbrIdx++) {
        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
        auto edgeOffset = csrEntry->relOffsets[nbrIdx];
        auto state = ifeMorsel->visitedNodes[nbrOffset];
        if (state == NOT_VISITED_DST) {
            ifeMorsel->visitedNodes[nbrOffset] = VISITED_DST;
            numDstVisitedLocal++;
            ifeMorsel->pathLength[nbrOffset] = ifeMorsel->currentLevel + 1;
            ifeMorsel->parentOffset[nbrOffset] = frontierOffset;
            ifeMorsel->edgeOffset[nbrOffset] = edgeOffset;
            ifeMorsel->nextFrontier[nbrOffset] = 1u;
        } else if (state == NOT_VISITED) {
            ifeMorsel->visitedNodes[nbrOffset] = VISITED;
            ifeMorsel->parentOffset[nbrOffset] = frontierOffset;
            ifeMorsel->edgeOffset[nbrOffset] = edgeOffset;
            ifeMorsel->nextFrontier[nbrOffset] = 1u;
            numNonDstVisitedLocal++;
        }
    }
    return {numDstVisitedLocal, numNonDstVisitedLocal};
}

static void extendNode(graph::Graph* graph, SPPathIFEMorsel* ifeMorsel,
    const common::offset_t offset, uint64_t& numDstVisitedLocal, uint64_t& numNonDstVisitedLocal,
    graph::NbrScanState* nbrScanState) {
    std::pair<uint64_t, uint64_t> retVal;
    if (graph->isInMemory) {
        retVal = visitNbrs(offset, ifeMorsel, graph);
        numDstVisitedLocal += retVal.first;
        numNonDstVisitedLocal += retVal.second;
    } else {
        graph->initializeStateFwdNbrs(offset, nbrScanState);
        do {
            graph->getFwdNbrs(nbrScanState);
            retVal = visitNbrs(ifeMorsel, *nbrScanState->dstNodeIDVector,
                *nbrScanState->relIDVector, offset);
            numDstVisitedLocal += retVal.first;
            numNonDstVisitedLocal += retVal.second;
        } while (graph->hasMoreFwdNbrs(nbrScanState));
    }
}

// BFS Frontier Extension function, return only after bfs extension complete.
static void extendFrontierFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
    auto ifeMorsel = (SPPathIFEMorsel*)shortestPathLocalState->ifeMorsel;
    while (!ifeMorsel->isBFSCompleteNoLock()) {
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
        std::pair<uint64_t, uint64_t> retVal;
        /*auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
        if (ifeMorsel->isSparseFrontier) {
            for (auto idx = 0u; idx < ifeMorsel->currentFrontierSize; idx++) {
                extendNode(graph.get(), ifeMorsel, ifeMorsel->bfsFrontier[idx], numDstVisitedLocal,
                    numNonDstVisitedLocal, nbrScanState.get());
            }
        } else {
            for (auto offset = 0u; offset < ifeMorsel->visitedNodes.size(); offset++) {
                if (!ifeMorsel->currentFrontier[offset]) {
                    continue;
                }
                extendNode(graph.get(), ifeMorsel, offset, numDstVisitedLocal,
                    numNonDstVisitedLocal, nbrScanState.get());
            }
        }
        ifeMorsel->mergeResults(numDstVisitedLocal, numNonDstVisitedLocal);
        /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
        printf("bfs source: %lu, level: %d extension done in %lu ms\n", ifeMorsel->srcOffset,
            ifeMorsel->currentLevel, millis1 - millis);*/
        ifeMorsel->initializeNextFrontierNoLock();
    }
}

static uint64_t shortestPathOutputFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
    auto ifeMorsel = (SPPathIFEMorsel*)shortestPathLocalState->ifeMorsel;
    auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
    if (!morsel.hasMoreToOutput()) {
        return 0;
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
    srcNodeVector->setValue<nodeID_t>(0, {ifeMorsel->srcOffset, nodeTableID});
    auto pos = 0;
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto pathLength = ifeMorsel->pathLength[offset];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, {offset, nodeTableID});
            pathLengthVector->setValue<int64_t>(pos, pathLength);
            // Max length of intermediate path can be (2 * upper_bound - 1)
            // There can be (upper_bound) no. of edges and (upper_bound - 1) no. of nodes
            auto listEntry = ListVector::addList(pathVector.get(), 2 * pathLength - 1);
            pathVector->setValue<list_entry_t>(pos, listEntry);
            pos++;
            auto parentOffset = ifeMorsel->parentOffset[offset],
                 edgeOffset = ifeMorsel->edgeOffset[offset];
            auto pathIdx = 2 * pathLength - 2;
            while (parentOffset != ifeMorsel->srcOffset) {
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                    {edgeOffset, edgeTableID});
                pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx - 1,
                    {parentOffset, nodeTableID});
                pathIdx -= 2;
                edgeOffset = ifeMorsel->edgeOffset[parentOffset];
                parentOffset = ifeMorsel->parentOffset[parentOffset];
            }
            pathDataVector->setValue<common::internalID_t>(listEntry.offset + pathIdx,
                {edgeOffset, edgeTableID});
        }
    }
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos; // return the no. of output values written to the value vectors
}

static uint64_t mainFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
    shortestPathLocalState->ifeMorsel->init();
    if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
        return shortestPathOutputFunc(sharedState, localState);
    }
    extendFrontierFunc(sharedState, localState);
    /*auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    printf("starting output writing for src: %lu at time: %lu\n",
        shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
    return shortestPathOutputFunc(sharedState, localState);
}

void _1T1SParallelSPPath::exec() {
    auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
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
        auto ifeMorsel = std::make_unique<SPPathIFEMorsel>(extraData->upperBound, lowerBound,
            maxNodeOffset, srcOffset);
        srcOffset++;
        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>(true);
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
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis =
                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            printf("bfs source: %lu finished at time: %lu\n",
                ifeMorselTasks[i].first->srcOffset, millis);*/
            auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                ifeMorselTasks[i].second->task.get());
            free(processorTask->getSink());
            ifeMorselTasks[i].second = nullptr;
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
            ifeMorselTasks[i].first->resetNoLock(srcOffset);
            srcOffset++;
            auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>(true);
            gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                mainFunc, 1u /* maxTaskThreads */};
            ifeMorselTasks[i].second = parallelUtils->submitTaskAndReturn(job);
        }
        std::this_thread::sleep_for(
            std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
    }
}

} // namespace function
} // namespace kuzu
