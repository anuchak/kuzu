#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/sp_ife_morsel.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"
#include "function/gds/1T1S_parallel_sp_length.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::pair<uint64_t, uint64_t> visitNbrs(SPIFEMorsel* ifeMorsel,
    ValueVector& dstNodeIDVector) {
    uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
    auto size = dstNodeIDVector.state->getSelVector().getSelSize();
    auto nbrNodes = (common::nodeID_t*)dstNodeIDVector.getData();
    common::nodeID_t dstNodeID;
    uint8_t state;
    for (auto j = 0u; j < size; j++) {
        dstNodeID = nbrNodes[j];
        state = ifeMorsel->visitedNodes[dstNodeID.offset];
        if (state == NOT_VISITED_DST) {
            ifeMorsel->visitedNodes[dstNodeID.offset] = VISITED_DST;
            numDstVisitedLocal++;
            ifeMorsel->pathLength[dstNodeID.offset] = ifeMorsel->currentLevel + 1;
            ifeMorsel->nextFrontier[dstNodeID.offset] = 1u;
        } else if (state == NOT_VISITED) {
            ifeMorsel->visitedNodes[dstNodeID.offset] = VISITED;
            ifeMorsel->nextFrontier[dstNodeID.offset] = 1u;
            numNonDstVisitedLocal++;
        }
    }
    return {numDstVisitedLocal, numNonDstVisitedLocal};
}

static std::pair<uint64_t, uint64_t> visitNbrs(common::offset_t frontierOffset,
    SPIFEMorsel* ifeMorsel, graph::Graph* graph) {
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
        auto state = ifeMorsel->visitedNodes[nbrOffset];
        if (state == NOT_VISITED_DST) {
            ifeMorsel->visitedNodes[nbrOffset] = VISITED_DST;
            numDstVisitedLocal++;
            ifeMorsel->pathLength[nbrOffset] = ifeMorsel->currentLevel + 1;
            ifeMorsel->nextFrontier[nbrOffset] = 1u;
        } else if (state == NOT_VISITED) {
            ifeMorsel->visitedNodes[nbrOffset] = VISITED;
            ifeMorsel->nextFrontier[nbrOffset] = 1u;
            numNonDstVisitedLocal++;
        }
    }
    return {numDstVisitedLocal, numNonDstVisitedLocal};
}

static void extendNode(graph::Graph* graph, SPIFEMorsel* ifeMorsel, const common::offset_t offset,
    uint64_t& numDstVisitedLocal, uint64_t& numNonDstVisitedLocal,
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
            retVal = visitNbrs(ifeMorsel, *nbrScanState->dstNodeIDVector);
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
    auto ifeMorsel = (SPIFEMorsel *) shortestPathLocalState->ifeMorsel;
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
    auto ifeMorsel = (SPIFEMorsel *) shortestPathLocalState->ifeMorsel;
    if (ifeMorsel->nextDstScanStartIdx >= ifeMorsel->maxOffset) {
        return 0;
    }
    auto tableID = sharedState->graph->getNodeTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    srcNodeVector->setValue<nodeID_t>(0, {ifeMorsel->srcOffset, tableID});
    auto pos = 0LU, offset = ifeMorsel->nextDstScanStartIdx.load(std::memory_order_acq_rel);
    while (pos < DEFAULT_VECTOR_CAPACITY && offset <= ifeMorsel->maxOffset) {
        auto pathLength = ifeMorsel->pathLength[offset];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, {offset, tableID});
            pathLengthVector->setValue<int64_t>(pos, pathLength);
            pos++;
        }
        offset++;
    }
    ifeMorsel->nextDstScanStartIdx.store(offset, std::memory_order_acq_rel);
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

void _1T1SParallelShortestPath::exec() {
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
        auto ifeMorsel = std::make_unique<SPIFEMorsel>(extraData->upperBound, lowerBound,
            maxNodeOffset, srcOffset);
        srcOffset++;
        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
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
            delete processorTask->getSink();
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
            auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
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
