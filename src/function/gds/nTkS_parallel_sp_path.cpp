#include "function/gds/nTkS_parallel_sp_path.h"

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
            if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[dstNodeID.offset], state,
                    VISITED_DST)) {
                numDstVisitedLocal++;
                __atomic_store_n(&ifeMorsel->pathLength[dstNodeID.offset],
                    ifeMorsel->currentLevel + 1, __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->parentOffset[dstNodeID.offset], parentOffset,
                    __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->edgeOffset[dstNodeID.offset], edgeID.offset,
                    __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u, __ATOMIC_RELEASE);
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[dstNodeID.offset], state,
                    VISITED)) {
                numNonDstVisitedLocal++;
                __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u, __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->parentOffset[dstNodeID.offset], parentOffset,
                    __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->edgeOffset[dstNodeID.offset], edgeID.offset,
                    __ATOMIC_RELEASE);
            }
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
            if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[nbrOffset], state,
                    VISITED_DST)) {
                numDstVisitedLocal++;
                __atomic_store_n(&ifeMorsel->pathLength[nbrOffset], ifeMorsel->currentLevel + 1,
                    __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->parentOffset[nbrOffset], frontierOffset,
                    __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->edgeOffset[nbrOffset], edgeOffset, __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELEASE);
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[nbrOffset], state, VISITED)) {
                numNonDstVisitedLocal++;
                __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->parentOffset[nbrOffset], frontierOffset,
                    __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->edgeOffset[nbrOffset], edgeOffset, __ATOMIC_RELEASE);
            }
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

static uint64_t extendSparseFrontierFunc(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
    auto ifeMorsel = (SPPathIFEMorsel*)shortestPathLocalState->ifeMorsel;
    if (!ifeMorsel->initializedIFEMorsel) {
        ifeMorsel->init();
    }
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = ifeMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0; // return 0 to indicate to thread it can exit from operator
    }
    auto& nbrScanState = shortestPathLocalState->nbrScanState;
    uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
    while (!ifeMorsel->isBFSCompleteNoLock() && frontierMorsel.hasMoreToOutput()) {
        for (auto idx = frontierMorsel.startOffset; idx < frontierMorsel.endOffset; idx++) {
            extendNode(graph.get(), ifeMorsel, ifeMorsel->bfsFrontier[idx], numDstVisitedLocal,
                numNonDstVisitedLocal, nbrScanState.get());
        }
        frontierMorsel = ifeMorsel->getMorsel(morselSize);
    }
    ifeMorsel->mergeResults(numDstVisitedLocal, numNonDstVisitedLocal);
    return 0u;
}

static uint64_t extendDenseFrontierFunc(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
    auto ifeMorsel = (SPPathIFEMorsel*)shortestPathLocalState->ifeMorsel;
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = ifeMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0; // return 0 to indicate to thread it can exit from operator
    }
    auto& nbrScanState = shortestPathLocalState->nbrScanState;
    uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
    while (!ifeMorsel->isBFSCompleteNoLock() && frontierMorsel.hasMoreToOutput()) {
        for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
            offset++) {
            if (!ifeMorsel->currentFrontier[offset]) {
                continue;
            }
            extendNode(graph.get(), ifeMorsel, offset, numDstVisitedLocal, numNonDstVisitedLocal,
                nbrScanState.get());
        }
        frontierMorsel = ifeMorsel->getMorsel(morselSize);
    }
    ifeMorsel->mergeResults(numDstVisitedLocal, numNonDstVisitedLocal);
    return 0u;
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

void nTkSParallelSPPath::exec() {
    auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
    auto morselSize = sharedState->graph->isInMemory ? 512LU : 256LU;
    auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
    auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
    auto maxConcurrentBFS = std::max(1LU, concurrentBFS);
    printf("max concurrent bfs setting: %lu, launching maxConcurrentBFS as: %lu\n", concurrentBFS,
        maxConcurrentBFS);
    auto maxNodeOffset = sharedState->graph->getNumNodes() - 1;
    auto lowerBound = 1u;
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    scheduledTaskMap ifeMorselTasks = scheduledTaskMap();
    std::vector<ParallelUtilsJob> jobs;    // stores the next batch of jobs to submit
    std::vector<unsigned int> jobIdxInMap; // stores the scheduledTaskMap idx <-> job mapping
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
        auto ifeMorsel = std::make_unique<SPPathIFEMorsel>(extraData->upperBound, lowerBound,
            maxNodeOffset, srcOffset);
        srcOffset++;
        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>(true);
        gdsLocalState->ifeMorsel = ifeMorsel.get();
        jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
            extendSparseFrontierFunc, 1 /* maxTaskThreads */});
        ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
    }
    if (totalBFSSources == 0) {
        return;
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
            if (ifeMorselTasks[i].first->isIFEMorselCompleteNoLock()) {
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
                auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, extendSparseFrontierFunc, 1u /* maxTaskThreads */});
                jobIdxInMap.push_back(i);
                continue;
            }
            bool isBFSComplete = ifeMorselTasks[i].first->isBFSCompleteNoLock();
            if (isBFSComplete) {
                auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                auto maxTaskThreads = std::min(maxThreads,
                    (uint64_t)std::ceil(ifeMorselTasks[i].first->maxOffset / 2048));
                jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, shortestPathOutputFunc, maxTaskThreads});
                jobIdxInMap.push_back(i);
                continue;
            } else {
                ifeMorselTasks[i].first->initializeNextFrontierNoLock();
                auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>(true);
                gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                auto maxTaskThreads = std::min(maxThreads,
                    (uint64_t)std::ceil(ifeMorselTasks[i].first->currentFrontierSize / morselSize));
                if (gdsLocalState->ifeMorsel->isSparseFrontier) {
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendSparseFrontierFunc, maxTaskThreads});
                } else {
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendDenseFrontierFunc, maxTaskThreads});
                }
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
