#include "function/gds/1T1S_parallel_sp_mega_length.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/parallel_msbfs_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/sp_ife_mega_morsel.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

typedef std::vector<std::pair<std::unique_ptr<SPIFEMegaMorsel>,
    std::shared_ptr<ScheduledTask>>> scheduledTaskMap;

static void visitNbrs(SPIFEMegaMorsel* ifeMorsel, ValueVector& dstNodeIDVector,
    std::vector<int>& activeLanes) {
    auto size = dstNodeIDVector.state->getSelVector().getSelSize();
    auto nbrNodes = (common::nodeID_t*)dstNodeIDVector.getData();
    common::nodeID_t dstNodeID;
    uint8_t state;
    for (auto j = 0u; j < size; j++) {
        dstNodeID = nbrNodes[j];
        for (auto lane : activeLanes) {
            auto& spIFEMorsel = ifeMorsel->spIFEMorsels[lane];
            state = spIFEMorsel->visitedNodes[dstNodeID.offset];
            if (state == NOT_VISITED_DST) {
                spIFEMorsel->visitedNodes[dstNodeID.offset] = VISITED_DST;
                spIFEMorsel->pathLength[dstNodeID.offset] = ifeMorsel->currentLevel + 1;
                spIFEMorsel->nextFrontier[dstNodeID.offset] = 1u;
            } else if (state == NOT_VISITED) {
                spIFEMorsel->visitedNodes[dstNodeID.offset] = VISITED;
                spIFEMorsel->nextFrontier[dstNodeID.offset] = 1u;
            }
        }
    }
}

static void visitNbrs(common::offset_t frontierOffset, SPIFEMegaMorsel* ifeMorsel,
    graph::Graph* graph, std::vector<int>& activeLanes) {
    auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph);
    auto& csr = inMemGraph->getInMemCSR();
    auto csrEntry = csr[frontierOffset >> RIGHT_SHIFT];
    if (!csrEntry) {
        return;
    }
    auto posInCSR = frontierOffset & OFFSET_DIV;
    for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1];
        nbrIdx++) {
        auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
        for (auto lane : activeLanes) {
            auto& spIFEMorsel = ifeMorsel->spIFEMorsels[lane];
            auto state = spIFEMorsel->visitedNodes[nbrOffset];
            if (state == NOT_VISITED_DST) {
                spIFEMorsel->visitedNodes[nbrOffset] = VISITED_DST;
                spIFEMorsel->pathLength[nbrOffset] = ifeMorsel->currentLevel + 1;
                spIFEMorsel->nextFrontier[nbrOffset] = 1u;
            } else if (state == NOT_VISITED) {
                spIFEMorsel->visitedNodes[nbrOffset] = VISITED;
                spIFEMorsel->nextFrontier[nbrOffset] = 1u;
            }
        }
    }
}

static void extendNode(graph::Graph* graph, SPIFEMegaMorsel* ifeMorsel,
    const common::offset_t offset, graph::NbrScanState* nbrScanState,
    std::vector<int>& activeLanes) {
    std::pair<uint64_t, uint64_t> retVal;
    if (graph->isInMemory) {
        visitNbrs(offset, ifeMorsel, graph, activeLanes);
    } else {
        graph->initializeStateFwdNbrs(offset, nbrScanState);
        do {
            graph->getFwdNbrs(nbrScanState);
            visitNbrs(ifeMorsel, *nbrScanState->dstNodeIDVector, activeLanes);
        } while (graph->hasMoreFwdNbrs(nbrScanState));
    }
}

static uint64_t extendFrontierFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (SPIFEMegaMorsel*)shortestPathLocalState->ifeMorsel;
    auto& nbrScanState = shortestPathLocalState->nbrScanState;
    auto activeLanes = std::vector<int>();
    while (!ifeMorsel->isBFSCompleteNoLock()) {
        for (auto offset = 0u; offset <= ifeMorsel->maxOffset; offset++) {
            auto lane = 0;
            for (auto& spIFEMorsel : ifeMorsel->spIFEMorsels) {
                if (spIFEMorsel->currentFrontier[offset]) {
                    activeLanes.push_back(lane);
                }
                lane++;
            }
            if (activeLanes.empty()) {
                continue;
            }
            extendNode(graph.get(), ifeMorsel, offset, nbrScanState.get(), activeLanes);
            activeLanes.clear();
        }
        ifeMorsel->initializeNextFrontierNoLock();
    }
    return 0u;
}

static uint64_t shortestPathOutputFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (SPIFEMegaMorsel*) shortestPathLocalState->ifeMorsel;
    auto& currentDstLane = shortestPathLocalState->currentDstLane;
    auto& morsel = shortestPathLocalState->dstScanMorsel;
    if (currentDstLane == ifeMorsel->totalActiveLanes || currentDstLane == UINT8_MAX) {
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
    auto& spifeMorsel = ifeMorsel->spIFEMorsels[currentDstLane];
    srcNodeVector->setValue<nodeID_t>(0, {spifeMorsel->srcOffset, tableID});
    auto pos = 0;
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto pathLength = spifeMorsel->pathLength[offset];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, {offset, tableID});
            pathLengthVector->setValue<int64_t>(pos, pathLength);
            pos++;
        }
    }
    currentDstLane++;
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos; // return the no. of output values written to the value vectors
}

static uint64_t mainFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
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

void _1T1SParallelMegaShortestPath::exec() {
    auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
    auto batchSize = extraData->laneWidth;
    auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
    auto maxConcurrentMorsels = std::max(1LU, concurrentBFS);
    printf("max concurrent bfs setting: %lu, launching maxConcurrentBFS as: %lu\n", concurrentBFS,
        maxConcurrentMorsels);
    auto maxNodeOffset = sharedState->graph->getNumNodes() - 1;
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    auto srcOffset = 0LU;
    std::vector<ParallelUtilsJob> jobs;    // stores the next batch of jobs to submit
    std::vector<unsigned int> jobIdxInMap; // stores the scheduledTaskMap idx <-> job mapping
    auto numCompletedMorsels = 0, totalMorsels = 0, countSources = 0;
    auto nextBatch = std::vector<common::offset_t>();
    auto ifeMorselTasks = scheduledTaskMap();
    /*
     * We need to seed `maxConcurrentBFS` no. of tasks into the queue first. And then we reuse
     * the IFEMorsels initialized again and again for further tasks.
     * (1) Prepare at most maxConcurrentBFS no. of IFEMorsels as tasks to push into queue
     * (2) If we reach maxConcurrentBFS before reaching end of total nodes, then break.
     * (3) If we reach total nodes before we hit maxConcurrentBFS, then break.
     */
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
        nextBatch.push_back(srcOffset);
        srcOffset++;
        if (countSources == batchSize) {
            totalMorsels++;
            countSources = 0;
            auto ifeMorsel = std::make_unique<SPIFEMegaMorsel>(extraData->upperBound, 1,
                maxNodeOffset, batchSize);
            ifeMorsel->totalActiveLanes = batchSize;
            for (auto i = 0 ; i < batchSize; i++) {
                ifeMorsel->spIFEMorsels[i]->srcOffset = nextBatch[i];
            }
            nextBatch.clear();
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                sharedState, mainFunc, 1});
            ifeMorselTasks.emplace_back(std::move(ifeMorsel), nullptr);
        }
    }
    if (countSources > 0) {
        auto ifeMorsel = std::make_unique<SPIFEMegaMorsel>(extraData->upperBound, 1,
            maxNodeOffset, countSources);
        ifeMorsel->totalActiveLanes = countSources;
        countSources = 0;
        srcOffset++;
        totalMorsels++;
        for (auto i = 0; i < ifeMorsel->totalActiveLanes; i++) {
            ifeMorsel->spIFEMorsels[i]->srcOffset = nextBatch[i];
        }
        nextBatch.clear();
        auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
        gdsLocalState->ifeMorsel = ifeMorsel.get();
        jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
            mainFunc, 1});
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
            auto &schedTask = ifeMorselTasks[i].second;
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
                free(processorTask->getSink());
                ifeMorselTasks[i].second = nullptr;
                numCompletedMorsels++;
                ifeMorselTasks[i].first->resetNoLock(common::INVALID_OFFSET);
                while ((srcOffset <= maxNodeOffset) && (countSources < batchSize)) {
                    if (inputMask->isMasked(srcOffset)) {
                        ifeMorselTasks[i].first->spIFEMorsels[countSources]->srcOffset = srcOffset;
                        countSources++;
                    }
                    srcOffset++;
                }
                if (countSources > 0) {
                    ifeMorselTasks[i].first->totalActiveLanes = countSources;
                    countSources = 0;
                    totalMorsels++;
                    auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, mainFunc, 1});
                    jobIdxInMap.push_back(i);
                    continue;
                }
                if ((srcOffset > maxNodeOffset) && (totalMorsels == numCompletedMorsels)) {
                    return;
                } else if (srcOffset > maxNodeOffset) {
                    continue;
                }
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
