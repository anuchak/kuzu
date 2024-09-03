#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/ife_morsel.h"
#include "function/gds/sp_ife_morsel.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

class ParallelSPLengths : public GDSAlgorithm {
public:
    ParallelSPLengths() = default;
    ParallelSPLengths(const ParallelSPLengths& other) = default;

    /*
     * Inputs are
     *
     * graph::ANY
     * srcNode::NODE
     * upperBound::INT64
     * bfsPolicy::STRING
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64,
            LogicalTypeID::STRING};
    }

    /*
     * Outputs are
     *
     * srcNode._id::INTERNAL_ID
     * dst::INTERNAL_ID
     * length::INT64
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        columns.push_back(bindData->nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(binder->createVariable("dst", LogicalType::INTERNAL_ID()));
        columns.push_back(binder->createVariable("length", LogicalType::INT64()));
        return columns;
    }

    void bind(const binder::expression_vector& params) override {
        KU_ASSERT(params.size() == 4);
        auto inputNode = params[1];
        ExpressionUtil::validateExpressionType(*params[2], ExpressionType::LITERAL);
        ExpressionUtil::validateDataType(*params[2], LogicalType::INT64());
        ExpressionUtil::validateExpressionType(*params[3], ExpressionType::LITERAL);
        auto upperBound = params[2]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        auto bfsPolicy =
            params[3]->constCast<LiteralExpression>().getValue().getValue<std::string>();
        bindData = std::make_unique<ParallelShortestPathBindData>(inputNode, upperBound, bfsPolicy);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<ParallelShortestPathLocalState>();
        localState->init(context);
    }

    static std::pair<uint64_t, uint64_t> visitNbrsOnDiskSingle(SPIFEMorsel* ifeMorsel,
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

    static std::pair<uint64_t, uint64_t> visitNbrsInMemSingle(common::offset_t frontierOffset,
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

    static std::pair<uint64_t, uint64_t> visitNbrsOnDiskParallel(SPIFEMorsel* ifeMorsel,
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
                if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[dstNodeID.offset], state,
                        VISITED_DST)) {
                    numDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->pathLength[dstNodeID.offset],
                        ifeMorsel->currentLevel + 1, __ATOMIC_RELEASE);
                    __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u,
                        __ATOMIC_RELEASE);
                }
            } else if (state == NOT_VISITED) {
                if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[dstNodeID.offset], state,
                        VISITED)) {
                    numNonDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u,
                        __ATOMIC_RELEASE);
                }
            }
        }
        return {numDstVisitedLocal, numNonDstVisitedLocal};
    }

    static std::pair<uint64_t, uint64_t> visitNbrsInMemParallel(common::offset_t frontierOffset,
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
                if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[nbrOffset], state,
                        VISITED_DST)) {
                    numDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->pathLength[nbrOffset], ifeMorsel->currentLevel + 1,
                        __ATOMIC_RELEASE);
                    __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELEASE);
                }
            } else if (state == NOT_VISITED) {
                if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[nbrOffset], state,
                        VISITED)) {
                    numNonDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELEASE);
                }
            }
        }
        return {numDstVisitedLocal, numNonDstVisitedLocal};
    }

    static void extendNodeParallel(graph::Graph* graph, SPIFEMorsel* ifeMorsel, const common::offset_t offset,
        uint64_t& numDstVisitedLocal, uint64_t& numNonDstVisitedLocal,
        graph::NbrScanState* nbrScanState) {
        std::pair<uint64_t, uint64_t> retVal;
        if (graph->isInMemory) {
            retVal = visitNbrsInMemParallel(offset, ifeMorsel, graph);
            numDstVisitedLocal += retVal.first;
            numNonDstVisitedLocal += retVal.second;
        } else {
            graph->initializeStateFwdNbrs(offset, nbrScanState);
            do {
                graph->getFwdNbrs(nbrScanState);
                retVal = visitNbrsOnDiskParallel(ifeMorsel, *nbrScanState->dstNodeIDVector);
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
        auto ifeMorsel = (SPIFEMorsel *)shortestPathLocalState->ifeMorsel;
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
                extendNodeParallel(graph.get(), ifeMorsel, ifeMorsel->bfsFrontier[idx],
                    numDstVisitedLocal, numNonDstVisitedLocal, nbrScanState.get());
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
        auto ifeMorsel = (SPIFEMorsel *)shortestPathLocalState->ifeMorsel;
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
                extendNodeParallel(graph.get(), ifeMorsel, offset, numDstVisitedLocal,
                    numNonDstVisitedLocal, nbrScanState.get());
            }
            frontierMorsel = ifeMorsel->getMorsel(morselSize);
        }
        ifeMorsel->mergeResults(numDstVisitedLocal, numNonDstVisitedLocal);
        return 0u;
    }

    static uint64_t shortestPathOutputFunc(GDSCallSharedState* sharedState,
        GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = (SPIFEMorsel *)shortestPathLocalState->ifeMorsel;
        auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto tableID = sharedState->graph->getNodeTableID();
        auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
        auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
        auto& pathLengthVector = shortestPathLocalState->lengthVector;
        srcNodeVector->setValue<nodeID_t>(0, common::nodeID_t{ifeMorsel->srcOffset, tableID});
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            uint64_t pathLength = ifeMorsel->pathLength[offset];
            if (pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, common::nodeID_t{offset, tableID});
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return UINT64_MAX;
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos; // return the no. of output values written to the value vectors
    }

    static void extendNodeSingle(graph::Graph* graph, SPIFEMorsel* ifeMorsel,
        const common::offset_t offset, uint64_t& numDstVisitedLocal,
        uint64_t& numNonDstVisitedLocal, graph::NbrScanState* nbrScanState) {
        std::pair<uint64_t, uint64_t> retVal;
        if (graph->isInMemory) {
            retVal = visitNbrsInMemSingle(offset, ifeMorsel, graph);
            numDstVisitedLocal += retVal.first;
            numNonDstVisitedLocal += retVal.second;
        } else {
            graph->initializeStateFwdNbrs(offset, nbrScanState);
            do {
                graph->getFwdNbrs(nbrScanState);
                retVal = visitNbrsOnDiskSingle(ifeMorsel, *nbrScanState->dstNodeIDVector);
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
        auto ifeMorsel = (SPIFEMorsel *)shortestPathLocalState->ifeMorsel;
        while (!ifeMorsel->isBFSCompleteNoLock()) {
            auto& nbrScanState = shortestPathLocalState->nbrScanState;
            uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            if (ifeMorsel->isSparseFrontier) {
                for (auto idx = 0u; idx < ifeMorsel->currentFrontierSize; idx++) {
                    extendNodeSingle(graph.get(), ifeMorsel, ifeMorsel->bfsFrontier[idx],
                        numDstVisitedLocal, numNonDstVisitedLocal, nbrScanState.get());
                }
            } else {
                for (auto offset = 0u; offset < ifeMorsel->visitedNodes.size(); offset++) {
                    if (!ifeMorsel->currentFrontier[offset]) {
                        continue;
                    }
                    extendNodeSingle(graph.get(), ifeMorsel, offset, numDstVisitedLocal,
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

    void executenT1SPolicy(processor::ExecutionContext* executionContext) {
        auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
        auto morselSize = sharedState->graph->isInMemory ? 512LU : 256LU;
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        auto numNodes = sharedState->graph->getNumNodes();
        auto ifeMorsel = std::make_unique<SPIFEMorsel>(extraData->upperBound, 1, numNodes - 1,
            common::INVALID_OFFSET);
        auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
        for (auto offset = 0u; offset < numNodes; offset++) {
            if (!inputMask->isMasked(offset)) {
                continue;
            }
            auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            ifeMorsel->resetNoLock(offset);
            ifeMorsel->init();
            while (!ifeMorsel->isBFSCompleteNoLock()) {
                // printf("starting bfs level: %d\n", ifeMorsel->currentLevel);
                auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                auto maxTaskThreads = std::min(maxThreads,
                    (uint64_t)std::ceil((double)ifeMorsel->currentFrontierSize / morselSize));
                if (ifeMorsel->isSparseFrontier) {
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendSparseFrontierFunc, maxTaskThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                } else {
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendDenseFrontierFunc, maxTaskThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                }
                auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                    millis1 - millis);
                ifeMorsel->initializeNextFrontierNoLock();
                duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                printf("bfs level: %d initialized in %ld ms \n", ifeMorsel->currentLevel,
                    millis - millis1);
            }
            duration = std::chrono::system_clock::now().time_since_epoch();
            millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto maxTaskThreads =
                std::min(maxThreads, (uint64_t)std::ceil(ifeMorsel->maxOffset / 2048));
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                shortestPathOutputFunc, maxTaskThreads};
            parallelUtils->submitParallelTaskAndWait(job);
            auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("output writing completed in %lu ms\n", millis1 - millis);
            printf("source: %u completed in %lu ms\n", offset, millis1 - ifeMorsel->startTime);
        }
    }

    void execute1T1SPolicy(processor::ExecutionContext* executionContext) {
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        // threads available will be 1 less than total (main thread makes gds call)
        auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
        // set max bfs always to min value between threads available and maxConcurrentBFS value
        auto maxConcurrentBFS = std::max(1LU, concurrentBFS);
        printf("max concurrent bfs setting: %lu, launching maxConcurrentBFS as: %lu\n",
            concurrentBFS, maxConcurrentBFS);
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
                auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                gdsLocalState->ifeMorsel = ifeMorselTask.first.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    mainFunc, 1u /* maxTaskThreads */};
                ifeMorselTask.second = parallelUtils->submitTaskAndReturn(job);
            }
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        }
    }

    void executenTkSPolicy(processor::ExecutionContext* executionContext) {
        auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
        auto morselSize = sharedState->graph->isInMemory ? 512LU : 256LU;
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        // threads available will be 1 less than total (main thread makes gds call)
        auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
        // set max bfs always to min value between threads available and maxConcurrentBFS value
        auto maxConcurrentBFS = std::max(1LU, concurrentBFS);
        printf("max concurrent bfs setting: %lu, launching maxConcurrentBFS as: %lu\n",
            concurrentBFS, maxConcurrentBFS);
        auto maxNodeOffset = sharedState->graph->getNumNodes() - 1;
        auto lowerBound = 1u;
        auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
        scheduledTaskMap ifeMorselTasks = scheduledTaskMap();
        std::vector<ParallelUtilsJob> jobs; // stores the next batch of jobs to submit
        std::vector<int> jobIdxInMap;       // stores the scheduledTaskMap idx <-> job mapping
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
            auto ifeMorsel = std::make_unique<SPIFEMorsel>(extraData->upperBound, lowerBound,
                maxNodeOffset, srcOffset);
            auto duration = std::chrono::system_clock::now().time_since_epoch();
            ifeMorsel->startTime =
                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            srcOffset++;
            auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                extendSparseFrontierFunc, 1 /* maxTaskThreads */});
            ifeMorselTasks.push_back({std::move(ifeMorsel), nullptr});
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
            for (auto i = 0; i < ifeMorselTasks.size(); i++) {
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
                    free(processorTask->getSink());
                    ifeMorselTasks[i].second = nullptr;
                    numCompletedTasks++;
                    auto duration = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    printf("source: %lu is completed in %lu\n", ifeMorselTasks[i].first->srcOffset,
                        millis1 - ifeMorselTasks[i].first->startTime);
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
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendSparseFrontierFunc, 1u /* maxTaskThreads */});
                    jobIdxInMap.push_back(i);
                    continue;
                }
                bool isBFSComplete = ifeMorselTasks[i].first->isBFSCompleteNoLock();
                if (isBFSComplete) {
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    auto maxTaskThreads = std::min(maxThreads,
                        (uint64_t)std::ceil(ifeMorselTasks[i].first->maxOffset / 2048));
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, shortestPathOutputFunc, maxTaskThreads});
                    jobIdxInMap.push_back(i);
                    continue;
                } else {
                    ifeMorselTasks[i].first->initializeNextFrontierNoLock();
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    auto maxTaskThreads = std::min(maxThreads,
                        (uint64_t)std::ceil(
                            ifeMorselTasks[i].first->currentFrontierSize / morselSize));
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

    void exec(processor::ExecutionContext* executionContext) override {
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        auto bfsPolicy = extraData->bfsPolicy;
        if (bfsPolicy == "nT1S") {
            return executenT1SPolicy(executionContext);
        } else if (bfsPolicy == "1T1S") {
            return execute1T1SPolicy(executionContext);
        } else {
            return executenTkSPolicy(executionContext);
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<ParallelSPLengths>(*this);
    }
};

function_set ParallelSPLengthsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name, std::make_unique<ParallelSPLengths>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
