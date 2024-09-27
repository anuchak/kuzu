#include "function/gds/nT1S_parallel_sp_mega_length.h"

#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/parallel_msbfs_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/sp_ife_mega_morsel.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

static void visitNbrs(SPIFEMegaMorsel* ifeMorsel, ValueVector& dstNodeIDVector,
    std::vector<int>& activeLanes) {
    auto size = dstNodeIDVector.state->getSelVector().getSelSize();
    auto nbrNodes = (common::nodeID_t*)dstNodeIDVector.getData();
    common::nodeID_t dstNodeID;
    uint8_t state;
    for (auto lane : activeLanes) {
        auto& spIFEMorsel = ifeMorsel->spIFEMorsels[lane];
        for (auto j = 0u; j < size; j++) {
            dstNodeID = nbrNodes[j];
            state = spIFEMorsel->visitedNodes[dstNodeID.offset];
            if (state == NOT_VISITED_DST) {
                if (__sync_bool_compare_and_swap(&spIFEMorsel->visitedNodes[dstNodeID.offset],
                        state, VISITED_DST)) {
                    __atomic_store_n(&spIFEMorsel->pathLength[dstNodeID.offset],
                        ifeMorsel->currentLevel + 1, __ATOMIC_RELEASE);
                    __atomic_store_n(&spIFEMorsel->nextFrontier[dstNodeID.offset], 1u,
                        __ATOMIC_RELEASE);
                }
            } else if (state == NOT_VISITED) {
                if (__sync_bool_compare_and_swap(&spIFEMorsel->visitedNodes[dstNodeID.offset],
                        state, VISITED)) {
                    __atomic_store_n(&spIFEMorsel->nextFrontier[dstNodeID.offset], 1u,
                        __ATOMIC_RELEASE);
                }
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
                if (__sync_bool_compare_and_swap(&spIFEMorsel->visitedNodes[nbrOffset], state,
                        VISITED_DST)) {
                    __atomic_store_n(&spIFEMorsel->pathLength[nbrOffset],
                        ifeMorsel->currentLevel + 1, __ATOMIC_RELEASE);
                    __atomic_store_n(&spIFEMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELEASE);
                }
            } else if (state == NOT_VISITED) {
                if (__sync_bool_compare_and_swap(&spIFEMorsel->visitedNodes[nbrOffset], state,
                        VISITED)) {
                    __atomic_store_n(&spIFEMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELEASE);
                }
            }
        }
    }
}

static void extendNode(graph::Graph* graph, SPIFEMegaMorsel* ifeMorsel,
    const common::offset_t offset, graph::NbrScanState* nbrScanState,
    std::vector<int>& activeLanes) {
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
    auto morselSize = graph->isInMemory ? 512LU : 256LU;
    auto frontierMorsel = ifeMorsel->getMorsel(morselSize);
    if (!frontierMorsel.hasMoreToOutput()) {
        return 0; // return 0 to indicate to thread it can exit from operator
    }
    auto& nbrScanState = shortestPathLocalState->nbrScanState;
    auto activeLanes = std::vector<int>();
    while (frontierMorsel.hasMoreToOutput()) {
        for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
            offset++) {
            // Cache inefficient, main reason for slowdown of this mega morsel approach
            // Unlike MS-BFS, we have to jump from 1 SPIFEMorsel to another to see if a node
            // is active. In MS-BFS, with 1 read we know whether a node is active or not.
            for (auto lane = 0; lane < ifeMorsel->totalActiveLanes; lane++) {
                auto &spIFEMorsel = ifeMorsel->spIFEMorsels[lane];
                if (spIFEMorsel->currentFrontier[offset]) {
                    activeLanes.push_back(lane);
                }
            }
            if (activeLanes.empty()) {
                continue;
            }
            extendNode(graph.get(), ifeMorsel, offset, nbrScanState.get(), activeLanes);
            activeLanes.clear();
        }
        frontierMorsel = ifeMorsel->getMorsel(morselSize);
    }
    return 0u;
}

static uint64_t shortestPathOutputFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
    auto ifeMorsel = (SPIFEMegaMorsel*)shortestPathLocalState->ifeMorsel;
    auto &currentDstLane = shortestPathLocalState->currentDstLane;
    auto &morsel = shortestPathLocalState->dstScanMorsel;
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

void nT1SParallelMegaShortestPath::exec() {
    auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
    auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
    auto batchSize = extraData->laneWidth;
    auto numNodes = sharedState->graph->getNumNodes();
    auto ifeMorsel = std::make_unique<SPIFEMegaMorsel>(extraData->upperBound, 1, numNodes - 1,
        batchSize);
    auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
    for (auto offset = 0u; offset < numNodes; offset++) {
        if (!inputMask->isMasked(offset)) {
            continue;
        }
        ifeMorsel->spIFEMorsels[ifeMorsel->totalActiveLanes]->srcOffset = offset;
        ifeMorsel->totalActiveLanes++;
        if (ifeMorsel->totalActiveLanes == batchSize) {
            ifeMorsel->resetNoLock(common::INVALID_OFFSET /* not being used */);
            ifeMorsel->init();
            while (!ifeMorsel->isBFSCompleteNoLock()) {
                /*auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                // printf("starting bfs level: %d\n", ifeMorsel->currentLevel);
                auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
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
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                shortestPathOutputFunc, maxThreads};
            parallelUtils->submitParallelTaskAndWait(job);
            ifeMorsel->totalActiveLanes = 0;
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("output writing completed in %lu ms\n", millis1 - millis);*/
        }
    }
    if (ifeMorsel->totalActiveLanes > 0) {
        ifeMorsel->resetNoLock(common::INVALID_OFFSET /* not being used */);
        ifeMorsel->init();
        while (!ifeMorsel->isBFSCompleteNoLock()) {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis =
                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            // printf("starting bfs level: %d\n", ifeMorsel->currentLevel);
            auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
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
        auto gdsLocalState = std::make_unique<ParallelMSBFSLocalState>();
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
