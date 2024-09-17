#include "binder/binder.h"
#include "common/types/types.h"
#include "function/gds/sp_ife_morsel.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "function/gds/nT1S_parallel_sp_length.h"

using namespace kuzu::common;
using namespace kuzu::binder;

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
            if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[dstNodeID.offset], state, VISITED_DST)) {
                numDstVisitedLocal++;
                __atomic_store_n(&ifeMorsel->pathLength[dstNodeID.offset],
                    ifeMorsel->currentLevel + 1, __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u,
                    __ATOMIC_RELEASE);
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[dstNodeID.offset], state, VISITED)) {
                numNonDstVisitedLocal++;
                __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u,
                    __ATOMIC_RELEASE);
            }
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
            if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[nbrOffset], state, VISITED_DST)) {
                numDstVisitedLocal++;
                __atomic_store_n(&ifeMorsel->pathLength[nbrOffset], ifeMorsel->currentLevel + 1,
                    __ATOMIC_RELEASE);
                __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELEASE);
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[nbrOffset], state, VISITED)) {
                numNonDstVisitedLocal++;
                __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELEASE);
            }
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

static uint64_t extendSparseFrontierFunc(GDSCallSharedState* sharedState,
    GDSLocalState* localState) {
    auto& graph = sharedState->graph;
    auto shortestPathLocalState =
        common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
    auto ifeMorsel = (SPIFEMorsel *) shortestPathLocalState->ifeMorsel;
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
    auto ifeMorsel = (SPIFEMorsel *) shortestPathLocalState->ifeMorsel;
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
            extendNode(graph.get(), ifeMorsel, offset, numDstVisitedLocal,
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
    auto ifeMorsel = (SPIFEMorsel *) shortestPathLocalState->ifeMorsel;
    auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto tableID = sharedState->graph->getNodeTableID();
    auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
    auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
    auto& pathLengthVector = shortestPathLocalState->lengthVector;
    srcNodeVector->setValue<nodeID_t>(0, {ifeMorsel->srcOffset, tableID});
    auto pos = 0;
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto pathLength = ifeMorsel->pathLength[offset];
        if (pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<nodeID_t>(pos, {offset, tableID});
            pathLengthVector->setValue<int64_t>(pos, pathLength);
            pos++;
        }
    }
    if (pos == 0) {
        return UINT64_MAX;
    }
    dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
    return pos; // return the no. of output values written to the value vectors
}

void nT1SParallelShortestPath::exec() {
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
        ifeMorsel->resetNoLock(offset);
        ifeMorsel->init();
        while (!ifeMorsel->isBFSCompleteNoLock()) {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis =
                std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            // printf("starting bfs level: %d\n", ifeMorsel->currentLevel);
            auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto maxTaskThreads = std::min(maxThreads,
                (uint64_t)std::ceil(ifeMorsel->currentFrontierSize / morselSize));
            if (ifeMorsel->isSparseFrontier) {
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, extendSparseFrontierFunc, maxTaskThreads};
                parallelUtils->submitParallelTaskAndWait(job);
            } else {
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                    sharedState, extendDenseFrontierFunc, maxTaskThreads};
                parallelUtils->submitParallelTaskAndWait(job);
            }
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 =
                std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                millis1 - millis);*/
            ifeMorsel->initializeNextFrontierNoLock();
        }
        /*auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
        gdsLocalState->ifeMorsel = ifeMorsel.get();
        auto maxTaskThreads =
            std::min(maxThreads, (uint64_t)std::ceil(ifeMorsel->maxOffset / 2048));
        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
            shortestPathOutputFunc, maxTaskThreads};
        parallelUtils->submitParallelTaskAndWait(job);
        /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
        printf("output writing completed in %lu ms\n", millis1 - millis);*/
    }
}

} // namespace function
} // namespace kuzu
