#include "binder/binder.h"
#include "common/exception/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/ife_morsel.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/msbfs_ife_morsel.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"
#include <immintrin.h>

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

class ParallelMSBFSLengths : public GDSAlgorithm {
public:
    ParallelMSBFSLengths() = default;
    ParallelMSBFSLengths(const ParallelMSBFSLengths& other) = default;

    /*
     * Inputs are
     *
     * graph::ANY
     * srcNode::NODE
     * upperBound::INT64
     * bfsPolicy::STRING
     * laneWidth::INT64
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64,
            LogicalTypeID::STRING, LogicalTypeID::INT64};
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
        KU_ASSERT(params.size() == 5);
        auto inputNode = params[1];
        ExpressionUtil::validateExpressionType(*params[2], ExpressionType::LITERAL);
        ExpressionUtil::validateDataType(*params[2], LogicalType::INT64());
        ExpressionUtil::validateExpressionType(*params[3], ExpressionType::LITERAL);
        ExpressionUtil::validateDataType(*params[4], LogicalType::INT64());
        auto upperBound = params[2]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        auto bfsPolicy =
            params[3]->constCast<LiteralExpression>().getValue().getValue<std::string>();
        auto laneWidth = params[4]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        if ((laneWidth % 8) || (laneWidth > 64)) {
            throw BinderException("Only lane width of 8 / 16 / 32 / 64 is supported currently");
        }
        bindData = std::make_unique<ParallelMSBFSPathBindData>(inputNode, upperBound, bfsPolicy,
            laneWidth);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<ParallelShortestPathLocalState>();
        localState->init(context);
    }

    static uint64_t extendFrontierLane8Func(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel *,
            MSBFSIFEMorsel<uint8_t>*>(shortestPathLocalState->ifeMorsel);
        if (!msbfsIFEMorsel->initializedIFEMorsel) {
            msbfsIFEMorsel->init();
        }
        auto morselSize = graph->isInMemory ? 512LU : 256LU;
        auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        if (!frontierMorsel.hasMoreToOutput()) {
            return 0;
        }
        if (graph->isInMemory) {
            auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
            auto &csr = inMemGraph->getInMemCSR();
            while (frontierMorsel.hasMoreToOutput()) {
                for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        auto csrEntry = csr[offset >> RIGHT_SHIFT];
                        auto posInCSR = offset & OFFSET_DIV;
                        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                            uint8_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                            if (shouldBeActive) {
                                if (!msbfsIFEMorsel->isBFSActive) {
                                    msbfsIFEMorsel->isBFSActive = true;
                                }
                                uint8_t oldVal = msbfsIFEMorsel->next[nbrOffset];
                                uint8_t newVal = oldVal | shouldBeActive;
                                while (!__sync_bool_compare_and_swap_1(
                                    &msbfsIFEMorsel->next[nbrOffset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[nbrOffset];
                                    newVal = oldVal | shouldBeActive;
                                }
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = nbrOffset * 8 + (8 - index);
                                    newVal = msbfsIFEMorsel->currentLevel + 1;
                                    __atomic_store_n(&msbfsIFEMorsel->pathLength[exact1BytePos], newVal, __ATOMIC_RELEASE);
                                    shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index-1));
                                }
                            }
                        }
                    }
                }
                frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
            }
        } else {
            auto &nbrScanState = shortestPathLocalState->nbrScanState;
            while (frontierMorsel.hasMoreToOutput()) {
                for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                        do {
                            graph->getFwdNbrs(nbrScanState.get());
                            auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                            auto nbrNodes = (common::nodeID_t *)nbrScanState->dstNodeIDVector->getData();
                            common::nodeID_t dstNodeID;
                            for (auto j = 0u; j < size; j++) {
                                dstNodeID = nbrNodes[j];
                                uint8_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[dstNodeID.offset];
                                if (shouldBeActive) {
                                    if (!msbfsIFEMorsel->isBFSActive) {
                                        msbfsIFEMorsel->isBFSActive = true;
                                    }
                                    uint8_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    uint8_t newVal = oldVal | shouldBeActive;
                                    while (!__sync_bool_compare_and_swap_1(
                                        &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                        oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                        newVal = oldVal | shouldBeActive;
                                    }
                                    while (shouldBeActive) {
                                        int index = __builtin_ctz(shouldBeActive) + 1;
                                        auto exact1BytePos = dstNodeID.offset * 8 + (8 - index);
                                        newVal = msbfsIFEMorsel->currentLevel + 1;
                                        __atomic_store_n(&msbfsIFEMorsel->pathLength[exact1BytePos], newVal, __ATOMIC_RELEASE);
                                        shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index- 1));
                                    }
                                }
                            }
                        } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                    }
                }
                frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
            }
        }
        return 0u;
    }

    static uint64_t extendFrontierLane16Func(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel *,
            MSBFSIFEMorsel<uint16_t>*>(shortestPathLocalState->ifeMorsel);
        if (!msbfsIFEMorsel->initializedIFEMorsel) {
            msbfsIFEMorsel->init();
        }
        auto morselSize = graph->isInMemory ? 512LU : 256LU;
        auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        if (!frontierMorsel.hasMoreToOutput()) {
            return 0;
        }
        if (graph->isInMemory) {
            auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
            auto &csr = inMemGraph->getInMemCSR();
            while (frontierMorsel.hasMoreToOutput()) {
                for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        auto csrEntry = csr[offset >> RIGHT_SHIFT];
                        auto posInCSR = offset & OFFSET_DIV;
                        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                            uint16_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                            if (shouldBeActive) {
                                if (!msbfsIFEMorsel->isBFSActive) {
                                    msbfsIFEMorsel->isBFSActive = true;
                                }
                                uint16_t oldVal = msbfsIFEMorsel->next[nbrOffset];
                                uint16_t newVal = oldVal | shouldBeActive;
                                while (!__sync_bool_compare_and_swap_2(
                                    &msbfsIFEMorsel->next[nbrOffset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[nbrOffset];
                                    newVal = oldVal | shouldBeActive;
                                }
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = nbrOffset * 16 + (16 - index);
                                    newVal = msbfsIFEMorsel->currentLevel + 1;
                                    __atomic_store_n(&msbfsIFEMorsel->pathLength[exact1BytePos], newVal, __ATOMIC_RELEASE);
                                    shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index-1));
                                }
                            }
                        }
                    }
                }
                frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
            }
        } else {
            auto &nbrScanState = shortestPathLocalState->nbrScanState;
            while (frontierMorsel.hasMoreToOutput()) {
                for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                        do {
                            graph->getFwdNbrs(nbrScanState.get());
                            auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                            auto nbrNodes = (common::nodeID_t *)nbrScanState->dstNodeIDVector->getData();
                            common::nodeID_t dstNodeID;
                            for (auto j = 0u; j < size; j++) {
                                dstNodeID = nbrNodes[j];
                                uint16_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[dstNodeID.offset];
                                if (shouldBeActive) {
                                    if (!msbfsIFEMorsel->isBFSActive) {
                                        msbfsIFEMorsel->isBFSActive = true;
                                    }
                                    uint16_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    uint16_t newVal = oldVal | shouldBeActive;
                                    while (!__sync_bool_compare_and_swap_2(
                                        &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                        oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                        newVal = oldVal | shouldBeActive;
                                    }
                                    while (shouldBeActive) {
                                        int index = __builtin_ctz(shouldBeActive) + 1;
                                        auto exact1BytePos = dstNodeID.offset * 16 + (16 - index);
                                        newVal = msbfsIFEMorsel->currentLevel + 1;
                                        __atomic_store_n(&msbfsIFEMorsel->pathLength[exact1BytePos], newVal, __ATOMIC_RELEASE);
                                        shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index-1));
                                    }
                                }
                            }
                        } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                    }
                }
                frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
            }
        }
        return 0u;
    }

    static uint64_t extendFrontierLane32Func(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel *,
            MSBFSIFEMorsel<uint32_t>*>(shortestPathLocalState->ifeMorsel);
        if (!msbfsIFEMorsel->initializedIFEMorsel) {
            msbfsIFEMorsel->init();
        }
        auto morselSize = graph->isInMemory ? 512LU : 256LU;
        auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        if (!frontierMorsel.hasMoreToOutput()) {
            return 0;
        }
        if (graph->isInMemory) {
            auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
            auto &csr = inMemGraph->getInMemCSR();
            while (frontierMorsel.hasMoreToOutput()) {
                for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        auto csrEntry = csr[offset >> RIGHT_SHIFT];
                        auto posInCSR = offset & OFFSET_DIV;
                        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                            uint32_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                            if (shouldBeActive) {
                                if (!msbfsIFEMorsel->isBFSActive) {
                                    msbfsIFEMorsel->isBFSActive = true;
                                }
                                uint32_t oldVal = msbfsIFEMorsel->next[nbrOffset];
                                uint32_t newVal = oldVal | shouldBeActive;
                                while (!__sync_bool_compare_and_swap_4(
                                    &msbfsIFEMorsel->next[nbrOffset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[nbrOffset];
                                    newVal = oldVal | shouldBeActive;
                                }
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = nbrOffset * 32 + (32 - index);
                                    newVal = msbfsIFEMorsel->currentLevel + 1;
                                    __atomic_store_n(&msbfsIFEMorsel->pathLength[exact1BytePos], newVal, __ATOMIC_RELEASE);
                                    shouldBeActive = shouldBeActive ^ ((uint32_t)1 << (index-1));
                                }
                            }
                        }
                    }
                }
                frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
            }
        } else {
            auto &nbrScanState = shortestPathLocalState->nbrScanState;
            while (frontierMorsel.hasMoreToOutput()) {
                for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                        do {
                            graph->getFwdNbrs(nbrScanState.get());
                            auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                            auto nbrNodes = (common::nodeID_t *)nbrScanState->dstNodeIDVector->getData();
                            common::nodeID_t dstNodeID;
                            for (auto j = 0u; j < size; j++) {
                                dstNodeID = nbrNodes[j];
                                uint32_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[dstNodeID.offset];
                                if (shouldBeActive) {
                                    if (!msbfsIFEMorsel->isBFSActive) {
                                        msbfsIFEMorsel->isBFSActive = true;
                                    }
                                    uint32_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    uint32_t newVal = oldVal | shouldBeActive;
                                    while (!__sync_bool_compare_and_swap_4(
                                        &msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                        oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                        newVal = oldVal | shouldBeActive;
                                    }
                                    while (shouldBeActive) {
                                        int index = __builtin_ctz(shouldBeActive) + 1;
                                        auto exact1BytePos = dstNodeID.offset * 32 + (32 - index);
                                        newVal = msbfsIFEMorsel->currentLevel + 1;
                                        __atomic_store_n(&msbfsIFEMorsel->pathLength[exact1BytePos], newVal, __ATOMIC_RELEASE);
                                        shouldBeActive = shouldBeActive ^ ((uint32_t)1 << (index-1));
                                    }
                                }
                            }
                        } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                    }
                }
                frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
            }
        }
        return 0u;
    }

    static uint64_t extendFrontierLane64Func(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel *,
            MSBFSIFEMorsel<uint64_t>*>(shortestPathLocalState->ifeMorsel);
        if (!msbfsIFEMorsel->initializedIFEMorsel) {
            msbfsIFEMorsel->init();
        }
        auto morselSize = graph->isInMemory ? 512LU : 256LU;
        auto frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
        if (!frontierMorsel.hasMoreToOutput()) {
            return 0;
        }
        if (graph->isInMemory) {
            auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
            auto &csr = inMemGraph->getInMemCSR();
            while (frontierMorsel.hasMoreToOutput()) {
                for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        auto csrEntry = csr[offset >> RIGHT_SHIFT];
                        auto posInCSR = offset & OFFSET_DIV;
                        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                            uint64_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                            if (shouldBeActive) {
                                if (!msbfsIFEMorsel->isBFSActive) {
                                    msbfsIFEMorsel->isBFSActive = true;
                                }
                                uint64_t oldVal = msbfsIFEMorsel->next[nbrOffset];
                                uint64_t newVal = oldVal | shouldBeActive;
                                while (!__sync_bool_compare_and_swap_8(
                                    &msbfsIFEMorsel->next[nbrOffset], oldVal, newVal)) {
                                    oldVal = msbfsIFEMorsel->next[nbrOffset];
                                    newVal = oldVal | shouldBeActive;
                                }
                                while (shouldBeActive) {
                                    int index = __builtin_ctzl(shouldBeActive) + 1;
                                    auto exact1BytePos = nbrOffset * 64 + (64 - index);
                                    newVal = msbfsIFEMorsel->currentLevel + 1;
                                    __atomic_store_n(&msbfsIFEMorsel->pathLength[exact1BytePos], newVal, __ATOMIC_RELEASE);
                                    shouldBeActive = shouldBeActive ^ ((uint64_t)1 << (index-1));
                                }
                            }
                        }
                    }
                }
                frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
            }
        } else {
            auto &nbrScanState = shortestPathLocalState->nbrScanState;
            while (frontierMorsel.hasMoreToOutput()) {
                for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                        do {
                            graph->getFwdNbrs(nbrScanState.get());
                            auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                            auto nbrNodes = (common::nodeID_t *)nbrScanState->dstNodeIDVector->getData();
                            common::nodeID_t dstNodeID;
                            for (auto j = 0u; j < size; j++) {
                                dstNodeID = nbrNodes[j];
                                uint64_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[dstNodeID.offset];
                                if (shouldBeActive) {
                                    if (!msbfsIFEMorsel->isBFSActive) {
                                        msbfsIFEMorsel->isBFSActive = true;
                                    }
                                    uint64_t oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                    uint64_t newVal = oldVal | shouldBeActive;
                                    while (!__sync_bool_compare_and_swap_8
                                        (&msbfsIFEMorsel->next[dstNodeID.offset], oldVal, newVal)) {
                                        oldVal = msbfsIFEMorsel->next[dstNodeID.offset];
                                        newVal = oldVal | shouldBeActive;
                                    }
                                    while (shouldBeActive) {
                                        int index = __builtin_ctzl(shouldBeActive) + 1;
                                        auto exact1BytePos = dstNodeID.offset * 64 + (64 - index);
                                        newVal = msbfsIFEMorsel->currentLevel + 1;
                                        __atomic_store_n(&msbfsIFEMorsel->pathLength[exact1BytePos], newVal, __ATOMIC_RELEASE);
                                        shouldBeActive = shouldBeActive ^ ((uint64_t)1 << (index-1));
                                    }
                                }
                            }
                        } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                    }
                }
                frontierMorsel = msbfsIFEMorsel->getMorsel(morselSize);
            }
        }
        return 0u;
    }

    static uint64_t extendFrontierLane8SingleFunc(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel *,
            MSBFSIFEMorsel<uint8_t>*>(shortestPathLocalState->ifeMorsel);
        if (!msbfsIFEMorsel->initializedIFEMorsel) {
            msbfsIFEMorsel->init();
        }
        if (graph->isInMemory) {
            auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
            auto &csr = inMemGraph->getInMemCSR();
            while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
                for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        auto csrEntry = csr[offset >> RIGHT_SHIFT];
                        auto posInCSR = offset & OFFSET_DIV;
                        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                            uint8_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[nbrOffset] |= shouldBeActive;
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = nbrOffset * 8 + (8 - index);
                                    msbfsIFEMorsel->pathLength[exact1BytePos] = msbfsIFEMorsel->currentLevel + 1;
                                    shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index-1));
                                }
                            }
                        }
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        } else {
            auto &nbrScanState = shortestPathLocalState->nbrScanState;
            while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
                for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                        do {
                            graph->getFwdNbrs(nbrScanState.get());
                            auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                            auto nbrNodes = (common::nodeID_t *)nbrScanState->dstNodeIDVector->getData();
                            common::nodeID_t dstNodeID;
                            for (auto j = 0u; j < size; j++) {
                                dstNodeID = nbrNodes[j];
                                uint8_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[dstNodeID.offset];
                                if (shouldBeActive) {
                                    msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                    while (shouldBeActive) {
                                        int index = __builtin_ctz(shouldBeActive) + 1;
                                        auto exact1BytePos = dstNodeID.offset * 8 + (8 - index);
                                        msbfsIFEMorsel->pathLength[exact1BytePos] =
                                            msbfsIFEMorsel->currentLevel + 1;
                                        shouldBeActive = shouldBeActive ^ ((uint8_t)1 << (index-1));
                                    }
                                }
                            }
                        } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        }
        return 0u;
    }

    static uint64_t extendFrontierLane16SingleFunc(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel *,
            MSBFSIFEMorsel<uint16_t>*>(shortestPathLocalState->ifeMorsel);
        if (!msbfsIFEMorsel->initializedIFEMorsel) {
            msbfsIFEMorsel->init();
        }
        if (graph->isInMemory) {
            auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
            auto &csr = inMemGraph->getInMemCSR();
            while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
                for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        auto csrEntry = csr[offset >> RIGHT_SHIFT];
                        auto posInCSR = offset & OFFSET_DIV;
                        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                            uint16_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[nbrOffset] |= shouldBeActive;
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = nbrOffset * 16 + (16 - index);
                                    msbfsIFEMorsel->pathLength[exact1BytePos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index-1));
                                }
                            }
                        }
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        } else {
            auto &nbrScanState = shortestPathLocalState->nbrScanState;
            while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
                for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                        do {
                            graph->getFwdNbrs(nbrScanState.get());
                            auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                            auto nbrNodes = (common::nodeID_t *)nbrScanState->dstNodeIDVector->getData();
                            common::nodeID_t dstNodeID;
                            for (auto j = 0u; j < size; j++) {
                                dstNodeID = nbrNodes[j];
                                uint16_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[dstNodeID.offset];
                                if (shouldBeActive) {
                                    msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                    while (shouldBeActive) {
                                        int index = __builtin_ctz(shouldBeActive) + 1;
                                        auto exact1BytePos = dstNodeID.offset * 16 + (16 - index);
                                        msbfsIFEMorsel->pathLength[exact1BytePos] =
                                            msbfsIFEMorsel->currentLevel + 1;
                                        shouldBeActive = shouldBeActive ^ ((uint16_t)1 << (index-1));
                                    }
                                }
                            }
                        } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        }
        return 0u;
    }

    static uint64_t extendFrontierLane32SingleFunc(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel *,
            MSBFSIFEMorsel<uint32_t>*>(shortestPathLocalState->ifeMorsel);
        if (!msbfsIFEMorsel->initializedIFEMorsel) {
            msbfsIFEMorsel->init();
        }
        if (graph->isInMemory) {
            auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
            auto &csr = inMemGraph->getInMemCSR();
            while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
                for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        auto csrEntry = csr[offset >> RIGHT_SHIFT];
                        auto posInCSR = offset & OFFSET_DIV;
                        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                            uint32_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[nbrOffset] |= shouldBeActive;
                                while (shouldBeActive) {
                                    int index = __builtin_ctz(shouldBeActive) + 1;
                                    auto exact1BytePos = nbrOffset * 32 + (32 - index);
                                    msbfsIFEMorsel->pathLength[exact1BytePos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    shouldBeActive = shouldBeActive ^ ((uint32_t)1 << (index-1));
                                }
                            }
                        }
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        } else {
            auto &nbrScanState = shortestPathLocalState->nbrScanState;
            while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
                for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                        do {
                            graph->getFwdNbrs(nbrScanState.get());
                            auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                            auto nbrNodes = (common::nodeID_t *)nbrScanState->dstNodeIDVector->getData();
                            common::nodeID_t dstNodeID;
                            for (auto j = 0u; j < size; j++) {
                                dstNodeID = nbrNodes[j];
                                uint32_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[dstNodeID.offset];
                                if (shouldBeActive) {
                                    msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                    while (shouldBeActive) {
                                        int index = __builtin_ctz(shouldBeActive) + 1;
                                        auto exact1BytePos = dstNodeID.offset * 32 + (32 - index);
                                        msbfsIFEMorsel->pathLength[exact1BytePos] =
                                            msbfsIFEMorsel->currentLevel + 1;
                                        shouldBeActive = shouldBeActive ^ ((uint32_t)1 << (index-1));
                                    }
                                }
                            }
                        } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        }
        return 0u;
    }

    static uint64_t extendFrontierLane64SingleFunc(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto msbfsIFEMorsel = common::ku_dynamic_cast<IFEMorsel *,
            MSBFSIFEMorsel<uint64_t>*>(shortestPathLocalState->ifeMorsel);
        if (!msbfsIFEMorsel->initializedIFEMorsel) {
            msbfsIFEMorsel->init();
        }
        if (graph->isInMemory) {
            auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph.get());
            auto &csr = inMemGraph->getInMemCSR();
            while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
                for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        auto csrEntry = csr[offset >> RIGHT_SHIFT];
                        auto posInCSR = offset & OFFSET_DIV;
                        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1]; nbrIdx++) {
                            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
                            uint64_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[nbrOffset];
                            if (shouldBeActive) {
                                msbfsIFEMorsel->next[nbrOffset] |= shouldBeActive;
                                while (shouldBeActive) {
                                    int index = __builtin_ctzl(shouldBeActive) + 1;
                                    auto exact1BytePos = nbrOffset * 64 + (64 - index);
                                    msbfsIFEMorsel->pathLength[exact1BytePos] =
                                        msbfsIFEMorsel->currentLevel + 1;
                                    shouldBeActive = shouldBeActive ^ ((uint64_t)1 << (index-1));
                                }
                            }
                        }
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        } else {
            auto &nbrScanState = shortestPathLocalState->nbrScanState;
            while (!msbfsIFEMorsel->isBFSCompleteNoLock()) {
                for (auto offset = 0u; offset <= msbfsIFEMorsel->maxOffset; offset++) {
                    if (msbfsIFEMorsel->current[offset]) {
                        graph->initializeStateFwdNbrs(offset, nbrScanState.get());
                        do {
                            graph->getFwdNbrs(nbrScanState.get());
                            auto size = nbrScanState->dstNodeIDVector->state->getSelVector().getSelSize();
                            auto nbrNodes = (common::nodeID_t *)nbrScanState->dstNodeIDVector->getData();
                            common::nodeID_t dstNodeID;
                            for (auto j = 0u; j < size; j++) {
                                dstNodeID = nbrNodes[j];
                                uint64_t shouldBeActive = msbfsIFEMorsel->current[offset] & ~msbfsIFEMorsel->seen[dstNodeID.offset];
                                if (shouldBeActive) {
                                    msbfsIFEMorsel->next[dstNodeID.offset] |= shouldBeActive;
                                    while (shouldBeActive) {
                                        int index = __builtin_ctzl(shouldBeActive) + 1;
                                        auto exact1BytePos = dstNodeID.offset * 64 + (64 - index);
                                        msbfsIFEMorsel->pathLength[exact1BytePos] =
                                            msbfsIFEMorsel->currentLevel + 1;
                                        shouldBeActive = shouldBeActive ^ ((uint64_t)1 << (index-1));
                                    }
                                }
                            }
                        } while (graph->hasMoreFwdNbrs(nbrScanState.get()));
                    }
                }
                msbfsIFEMorsel->initializeNextFrontierNoLock();
            }
        }
        return 0u;
    }

    static uint64_t shortestPathOutputLane8Func(GDSCallSharedState *sharedState,
        GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = (MSBFSIFEMorsel<uint8_t>*)shortestPathLocalState->ifeMorsel;
        auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto tableID = sharedState->graph->getNodeTableID();
        auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
        auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
        auto& pathLengthVector = shortestPathLocalState->lengthVector;
        srcNodeVector->setValue<nodeID_t>(0, common::nodeID_t{
                                                 ifeMorsel->srcOffsets[ifeMorsel->currentDstLane],
                                                 tableID});
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            auto exactPathLengthPos = offset * 8 + (7 - ifeMorsel->currentDstLane);
            uint64_t pathLength = ifeMorsel->pathLength[exactPathLengthPos];
            if (pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, tableID});
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return UINT64_MAX;
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos;
    }

    static uint64_t shortestPathOutputLane16Func(GDSCallSharedState *sharedState,
        GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = (MSBFSIFEMorsel<uint16_t>*)shortestPathLocalState->ifeMorsel;
        auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto tableID = sharedState->graph->getNodeTableID();
        auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
        auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
        auto& pathLengthVector = shortestPathLocalState->lengthVector;
        srcNodeVector->setValue<nodeID_t>(0, common::nodeID_t{
                                                 ifeMorsel->srcOffsets[ifeMorsel->currentDstLane],
                                                 tableID});
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            auto exactPathLengthPos = offset * 16 + (15 - ifeMorsel->currentDstLane);
            uint64_t pathLength = ifeMorsel->pathLength[exactPathLengthPos];
            if (pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, tableID});
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return UINT64_MAX;
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos;
    }

    static uint64_t shortestPathOutputLane32Func(GDSCallSharedState *sharedState,
        GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = (MSBFSIFEMorsel<uint32_t>*)shortestPathLocalState->ifeMorsel;
        auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto tableID = sharedState->graph->getNodeTableID();
        auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
        auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
        auto& pathLengthVector = shortestPathLocalState->lengthVector;
        srcNodeVector->setValue<nodeID_t>(0, common::nodeID_t{
                                                 ifeMorsel->srcOffsets[ifeMorsel->currentDstLane],
                                                 tableID});
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            auto exactPathLengthPos = offset * 32 + (31 - ifeMorsel->currentDstLane);
            uint64_t pathLength = ifeMorsel->pathLength[exactPathLengthPos];
            if (pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, tableID});
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return UINT64_MAX;
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos;
    }

    static uint64_t shortestPathOutputLane64Func(GDSCallSharedState *sharedState,
        GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = (MSBFSIFEMorsel<uint64_t>*)shortestPathLocalState->ifeMorsel;
        auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto tableID = sharedState->graph->getNodeTableID();
        auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
        auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
        auto& pathLengthVector = shortestPathLocalState->lengthVector;
        srcNodeVector->setValue<nodeID_t>(0, common::nodeID_t{
                                                 ifeMorsel->srcOffsets[ifeMorsel->currentDstLane],
                                                 tableID});
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            auto exactPathLengthPos = offset * 64 + (63 - ifeMorsel->currentDstLane);
            uint64_t pathLength = ifeMorsel->pathLength[exactPathLengthPos];
            if (pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, nodeID_t{offset, tableID});
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return UINT64_MAX;
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos;
    }

    static uint64_t shortestPathOutputLane8FuncSingle(GDSCallSharedState *sharedState,
        GDSLocalState* localState) {

        return 0;
    }

    static uint64_t shortestPathOutputLane16FuncSingle(GDSCallSharedState *sharedState,
        GDSLocalState* localState) {
        return 0;
    }

    static uint64_t shortestPathOutputLane32FuncSingle(GDSCallSharedState *sharedState,
        GDSLocalState* localState) {
        return 0;
    }

    static uint64_t shortestPathOutputLane64FuncSingle(GDSCallSharedState *sharedState,
        GDSLocalState* localState) {
        return 0;
    }

    void executenT1SPolicy(processor::ExecutionContext *executionContext) {
        auto maxThreads = executionContext->clientContext->getClientConfig()->numThreads;
        auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
        auto laneWidth = extraData->laneWidth;
        auto numNodes = sharedState->graph->getNumNodes();
        auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
        if (laneWidth == 8) {
            auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint8_t>>(
                extraData->upperBound,1, numNodes - 1);
            auto count = 0;
            for (auto offset = 0u; offset < numNodes; offset++) {
                if (!inputMask->isMasked(offset)) {
                    continue;
                }
                ifeMorsel->srcOffsets.push_back(offset);
                count++;
                if (count == laneWidth) {
                    auto duration = std::chrono::system_clock::now().time_since_epoch();
                    auto millis =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    // just pass a placeholder offset, it is not used for ms bfs ife morsel
                    ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                    while (!ifeMorsel->isBFSCompleteNoLock()) {
                        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                        gdsLocalState->ifeMorsel = ifeMorsel.get();
                        // Set BFS active as false, when a thread encounters a new neighbour
                        // it will set this variable as true. Else it remains false indicating
                        // next Frontier is empty.
                        ifeMorsel->isBFSActive = false;
                        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane8Func, maxThreads};
                        parallelUtils->submitParallelTaskAndWait(job);
                        auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                        printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                            millis1 - millis);
                        ifeMorsel->initializeNextFrontierNoLock();
                    }
                    duration = std::chrono::system_clock::now().time_since_epoch();
                    millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    for (auto i = 0u; i < ifeMorsel->srcOffsets.size(); i++) {
                        ifeMorsel->currentDstLane = i;
                        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                        gdsLocalState->ifeMorsel = ifeMorsel.get();
                        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                            shortestPathOutputLane8Func, maxThreads};
                        parallelUtils->submitParallelTaskAndWait(job);
                        ifeMorsel->nextDstScanStartIdx.store(0u, std::memory_order_release);
                    }
                    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("output writing completed in %lu ms\n", millis1 - millis);
                    printf("sources: [");
                    for (auto &offset_ : ifeMorsel->srcOffsets) {
                        printf("%lu, ", offset_);
                    }
                    printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);
                    ifeMorsel->srcOffsets.clear();
                    count = 0;
                }
            }
            // There are some BFS sources still remaining, full 8-lane not utilized.
            // But the remaining sources need to be completed.
            if (count > 0) {
                auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                while (!ifeMorsel->isBFSCompleteNoLock()) {
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    // Set BFS active as false, when a thread encounters a new neighbour
                    // it will set this variable as true. Else it remains false indicating
                    // next Frontier is empty.
                    ifeMorsel->isBFSActive = false;
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane8Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                        millis1 - millis);
                    ifeMorsel->initializeNextFrontierNoLock();
                    millis = millis1;
                }
                duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                for (auto i = 0u; i < ifeMorsel->srcOffsets.size(); i++) {
                    ifeMorsel->currentDstLane = i;
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                        shortestPathOutputLane8Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    ifeMorsel->nextDstScanStartIdx.store(0u, std::memory_order_release);
                }
                auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("output writing completed in %lu ms\n", millis1 - millis);
                printf("sources: [");
                for (auto &offset_ : ifeMorsel->srcOffsets) {
                    printf("%lu, ", offset_);
                }
                printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);
                ifeMorsel->srcOffsets.clear();
            }
        } else if (laneWidth == 16) {
            auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint16_t>>(
                extraData->upperBound,1, numNodes - 1);
            auto count = 0;
            for (auto offset = 0u; offset < numNodes; offset++) {
                if (!inputMask->isMasked(offset)) {
                    continue;
                }
                ifeMorsel->srcOffsets.push_back(offset);
                count++;
                if (count == laneWidth) {
                    auto duration = std::chrono::system_clock::now().time_since_epoch();
                    auto millis =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    // just pass a placeholder offset, it is not used for ms bfs ife morsel
                    ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                    while (!ifeMorsel->isBFSCompleteNoLock()) {
                        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                        gdsLocalState->ifeMorsel = ifeMorsel.get();
                        // Set BFS active as false, when a thread encounters a new neighbour
                        // it will set this variable as true. Else it remains false indicating
                        // next Frontier is empty.
                        ifeMorsel->isBFSActive = false;
                        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                            sharedState, extendFrontierLane16Func, maxThreads};
                        parallelUtils->submitParallelTaskAndWait(job);
                        auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                        printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                            millis1 - millis);
                        ifeMorsel->initializeNextFrontierNoLock();
                    }
                    duration = std::chrono::system_clock::now().time_since_epoch();
                    millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    for (auto i = 0u; i < ifeMorsel->srcOffsets.size(); i++) {
                        ifeMorsel->currentDstLane = i;
                        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                        gdsLocalState->ifeMorsel = ifeMorsel.get();
                        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                            shortestPathOutputLane16Func, maxThreads};
                        parallelUtils->submitParallelTaskAndWait(job);
                        ifeMorsel->nextDstScanStartIdx.store(0u, std::memory_order_release);
                    }
                    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("output writing completed in %lu ms\n", millis1 - millis);
                    printf("sources: [");
                    for (auto &offset_ : ifeMorsel->srcOffsets) {
                        printf("%lu, ", offset_);
                    }
                    printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);
                    ifeMorsel->srcOffsets.clear();
                    count = 0;
                }
            }
            // There are some BFS sources still remaining, full 8-lane not utilized.
            // But the remaining sources need to be completed.
            if (count > 0) {
                auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                while (!ifeMorsel->isBFSCompleteNoLock()) {
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    // Set BFS active as false, when a thread encounters a new neighbour
                    // it will set this variable as true. Else it remains false indicating
                    // next Frontier is empty.
                    ifeMorsel->isBFSActive = false;
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane16Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                        millis1 - millis);
                    ifeMorsel->initializeNextFrontierNoLock();
                    millis = millis1;
                }
                duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                for (auto i = 0u; i < ifeMorsel->srcOffsets.size(); i++) {
                    ifeMorsel->currentDstLane = i;
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                        shortestPathOutputLane16Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    ifeMorsel->nextDstScanStartIdx.store(0u, std::memory_order_release);
                }
                auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("output writing completed in %lu ms\n", millis1 - millis);
                printf("sources: [");
                for (auto &offset_ : ifeMorsel->srcOffsets) {
                    printf("%lu, ", offset_);
                }
                printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);
                ifeMorsel->srcOffsets.clear();
            }
        } else if (laneWidth == 32) {
            auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint32_t>>(
                extraData->upperBound,1, numNodes - 1);
            auto count = 0;
            for (auto offset = 0u; offset < numNodes; offset++) {
                if (!inputMask->isMasked(offset)) {
                    continue;
                }
                ifeMorsel->srcOffsets.push_back(offset);
                count++;
                if (count == laneWidth) {
                    auto duration = std::chrono::system_clock::now().time_since_epoch();
                    auto millis =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    // just pass a placeholder offset, it is not used for ms bfs ife morsel
                    ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                    while (!ifeMorsel->isBFSCompleteNoLock()) {
                        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                        gdsLocalState->ifeMorsel = ifeMorsel.get();
                        // Set BFS active as false, when a thread encounters a new neighbour
                        // it will set this variable as true. Else it remains false indicating
                        // next Frontier is empty.
                        ifeMorsel->isBFSActive = false;
                        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                            sharedState, extendFrontierLane32Func, maxThreads};
                        parallelUtils->submitParallelTaskAndWait(job);
                        auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                        printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                            millis1 - millis);
                        ifeMorsel->initializeNextFrontierNoLock();
                    }
                    duration = std::chrono::system_clock::now().time_since_epoch();
                    millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    for (auto i = 0u; i < ifeMorsel->srcOffsets.size(); i++) {
                        ifeMorsel->currentDstLane = i;
                        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                        gdsLocalState->ifeMorsel = ifeMorsel.get();
                        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                            shortestPathOutputLane32Func, maxThreads};
                        parallelUtils->submitParallelTaskAndWait(job);
                        ifeMorsel->nextDstScanStartIdx.store(0u, std::memory_order_release);
                    }
                    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("output writing completed in %lu ms\n", millis1 - millis);
                    printf("sources: [");
                    for (auto &offset_ : ifeMorsel->srcOffsets) {
                        printf("%lu, ", offset_);
                    }
                    printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);
                    ifeMorsel->srcOffsets.clear();
                    count = 0;
                }
            }
            // There are some BFS sources still remaining, full 8-lane not utilized.
            // But the remaining sources need to be completed.
            if (count > 0) {
                auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                while (!ifeMorsel->isBFSCompleteNoLock()) {
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    // Set BFS active as false, when a thread encounters a new neighbour
                    // it will set this variable as true. Else it remains false indicating
                    // next Frontier is empty.
                    ifeMorsel->isBFSActive = false;
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane32Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                        millis1 - millis);
                    ifeMorsel->initializeNextFrontierNoLock();
                    millis = millis1;
                }
                duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                for (auto i = 0u; i < ifeMorsel->srcOffsets.size(); i++) {
                    ifeMorsel->currentDstLane = i;
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                        shortestPathOutputLane32Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    ifeMorsel->nextDstScanStartIdx.store(0u, std::memory_order_release);
                }
                auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("output writing completed in %lu ms\n", millis1 - millis);
                printf("sources: [");
                for (auto &offset_ : ifeMorsel->srcOffsets) {
                    printf("%lu, ", offset_);
                }
                printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);
                ifeMorsel->srcOffsets.clear();
            }
        } else {
            auto ifeMorsel = std::make_unique<MSBFSIFEMorsel<uint64_t>>(
                extraData->upperBound,1, numNodes - 1);
            auto count = 0;
            for (auto offset = 0u; offset < numNodes; offset++) {
                if (!inputMask->isMasked(offset)) {
                    continue;
                }
                ifeMorsel->srcOffsets.push_back(offset);
                count++;
                if (count == laneWidth) {
                    auto duration = std::chrono::system_clock::now().time_since_epoch();
                    auto millis =
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    // just pass a placeholder offset, it is not used for ms bfs ife morsel
                    ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                    while (!ifeMorsel->isBFSCompleteNoLock()) {
                        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                        gdsLocalState->ifeMorsel = ifeMorsel.get();
                        // Set BFS active as false, when a thread encounters a new neighbour
                        // it will set this variable as true. Else it remains false indicating
                        // next Frontier is empty.
                        ifeMorsel->isBFSActive = false;
                        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                            sharedState, extendFrontierLane64Func, maxThreads};
                        parallelUtils->submitParallelTaskAndWait(job);
                        auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                        printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                            millis1 - millis);
                        ifeMorsel->initializeNextFrontierNoLock();
                    }
                    duration = std::chrono::system_clock::now().time_since_epoch();
                    millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                    for (auto i = 0u; i < ifeMorsel->srcOffsets.size(); i++) {
                        ifeMorsel->currentDstLane = i;
                        auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                        gdsLocalState->ifeMorsel = ifeMorsel.get();
                        auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                            shortestPathOutputLane64Func, maxThreads};
                        parallelUtils->submitParallelTaskAndWait(job);
                        ifeMorsel->nextDstScanStartIdx.store(0u, std::memory_order_release);
                    }
                    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("output writing completed in %lu ms\n", millis1 - millis);
                    printf("sources: [");
                    for (auto &offset_ : ifeMorsel->srcOffsets) {
                        printf("%lu, ", offset_);
                    }
                    printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);
                    ifeMorsel->srcOffsets.clear();
                    count = 0;
                }
            }
            // There are some BFS sources still remaining, full 8-lane not utilized.
            // But the remaining sources need to be completed.
            if (count > 0) {
                auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                // just pass a placeholder offset, it is not used for ms bfs ife morsel
                ifeMorsel->resetNoLock(common::INVALID_OFFSET);
                while (!ifeMorsel->isBFSCompleteNoLock()) {
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    // Set BFS active as false, when a thread encounters a new neighbour
                    // it will set this variable as true. Else it remains false indicating
                    // next Frontier is empty.
                    ifeMorsel->isBFSActive = false;
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierLane64Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                    printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                        millis1 - millis);
                    ifeMorsel->initializeNextFrontierNoLock();
                    millis = millis1;
                }
                duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                for (auto i = 0u; i < ifeMorsel->srcOffsets.size(); i++) {
                    ifeMorsel->currentDstLane = i;
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorsel.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                        shortestPathOutputLane64Func, maxThreads};
                    parallelUtils->submitParallelTaskAndWait(job);
                    ifeMorsel->nextDstScanStartIdx.store(0u, std::memory_order_release);
                }
                auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("output writing completed in %lu ms\n", millis1 - millis);
                printf("sources: [");
                for (auto &offset_ : ifeMorsel->srcOffsets) {
                    printf("%lu, ", offset_);
                }
                printf("] completed in %lu ms\n", millis1 - ifeMorsel->startTime);
                ifeMorsel->srcOffsets.clear();
            }
        }
    }

    void execute1T1SPolicy(processor::ExecutionContext *executionContext) {

    }

    void executenTkSPolicy(processor::ExecutionContext *executionContext) {

    }

    void exec(processor::ExecutionContext *executionContext) override {
        auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
        auto bfsPolicy = extraData->bfsPolicy;
        if (bfsPolicy == "nT1S") {
            return executenT1SPolicy(executionContext);
        } else if (bfsPolicy == "1T1S") {
            return execute1T1SPolicy(executionContext);
        } else if (bfsPolicy == "nTkS") {
            return executenTkSPolicy(executionContext);
        } else {
            throw RuntimeException("Unidentified BFS Policy passed,"
                                   " supported policies: nT1S, 1T1S, nTkS\n");
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<ParallelMSBFSLengths>(*this);
    }
};

function_set ParallelMSBFSLengthsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name,
        std::make_unique<ParallelMSBFSLengths>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
