#include "binder/binder.h"
#include "common/exception/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/ife_morsel.h"
#include "function/gds/parallel_msbfs_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds/msbfs_ife_morsel.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"
#include "function/gds/nT1S_parallel_msbfs_length.h"
#include "function/gds/nTkS_parallel_msbfs_length.h"
#include "function/gds/1T1S_parallel_msbfs_length.h"

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

    static uint64_t extendFrontierLane8SingleFunc(GDSCallSharedState *sharedState, GDSLocalState *localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
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
            common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
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
            common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
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
            common::ku_dynamic_cast<GDSLocalState*, ParallelMSBFSLocalState*>(localState);
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
                                    int index = __builtin_ctzll(shouldBeActive) + 1;
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
                                        int index = __builtin_ctzll(shouldBeActive) + 1;
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

    void exec(processor::ExecutionContext *executionContext) override {
        auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
        auto bfsPolicy = extraData->bfsPolicy;
        if (bfsPolicy == "nT1S") {
            auto executenT1SPolicy = std::make_unique<nT1SParallelMSBFSLength>(
                executionContext, sharedState, bindData.get(), parallelUtils.get());
            executenT1SPolicy->exec();
        } else if (bfsPolicy == "1T1S") {
            auto execute1T1SPolicy = std::make_unique<_1T1SParallelMSBFSLength>(
                executionContext, sharedState, bindData.get(), parallelUtils.get());
            execute1T1SPolicy->exec();
        } else if (bfsPolicy == "nTkS") {
            auto executenTkSPolicy = std::make_unique<nTkSParallelMSBFSLength>(
                executionContext, sharedState, bindData.get(), parallelUtils.get());
            executenTkSPolicy->exec();
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
