#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/1T1S_parallel_sp_length.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/nT1S_parallel_sp_length.h"
#include "function/gds/nTkS_parallel_sp_length.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"

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

    void exec(processor::ExecutionContext* executionContext) override {
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        auto bfsPolicy = extraData->bfsPolicy;
        if (bfsPolicy == "nT1S") {
            auto execnT1SPolicy = std::make_unique<nT1SParallelShortestPath>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            execnT1SPolicy->exec();
        } else if (bfsPolicy == "1T1S") {
            auto exec1T1SPolicy = std::make_unique<_1T1SParallelShortestPath>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            exec1T1SPolicy->exec();
        } else if (bfsPolicy == "nTkS") {
            auto execnTkSPolicy = std::make_unique<nTkSParallelSPLength>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            execnTkSPolicy->exec();
        } else {
            throw RuntimeException("Unidentified BFS Policy passed,"
                                   " supported policies: nT1S, 1T1S, nTkS\n");
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
