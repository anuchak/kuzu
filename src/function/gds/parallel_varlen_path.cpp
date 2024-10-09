#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/1T1S_parallel_varlen_path.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/nT1S_parallel_varlen_path.h"
#include "function/gds/nTkS_parallel_varlen_path.h"
#include "function/gds/parallel_var_len_commons.h"
#include "function/gds_function.h"
#include "processor/processor_task.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

class ParallelVarlenPaths : public GDSAlgorithm {
public:
    ParallelVarlenPaths() = default;
    ParallelVarlenPaths(const ParallelVarlenPaths& other) = default;

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
     * path::LIST (INTERNAL_ID)
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        columns.push_back(bindData->nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(binder->createVariable("dst", LogicalType::INTERNAL_ID()));
        columns.push_back(binder->createVariable("length", LogicalType::INT64()));
        columns.push_back(
            binder->createVariable("path", LogicalType::LIST(LogicalType::INTERNAL_ID())));
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
        bindData = std::make_unique<ParallelVarlenBindData>(inputNode, upperBound, bfsPolicy);
    }

    inline bool hasPathOutput() const override { return true; }

    void exec(processor::ExecutionContext* executionContext) override {
        auto extraData = bindData->ptrCast<ParallelVarlenBindData>();
        auto bfsPolicy = extraData->bfsPolicy;
        if (bfsPolicy == "nT1S") {
            auto execnT1SPolicy = std::make_unique<nT1SParallelVarlenPath>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            execnT1SPolicy->exec();
        } else if (bfsPolicy == "1T1S") {
            auto exec1T1SPolicy = std::make_unique<_1T1SParallelVarlenPath>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            exec1T1SPolicy->exec();
        } else if (bfsPolicy == "nTkS") {
            auto execnTkSPolicy = std::make_unique<nTkSParallelVarlenPath>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            execnTkSPolicy->exec();
        } else {
            throw RuntimeException("Unidentified BFS Policy passed,"
                                   " supported policies: nT1S, 1T1S, nTkS\n");
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<ParallelVarlenPaths>(*this);
    }
};

function_set ParallelVarLenPathsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name, std::make_unique<ParallelVarlenPaths>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
