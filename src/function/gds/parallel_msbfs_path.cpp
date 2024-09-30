#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/types/types.h"
#include "function/gds/1T1S_parallel_msbfs_path.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/nT1S_parallel_msbfs_path.h"
#include "function/gds/nTkS_parallel_msbfs_path.h"
#include "function/gds/parallel_msbfs_commons.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"
#include "processor/processor_task.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

class ParallelMSBFSPaths : public GDSAlgorithm {
public:
    ParallelMSBFSPaths() = default;
    ParallelMSBFSPaths(const ParallelMSBFSPaths& other) = default;

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

    bool hasPathOutput() const override {
        return true;
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

    void exec(processor::ExecutionContext* executionContext) override {
        auto extraData = bindData->ptrCast<ParallelMSBFSPathBindData>();
        auto bfsPolicy = extraData->bfsPolicy;
        if (bfsPolicy == "nT1S") {
            auto executenT1SPolicy = std::make_unique<nT1SParallelMSBFSPath>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            executenT1SPolicy->exec();
        } else if (bfsPolicy == "1T1S") {
            auto execute1T1SPolicy = std::make_unique<_1T1SParallelMSBFSPath>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            execute1T1SPolicy->exec();
        } else if (bfsPolicy == "nTkS") {
            auto executenTkSPolicy = std::make_unique<nTkSParallelMSBFSPath>(executionContext,
                sharedState, bindData.get(), parallelUtils.get());
            executenTkSPolicy->exec();
        } else {
            throw RuntimeException("Unidentified BFS Policy passed,"
                                   " supported policies: nT1S, 1T1S, nTkS\n");
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<ParallelMSBFSPaths>(*this);
    }
};

function_set ParallelMSBFSPathsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name,
        std::make_unique<ParallelMSBFSPaths>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
