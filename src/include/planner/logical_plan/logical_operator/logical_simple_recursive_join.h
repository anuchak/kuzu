#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"

using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalSimpleRecursiveJoin : public LogicalOperator {

public:
    LogicalSimpleRecursiveJoin(std::shared_ptr<NodeExpression> nodesToExtendNbrExpr,
        std::shared_ptr<binder::NodeExpression> nodesAfterExtendNbrExpr,
        std::shared_ptr<RelExpression> relExpression, std::shared_ptr<LogicalOperator> child)
        : nodesToExtendNbrExpr{std::move(nodesToExtendNbrExpr)},
          nodesAfterExtendNbrExpr{std::move(nodesAfterExtendNbrExpr)}, relExpression{std::move(
                                                                           relExpression)},
          LogicalOperator(LogicalOperatorType::SIMPLE_RECURSIVE_JOIN, std::move(child)) {}

    std::shared_ptr<NodeExpression> getNodesAfterExtendExpression() {
        return nodesAfterExtendNbrExpr;
    }

    void computeSchema() override;

    std::string getExpressionsForPrinting() const override { return ""; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSimpleRecursiveJoin>(
            nodesToExtendNbrExpr, nodesAfterExtendNbrExpr, relExpression, children[0]->copy());
    }

private:
    std::shared_ptr<NodeExpression> nodesToExtendNbrExpr;
    std::shared_ptr<NodeExpression> nodesAfterExtendNbrExpr;
    std::shared_ptr<RelExpression> relExpression;
};
} // namespace planner
} // namespace kuzu
