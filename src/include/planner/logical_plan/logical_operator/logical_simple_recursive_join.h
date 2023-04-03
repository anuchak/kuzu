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
    LogicalSimpleRecursiveJoin(std::shared_ptr<NodeExpression> sourceNodeExpression,
        std::shared_ptr<NodeExpression> dstNodeExpression,
        std::shared_ptr<Expression> pathExpression, std::shared_ptr<NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode,
        std::shared_ptr<RelExpression> relExpression, std::shared_ptr<LogicalOperator> child)
        : sourceNodeExpression{std::move(sourceNodeExpression)}, dstNodeExpression{std::move(
                                                                     dstNodeExpression)},
          pathExpression{std::move(pathExpression)}, boundNode{std::move(boundNode)},
          nbrNode{std::move(nbrNode)}, relExpression{std::move(relExpression)},
          LogicalOperator(LogicalOperatorType::SIMPLE_RECURSIVE_JOIN, std::move(child)) {}

    std::shared_ptr<NodeExpression> getNbrNodeExpression() { return nbrNode; }

    const std::shared_ptr<NodeExpression>& getSourceNodeExpression() const {
        return sourceNodeExpression;
    }

    const std::shared_ptr<NodeExpression>& getDstNodeExpression() const {
        return dstNodeExpression;
    }
    const std::shared_ptr<Expression>& getPathExpression() const { return pathExpression; }

    uint8_t getLowerBound() { return relExpression->getLowerBound(); }

    uint8_t getUpperBound() { return relExpression->getUpperBound(); }

    void computeFactorizedSchema() override;

    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return ""; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSimpleRecursiveJoin>(sourceNodeExpression, dstNodeExpression,
            pathExpression, boundNode, nbrNode, relExpression, children[0]->copy());
    }

private:
    std::shared_ptr<NodeExpression> sourceNodeExpression;
    std::shared_ptr<NodeExpression> dstNodeExpression;
    std::shared_ptr<Expression> pathExpression;
    std::shared_ptr<NodeExpression> boundNode;
    std::shared_ptr<NodeExpression> nbrNode;
    std::shared_ptr<RelExpression> relExpression;
};
} // namespace planner
} // namespace kuzu
