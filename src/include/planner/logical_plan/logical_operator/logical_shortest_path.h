#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalShortestPath : public LogicalOperator {

public:
    LogicalShortestPath(std::shared_ptr<NodeExpression> sourceNode,
        std::shared_ptr<NodeExpression> destNode, std::shared_ptr<RelExpression> rel,
        std::shared_ptr<Expression> relProperty, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SHORTEST_PATH, std::move(child)},
          sourceNode{std::move(sourceNode)}, destNode{std::move(destNode)}, rel{std::move(rel)},
          relProperty{move(relProperty)} {}

    void computeSchema() override { copyChildSchema(0); }

    std::shared_ptr<NodeExpression> getSrcNodeExpression() { return sourceNode; }

    std::shared_ptr<NodeExpression> getDestNodeExpression() { return destNode; }

    std::shared_ptr<RelExpression> getRelExpression() { return rel; }

    std::shared_ptr<Expression> getRelPropertyExpression() { return relProperty; }

    std::string getExpressionsForPrinting() const override {
        return sourceNode->getRawName() + ("->") + destNode->getRawName();
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalShortestPath>(
            sourceNode, destNode, rel, relProperty, children[0]->copy());
    }

private:
    std::shared_ptr<NodeExpression> sourceNode;
    std::shared_ptr<NodeExpression> destNode;
    std::shared_ptr<RelExpression> rel;
    std::shared_ptr<Expression> relProperty;
};

} // namespace planner
} // namespace kuzu