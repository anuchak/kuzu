#pragma once

#include <utility>

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

using namespace kuzu::planner;

namespace kuzu {
namespace planner {

using namespace binder;

class LogicalScanBFSLevel : public LogicalOperator {

public:
    LogicalScanBFSLevel(std::shared_ptr<NodeExpression> tmpSourceNodeExpression,
        std::shared_ptr<NodeExpression> tmpDestNodeExpression,
        std::shared_ptr<NodeExpression> nodesToExtendBoundExpr,
        std::shared_ptr<NodeExpression> nodesAfterExtendNbrExpr,
        std::shared_ptr<Expression> pathExpression, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::SCAN_BFS_LEVEL, std::move(child)),
          tmpSourceNodeExpression{std::move(tmpSourceNodeExpression)},
          tmpDestNodeExpression{std::move(tmpDestNodeExpression)}, nodesToExtendBoundExpr{std::move(
                                                                       nodesToExtendBoundExpr)},
          nodesAfterExtendNbrExpr{std::move(nodesAfterExtendNbrExpr)}, pathExpression{std::move(
                                                                           pathExpression)} {}

    std::shared_ptr<NodeExpression> getTmpSourceNodeExpression() { return tmpSourceNodeExpression; }

    std::shared_ptr<NodeExpression> getTmpDestNodeExpression() { return tmpDestNodeExpression; }

    inline std::shared_ptr<NodeExpression> getNodesToExtendBoundExpr() {
        return nodesToExtendBoundExpr;
    }

    inline std::shared_ptr<Expression> getPathExpression() { return pathExpression; }

    inline void setSrcDstNodePropertiesToScan(expression_vector srcDstNodePropertiesToScan_) {
        srcDstNodePropertiesToScan = std::move(srcDstNodePropertiesToScan_);
    }

    expression_vector getSrcDstNodePropertiesToScan() { return srcDstNodePropertiesToScan; }

    void computeSchema() override;

    std::string getExpressionsForPrinting() const override { return ""; }

    std::unique_ptr<LogicalOperator> copy() override {
        auto logicalScanBFSLevel = make_unique<LogicalScanBFSLevel>(tmpSourceNodeExpression,
            tmpDestNodeExpression, nodesToExtendBoundExpr, nodesAfterExtendNbrExpr, pathExpression,
            children[0]->copy());
        logicalScanBFSLevel->setSrcDstNodePropertiesToScan(srcDstNodePropertiesToScan);
        return std::move(logicalScanBFSLevel);
    }

private:
    std::shared_ptr<NodeExpression> tmpSourceNodeExpression;
    std::shared_ptr<NodeExpression> tmpDestNodeExpression;
    std::shared_ptr<NodeExpression> nodesToExtendBoundExpr;
    std::shared_ptr<NodeExpression> nodesAfterExtendNbrExpr;
    std::shared_ptr<Expression> pathExpression;
    expression_vector srcDstNodePropertiesToScan;
};
} // namespace planner
} // namespace kuzu