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
    LogicalScanBFSLevel(uint8_t lowerBound, uint8_t upperBound,
        NodeExpression* sourceNodeExpression, NodeExpression* destNodeExpression,
        std::shared_ptr<Expression> pathExpression, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::SCAN_BFS_LEVEL, std::move(child)),
          lowerBound{lowerBound}, upperBound{upperBound},
          sourceNodeExpression{sourceNodeExpression}, destNodeExpression{destNodeExpression},
          pathExpression{std::move(pathExpression)} {}

    inline uint8_t getLowerBound() const { return lowerBound; }

    inline uint8_t getUpperBound() const { return upperBound; }

    inline NodeExpression* getSourceNodeExpression() { return sourceNodeExpression; }

    inline NodeExpression* getTmpDestNodeExpression() { return destNodeExpression; }

    inline void setNodesToExtendBoundExpr(
        std::shared_ptr<NodeExpression>& nodesToExtendBoundExpr_) {
        nodesToExtendBoundExpr = nodesToExtendBoundExpr_;
    }

    inline std::shared_ptr<NodeExpression> getNodesToExtendBoundExpr() {
        return nodesToExtendBoundExpr;
    }

    inline void setNodesAfterExtendNbrExpr(
        std::shared_ptr<NodeExpression>& nodesAfterExtendNbrExpr_) {
        nodesAfterExtendNbrExpr = nodesAfterExtendNbrExpr_;
    }

    std::shared_ptr<NodeExpression> getNodesAfterExtendNbrExpr();

    inline std::shared_ptr<Expression> getPathExpression() { return pathExpression; }

    inline void setSrcDstNodePropertiesToScan(expression_vector srcDstNodePropertiesToScan_) {
        srcDstNodePropertiesToScan = std::move(srcDstNodePropertiesToScan_);
    }

    inline expression_vector getSrcDstNodePropertiesToScan() { return srcDstNodePropertiesToScan; }

    void computeFactorizedSchema() override;

    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return ""; }

    std::unique_ptr<LogicalOperator> copy() override {
        auto logicalScanBFSLevel = make_unique<LogicalScanBFSLevel>(lowerBound, upperBound,
            sourceNodeExpression, destNodeExpression, pathExpression, children[0]->copy());
        logicalScanBFSLevel->setSrcDstNodePropertiesToScan(srcDstNodePropertiesToScan);
        logicalScanBFSLevel->setNodesToExtendBoundExpr(nodesToExtendBoundExpr);
        logicalScanBFSLevel->setNodesAfterExtendNbrExpr(nodesAfterExtendNbrExpr);
        return std::move(logicalScanBFSLevel);
    }

private:
    uint8_t lowerBound;
    uint8_t upperBound;
    NodeExpression* sourceNodeExpression;
    NodeExpression* destNodeExpression;
    std::shared_ptr<NodeExpression> nodesToExtendBoundExpr;
    std::shared_ptr<NodeExpression> nodesAfterExtendNbrExpr;
    std::shared_ptr<Expression> pathExpression;
    expression_vector srcDstNodePropertiesToScan;
};
} // namespace planner
} // namespace kuzu
