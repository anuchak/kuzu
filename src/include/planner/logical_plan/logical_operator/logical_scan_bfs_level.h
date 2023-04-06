#pragma once

#include <utility>

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"

using namespace kuzu::planner;

namespace kuzu {
namespace planner {

using namespace binder;

class LogicalScanBFSLevel : public LogicalOperator {

public:
    LogicalScanBFSLevel(uint8_t lowerBound, uint8_t upperBound,
        std::shared_ptr<NodeExpression>& sourceNodeExpression,
        std::shared_ptr<NodeExpression>& destNodeExpression,
        std::shared_ptr<Expression> pathExpression, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::SCAN_BFS_LEVEL, std::move(child)),
          lowerBound{lowerBound}, upperBound{upperBound},
          sourceNodeExpression{sourceNodeExpression}, destNodeExpression{destNodeExpression},
          pathExpression{std::move(pathExpression)} {}

    inline uint8_t getLowerBound() const { return lowerBound; }

    inline uint8_t getUpperBound() const { return upperBound; }

    inline std::shared_ptr<NodeExpression> getSourceNodeExpression() {
        return sourceNodeExpression;
    }

    inline std::shared_ptr<NodeExpression> getDestNodeExpression() { return destNodeExpression; }

    inline void setNodesToExtendBoundExpr(
        std::shared_ptr<NodeExpression>& nodesToExtendBoundExpr_) {
        nodesToExtendBoundExpr = nodesToExtendBoundExpr_;
    }

    inline std::shared_ptr<NodeExpression> getNodesToExtendBoundExpr() {
        return nodesToExtendBoundExpr;
    }

    inline std::shared_ptr<Expression> getPathExpression() { return pathExpression; }

    inline void setSrcDstPropertiesToScan(expression_vector srcDstPropertiesToScan_) {
        srcDstPropertiesToScan = std::move(srcDstPropertiesToScan_);
    }

    inline expression_vector getSrcDstPropertiesToScan() { return srcDstPropertiesToScan; }

    inline f_group_pos getGroupPosOfSrcNodeToFlatten() {
        return schema->getGroupPos(sourceNodeExpression->getInternalIDProperty()->getUniqueName());
    }

    void computeFactorizedSchema() override;

    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return ""; }

    std::unique_ptr<LogicalOperator> copy() override {
        auto logicalScanBFSLevel = make_unique<LogicalScanBFSLevel>(lowerBound, upperBound,
            sourceNodeExpression, destNodeExpression, pathExpression, children[0]->copy());
        logicalScanBFSLevel->setSrcDstPropertiesToScan(srcDstPropertiesToScan);
        logicalScanBFSLevel->setNodesToExtendBoundExpr(nodesToExtendBoundExpr);
        return std::move(logicalScanBFSLevel);
    }

private:
    uint8_t lowerBound;
    uint8_t upperBound;
    std::shared_ptr<NodeExpression> sourceNodeExpression;
    std::shared_ptr<NodeExpression> destNodeExpression;
    std::shared_ptr<NodeExpression> nodesToExtendBoundExpr;
    std::shared_ptr<Expression> pathExpression;
    expression_vector srcDstPropertiesToScan;
};
} // namespace planner
} // namespace kuzu
