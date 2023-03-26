#include "planner/logical_plan/logical_operator/logical_scan_bfs_level.h"

#include "binder/binder.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalScanBFSLevel::computeFactorizedSchema() {
    copyChildSchema(0);
    auto destGroup =
        schema->getGroupPos(destNodeExpression->getInternalIDProperty()->getUniqueName());
    schema->insertToGroupAndScope(pathExpression, destGroup);
    // nodes to extend internal expression for writing at every bfs level to extend
    auto nodesToExtendBoundExpr_ =
        std::make_shared<NodeExpression>(sourceNodeExpression->getUniqueName() + "_",
            sourceNodeExpression->getVariableName() + "_", sourceNodeExpression->getTableIDs());
    auto propertyIDPerTable = std::unordered_map<common::table_id_t, common::property_id_t>();
    for (auto tableID : sourceNodeExpression->getTableIDs()) {
        propertyIDPerTable.insert({tableID, common::INVALID_PROPERTY_ID});
    }
    auto uniqueInternalPropertyName =
        "_" + sourceNodeExpression->getInternalIDProperty()->getUniqueName() + "_";
    auto srcInternalIDExpr = std::make_unique<PropertyExpression>(
        common::DataType(common::INTERNAL_ID), uniqueInternalPropertyName, *sourceNodeExpression,
        std::move(propertyIDPerTable), false);
    nodesToExtendBoundExpr_->setInternalIDProperty(std::move(srcInternalIDExpr));
    setNodesToExtendBoundExpr(nodesToExtendBoundExpr_);
    auto nodesToExtendGroup = schema->createGroup();
    schema->insertToGroupAndScope(
        nodesToExtendBoundExpr_->getInternalIDProperty(), nodesToExtendGroup);
}

void LogicalScanBFSLevel::computeFlatSchema() {
    copyChildSchema(0);
    if (!schema->isExpressionInScope(*pathExpression)) {
        auto destGroup =
            schema->getGroupPos(destNodeExpression->getInternalIDProperty()->getUniqueName());
        schema->insertToGroupAndScope(pathExpression, destGroup);
    }
    if (!schema->isExpressionInScope(*nodesToExtendBoundExpr->getInternalIDProperty())) {
        auto nodesToExtendGroup = schema->createGroup();
        schema->insertToGroupAndScope(
            nodesToExtendBoundExpr->getInternalIDProperty(), nodesToExtendGroup);
    }
}

} // namespace planner
} // namespace kuzu
