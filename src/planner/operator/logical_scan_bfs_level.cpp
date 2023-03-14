#include "planner/logical_plan/logical_operator/logical_scan_bfs_level.h"

namespace kuzu {
namespace planner {

void LogicalScanBFSLevel::computeSchema() {
    copyChildSchema(0);

    // temp dest node value vector, copied from ftable dest column
    auto destGroup =
        schema->getGroupPos(tmpDestNodeExpression->getInternalIDProperty()->getUniqueName());
    schema->insertToGroupAndScope(pathExpression, destGroup);

    // nodes to extend internal expression for writing at every bfs level to extend
    auto nodesToExtendGroup = schema->createGroup();
    schema->insertToGroupAndScope(
        nodesToExtendBoundExpr->getInternalIDProperty(), nodesToExtendGroup);
    // schema->flattenGroup(nodesToExtendGroup);
}
} // namespace planner
} // namespace kuzu
