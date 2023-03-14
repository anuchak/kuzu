#include "planner/logical_plan/logical_operator/logical_simple_recursive_join.h"

namespace kuzu {
namespace planner {

/*
 * There are 3 expressions being removed from the output schema of this operator:
 * 1) nodesToExtendNbrExpr
 * 2) nodesAfterExtendNbrExpr
 * 3) relExpression
 *
 * 1) and 2) are temporary expressions that we are creating so that we can write 2 types of nodes.
 * 1) is for nodes that we want to extend to the next BFS Level. 2) is for the value vector into
 * which we will be writing the neighbour nodes after extension. These value vectors belong to their
 * own data chunk in which the value vectors ar placed. If they are not removed from the schema then
 * logicalProjection will consider the data chunk to be a discarded data chunk pos, and use it later
 * for calculating multiplicity. This causes problems since the selected size of the value vector is
 * taken into account when updating the multiplicity of the result set. To avoid this, we are
 * removing the expressions from the schema. 3) is also being removed from scope, due to same reason
 * because we don't want it to be considered when projection is calculating the multiplicity from
 * the discarded data chunk position.
 *
 * In future, if we have to output the relation properties or other node properties of a path then
 * these expressions can be put back in the output schema of LogicalSimpleRecursion.
 *
 */
void LogicalSimpleRecursiveJoin::computeSchema() {
    auto inSchema = children[0]->getSchema();
    schema = children[0]->getSchema()->copy();
    schema->clearExpressionsInScope();
    auto expressionsInScope = inSchema->getExpressionsInScope();
    for (auto& expression : expressionsInScope) {
        if (expression->getUniqueName() !=
                nodesToExtendNbrExpr->getInternalIDProperty()->getUniqueName() &&
            expression->getUniqueName() !=
                nodesAfterExtendNbrExpr->getInternalIDProperty()->getUniqueName() &&
            expression->getUniqueName() !=
                relExpression->getInternalIDProperty()->getUniqueName()) {
            auto groupPos = inSchema->getGroupPos(expression->getUniqueName());
            schema->insertToScope(expression, groupPos);
        }
    }
}

} // namespace planner
} // namespace kuzu
