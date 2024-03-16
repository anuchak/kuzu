#include "planner/logical_plan/extend/logical_csr_build.h"
#include "processor/operator/csr_index_build.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCSRBuild(
    planner::LogicalOperator* logicalOperator) {
    auto logicalCSRBuild = (LogicalCSRBuild*)logicalOperator;
    auto child = mapOperator(logicalOperator->getChild(0).get());
    auto schema = logicalOperator->getSchema();
    auto resultSetDescriptor = std::make_unique<ResultSetDescriptor>(schema);
    auto boundNodeVectorPos = DataPos(
        schema->getExpressionPos(*logicalCSRBuild->getBoundNode()->getInternalIDProperty()));
    auto nbrNodeVectorPos =
        DataPos(schema->getExpressionPos(*logicalCSRBuild->getNbrNode()->getInternalIDProperty()));
    auto relIDVectorPos =
        DataPos(schema->getExpressionPos(*logicalCSRBuild->getRel()->getInternalIDProperty()));
    auto commonNodeTableID = logicalCSRBuild->getBoundNode()->getSingleTableID();
    auto commonEdgeTableID = logicalCSRBuild->getRel()->getSingleTableID();
    auto sharedState = std::make_shared<csrIndexSharedState>();
    return std::make_unique<CSRIndexBuild>(std::move(resultSetDescriptor), commonNodeTableID,
        commonEdgeTableID, boundNodeVectorPos, nbrNodeVectorPos, relIDVectorPos, sharedState,
        std::move(child), getOperatorID(), logicalCSRBuild->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
