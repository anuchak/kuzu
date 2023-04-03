#include "planner/logical_plan/logical_operator/logical_scan_bfs_level.h"
#include "planner/logical_plan/logical_operator/logical_simple_recursive_join.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/shortestpath/scan_bfs_level.h"
#include "processor/operator/shortestpath/simple_recursive_join.h"

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSimpleRecursiveJoinToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto* logicalSimpleRecursiveJoin = (LogicalSimpleRecursiveJoin*)logicalOperator;
    auto logicalPrevOperator = logicalOperator->getChild(0);
    auto prevOperator = mapLogicalOperatorToPhysical(logicalPrevOperator);
    auto* scanBFSLevel = (ScanBFSLevel*)prevOperator->getChild(0)->getChild(0);
    auto outSchema = logicalPrevOperator->getSchema();
    auto nodeIDVectorDataPos = DataPos(outSchema->getExpressionPos(
        *logicalSimpleRecursiveJoin->getNbrNodeExpression()->getInternalIDProperty()));
    auto dstDistanceVectorDataPos =
        DataPos(outSchema->getExpressionPos(*logicalSimpleRecursiveJoin->getPathExpression()));
    std::vector<DataPos> srcDstVectorsDataPos = std::vector<DataPos>();
    srcDstVectorsDataPos.emplace_back(outSchema->getExpressionPos(
        *logicalSimpleRecursiveJoin->getSourceNodeExpression()->getInternalIDProperty()));
    srcDstVectorsDataPos.emplace_back(outSchema->getExpressionPos(
        *logicalSimpleRecursiveJoin->getDstNodeExpression()->getInternalIDProperty()));
    return std::move(std::make_unique<SimpleRecursiveJoin>(
        logicalSimpleRecursiveJoin->getLowerBound(), logicalSimpleRecursiveJoin->getUpperBound(),
        scanBFSLevel->getSimpleRecursiveJoinGlobalState(), nodeIDVectorDataPos,
        srcDstVectorsDataPos, dstDistanceVectorDataPos, std::move(prevOperator), getOperatorID(),
        logicalSimpleRecursiveJoin->getExpressionsForPrinting()));
}
} // namespace processor
} // namespace kuzu
