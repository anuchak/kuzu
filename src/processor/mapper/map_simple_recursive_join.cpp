#include "planner/logical_plan/logical_operator/logical_scan_bfs_level.h"
#include "planner/logical_plan/logical_operator/logical_simple_recursive_join.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/shortestpath/scan_bfs_level.h"
#include "processor/operator/shortestpath/simple_recursive_join.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSimpleRecursiveJoinToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto* logicalSimpleRecursiveJoin = (LogicalSimpleRecursiveJoin*)logicalOperator;
    auto logicalPrevOperator = logicalOperator->getChild(0);
    auto prevOperator = mapLogicalOperatorToPhysical(logicalPrevOperator);
    auto* scanBFSLevel = (ScanBFSLevel*)prevOperator->getChild(0)->getChild(0);
    auto inputSchema = logicalPrevOperator->getSchema();
    auto inputIDPos = DataPos(inputSchema->getExpressionPos(
        *logicalSimpleRecursiveJoin->getNbrNodeExpression()->getInternalIDProperty()));
    auto dstDistancesPos =
        DataPos(inputSchema->getExpressionPos(*logicalSimpleRecursiveJoin->getPathExpression()));
    auto sharedOutputFTState = std::make_shared<FTableSharedState>();
    std::vector<std::pair<DataPos, common::DataType>> payloadsPosAndType;
    std::vector<bool> isPayloadFlat;
    std::vector<DataPos> outVecPositions;
    std::vector<uint32_t> colIndicesToScan;
    auto dstNodeIDExpression =
        logicalSimpleRecursiveJoin->getDstNodeExpression()->getInternalIDProperty();
    auto dstInternalIDPos = DataPos(inputSchema->getExpressionPos(*dstNodeIDExpression));
    auto expressions = logicalSimpleRecursiveJoin->getSchema()->getExpressionsInScope();
    for (int i = 0; i < expressions.size(); i++) {
        auto dataPos = DataPos(inputSchema->getExpressionPos(*expressions[i]));
        auto expressionName = expressions[i]->getUniqueName();
        auto isFlat = inputSchema->getGroup(expressionName)->isFlat();
        payloadsPosAndType.emplace_back(dataPos, expressions[i]->dataType);
        isPayloadFlat.push_back(isFlat);
        outVecPositions.push_back(dataPos);
        colIndicesToScan.push_back(i);
    }
    auto simpleRecursiveJoin = std::make_unique<SimpleRecursiveJoin>(
        std::make_unique<ResultSetDescriptor>(*logicalSimpleRecursiveJoin->getSchema()),
        logicalSimpleRecursiveJoin->getLowerBound(), logicalSimpleRecursiveJoin->getUpperBound(),
        dstInternalIDPos, scanBFSLevel->getSSSPMorselTracker(), inputIDPos, dstDistancesPos,
        sharedOutputFTState, payloadsPosAndType, isPayloadFlat, std::move(prevOperator),
        getOperatorID(), logicalSimpleRecursiveJoin->getExpressionsForPrinting());
    auto ftableScan = std::make_unique<FactorizedTableScan>(outVecPositions, colIndicesToScan,
        sharedOutputFTState, std::move(simpleRecursiveJoin), getOperatorID(),
        logicalSimpleRecursiveJoin->getExpressionsForPrinting());
    return ftableScan;
}
} // namespace processor
} // namespace kuzu
