#include "planner/logical_plan/logical_operator/logical_scan_bfs_level.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/shortestpath/scan_bfs_level.h"

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanBFSLevelToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto* logicalScanBFSLevel = (LogicalScanBFSLevel*)logicalOperator;
    auto outSchema = logicalScanBFSLevel->getSchema();
    auto subPlanSchema = logicalScanBFSLevel->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto resultCollector = appendResultCollector(
        subPlanSchema->getExpressionsInScope(), *subPlanSchema, std::move(prevOperator));
    auto maxNodeOffsetsPerTable =
        storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getMaxNodeOffsetPerTable();
    auto maxNodeOffset =
        maxNodeOffsetsPerTable.at(logicalScanBFSLevel->getDestNodeExpression()->getSingleTableID());
    DataPos nodesToExtendDataPos = DataPos(outSchema->getExpressionPos(
        *logicalScanBFSLevel->getNodesToExtendBoundExpr()->getInternalIDProperty()));
    // We need to maintain position of src, dst nodeID at first to identify them separately.
    std::vector<DataPos> srcDstVectorsDataPos = std::vector<DataPos>();
    srcDstVectorsDataPos.emplace_back(outSchema->getExpressionPos(
        *logicalScanBFSLevel->getSourceNodeExpression()->getInternalIDProperty()));
    srcDstVectorsDataPos.emplace_back(outSchema->getExpressionPos(
        *logicalScanBFSLevel->getDestNodeExpression()->getInternalIDProperty()));
    for (auto& expression : logicalScanBFSLevel->getSrcDstNodePropertiesToScan()) {
        srcDstVectorsDataPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    DataPos dstDistanceVectorDataPos =
        DataPos(outSchema->getExpressionPos(*logicalScanBFSLevel->getPathExpression()));
    std::vector<uint32_t> ftColIndicesToScan = std::vector<uint32_t>();
    auto srcInternalIDName =
        logicalScanBFSLevel->getSourceNodeExpression()->getInternalIDProperty()->getUniqueName();
    auto dstInternalIDName =
        logicalScanBFSLevel->getDestNodeExpression()->getInternalIDProperty()->getUniqueName();
    auto expressionsInScope = subPlanSchema->getExpressionsInScope();
    // Same goes for here, we need to maintain src, dst nodeID separately at first to scan.
    for (int i = 0; i < expressionsInScope.size(); i++) {
        if (expressionsInScope[i]->getUniqueName() == srcInternalIDName) {
            ftColIndicesToScan.push_back(i);
        } else if (expressionsInScope[i]->getUniqueName() == dstInternalIDName) {
            ftColIndicesToScan.push_back(i);
            break;
        }
    }
    for (int i = 0; i < subPlanSchema->getExpressionsInScope().size(); i++) {
        auto expression = subPlanSchema->getExpressionsInScope()[i];
        if (expression->getUniqueName() == srcInternalIDName ||
            expression->getUniqueName() == dstInternalIDName) {
            continue;
        }
        ftColIndicesToScan.push_back(i);
    }
    auto sharedState = resultCollector->getSharedState();
    auto simpleRecursiveJoinSharedState =
        std::make_shared<SimpleRecursiveJoinGlobalState>(sharedState);
    auto scanBFSLevel = std::make_unique<ScanBFSLevel>(maxNodeOffset, nodesToExtendDataPos,
        srcDstVectorsDataPos, dstDistanceVectorDataPos, ftColIndicesToScan,
        logicalScanBFSLevel->getLowerBound(), logicalScanBFSLevel->getUpperBound(),
        simpleRecursiveJoinSharedState, std::move(resultCollector), getOperatorID(),
        logicalScanBFSLevel->getExpressionsForPrinting());
    return std::move(scanBFSLevel);
}
} // namespace processor
} // namespace kuzu
