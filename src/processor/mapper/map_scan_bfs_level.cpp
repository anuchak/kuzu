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
    std::vector<DataPos> srcDstNodeIDVectorsDataPos = std::vector<DataPos>();
    srcDstNodeIDVectorsDataPos.emplace_back(outSchema->getExpressionPos(
        *logicalScanBFSLevel->getSourceNodeExpression()->getInternalIDProperty()));
    srcDstNodeIDVectorsDataPos.emplace_back(outSchema->getExpressionPos(
        *logicalScanBFSLevel->getDestNodeExpression()->getInternalIDProperty()));
    std::vector<DataPos> srcDstNodePropertiesVectorsDataPos;
    std::unordered_set<std::string> tempNodePropertiesVector = std::unordered_set<std::string>();
    for (auto& expression : logicalScanBFSLevel->getSrcDstNodePropertiesToScan()) {
        srcDstNodePropertiesVectorsDataPos.emplace_back(outSchema->getExpressionPos(*expression));
        tempNodePropertiesVector.insert(expression->getUniqueName());
    }
    DataPos dstDistanceVectorDataPos =
        DataPos(outSchema->getExpressionPos(*logicalScanBFSLevel->getPathExpression()));
    std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeIDs = std::vector<uint32_t>();
    std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeProperties = std::vector<uint32_t>();
    auto internalIDExprSrc =
        logicalScanBFSLevel->getSourceNodeExpression()->getInternalIDProperty()->getUniqueName();
    auto internalIDExprDest =
        logicalScanBFSLevel->getDestNodeExpression()->getInternalIDProperty()->getUniqueName();
    for (int i = 0; i < subPlanSchema->getExpressionsInScope().size(); i++) {
        auto expression = subPlanSchema->getExpressionsInScope()[i];
        if (expression->getUniqueName() == internalIDExprSrc ||
            expression->getUniqueName() == internalIDExprDest) {
            ftColIndicesOfSrcAndDstNodeIDs.push_back(i);
        } else if (tempNodePropertiesVector.contains(expression->getUniqueName())) {
            ftColIndicesOfSrcAndDstNodeProperties.push_back(i);
        }
    }
    auto sharedState = resultCollector->getSharedState();
    auto simpleRecursiveJoinSharedState =
        std::make_shared<SimpleRecursiveJoinGlobalState>(sharedState);
    auto scanBFSLevel = std::make_unique<ScanBFSLevel>(maxNodeOffset, nodesToExtendDataPos,
        srcDstNodeIDVectorsDataPos, srcDstNodePropertiesVectorsDataPos, dstDistanceVectorDataPos,
        ftColIndicesOfSrcAndDstNodeIDs, ftColIndicesOfSrcAndDstNodeProperties,
        logicalScanBFSLevel->getLowerBound(), logicalScanBFSLevel->getUpperBound(),
        simpleRecursiveJoinSharedState, std::move(resultCollector), getOperatorID(),
        logicalScanBFSLevel->getExpressionsForPrinting());
    return std::move(scanBFSLevel);
}
} // namespace processor
} // namespace kuzu
