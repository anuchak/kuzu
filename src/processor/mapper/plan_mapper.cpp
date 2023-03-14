#include "processor/mapper/plan_mapper.h"

#include <set>

#include "planner/logical_plan/logical_operator/logical_cross_product.h"
#include "planner/logical_plan/logical_operator/logical_scan_bfs_level.h"
#include "planner/logical_plan/logical_operator/logical_simple_recursive_join.h"
#include "processor/mapper/expression_mapper.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/shortestpath/scan_bfs_level.h"
#include "processor/operator/shortestpath/simple_recursive_join.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalPlan> PlanMapper::mapLogicalPlanToPhysical(LogicalPlan* logicalPlan,
    const binder::expression_vector& expressionsToCollect, common::StatementType statementType) {
    auto lastOperator = mapLogicalOperatorToPhysical(logicalPlan->getLastOperator());
    if (!StatementTypeUtils::isCopyCSV(statementType)) {
        lastOperator = appendResultCollector(
            expressionsToCollect, *logicalPlan->getSchema(), std::move(lastOperator));
    }
    return make_unique<PhysicalPlan>(std::move(lastOperator));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalOperatorToPhysical(
    const std::shared_ptr<LogicalOperator>& logicalOperator) {
    std::unique_ptr<PhysicalOperator> physicalOperator;
    auto operatorType = logicalOperator->getOperatorType();
    switch (operatorType) {
    case LogicalOperatorType::SCAN_NODE: {
        physicalOperator = mapLogicalScanNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        physicalOperator = mapLogicalIndexScanNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::UNWIND: {
        physicalOperator = mapLogicalUnwindToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::EXTEND: {
        physicalOperator = mapLogicalExtendToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FLATTEN: {
        physicalOperator = mapLogicalFlattenToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FILTER: {
        physicalOperator = mapLogicalFilterToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::PROJECTION: {
        physicalOperator = mapLogicalProjectionToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SEMI_MASKER: {
        physicalOperator = mapLogicalSemiMaskerToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        physicalOperator = mapLogicalHashJoinToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::INTERSECT: {
        physicalOperator = mapLogicalIntersectToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CROSS_PRODUCT: {
        physicalOperator = mapLogicalCrossProductToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SCAN_NODE_PROPERTY: {
        physicalOperator = mapLogicalScanNodePropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::MULTIPLICITY_REDUCER: {
        physicalOperator = mapLogicalMultiplicityReducerToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SKIP: {
        physicalOperator = mapLogicalSkipToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::LIMIT: {
        physicalOperator = mapLogicalLimitToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::AGGREGATE: {
        physicalOperator = mapLogicalAggregateToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DISTINCT: {
        physicalOperator = mapLogicalDistinctToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ORDER_BY: {
        physicalOperator = mapLogicalOrderByToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::UNION_ALL: {
        physicalOperator = mapLogicalUnionAllToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ACCUMULATE: {
        physicalOperator = mapLogicalAccumulateToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        physicalOperator = mapLogicalExpressionsScanToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::FTABLE_SCAN: {
        physicalOperator = mapLogicalFTableScanToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_NODE: {
        physicalOperator = mapLogicalCreateNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_REL: {
        physicalOperator = mapLogicalCreateRelToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        physicalOperator = mapLogicalSetNodePropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SET_REL_PROPERTY: {
        physicalOperator = mapLogicalSetRelPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DELETE_NODE: {
        physicalOperator = mapLogicalDeleteNodeToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DELETE_REL: {
        physicalOperator = mapLogicalDeleteRelToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_NODE_TABLE: {
        physicalOperator = mapLogicalCreateNodeTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::CREATE_REL_TABLE: {
        physicalOperator = mapLogicalCreateRelTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::COPY_CSV: {
        physicalOperator = mapLogicalCopyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DROP_TABLE: {
        physicalOperator = mapLogicalDropTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::RENAME_TABLE: {
        physicalOperator = mapLogicalRenameTableToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::ADD_PROPERTY: {
        physicalOperator = mapLogicalAddPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::DROP_PROPERTY: {
        physicalOperator = mapLogicalDropPropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::RENAME_PROPERTY: {
        physicalOperator = mapLogicalRenamePropertyToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SCAN_BFS_LEVEL: {
        physicalOperator = mapLogicalScanBFSLevelToPhysical(logicalOperator.get());
    } break;
    case LogicalOperatorType::SIMPLE_RECURSIVE_JOIN: {
        physicalOperator = mapLogicalSimpleRecursiveJoinToPhysical(logicalOperator.get());
    } break;
    default:
        assert(false);
    }
    return physicalOperator;
}

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
    auto maxNodeOffset = maxNodeOffsetsPerTable.at(
        logicalScanBFSLevel->getTmpDestNodeExpression()->getSingleTableID());
    DataPos nodesToExtendDataPos = DataPos(outSchema->getExpressionPos(
        *logicalScanBFSLevel->getNodesToExtendBoundExpr()->getInternalIDProperty()));
    std::vector<DataPos> srcDstNodeIDVectorsDataPos = std::vector<DataPos>();
    srcDstNodeIDVectorsDataPos.emplace_back(outSchema->getExpressionPos(
        *logicalScanBFSLevel->getTmpSourceNodeExpression()->getInternalIDProperty()));
    srcDstNodeIDVectorsDataPos.emplace_back(outSchema->getExpressionPos(
        *logicalScanBFSLevel->getTmpDestNodeExpression()->getInternalIDProperty()));
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
        logicalScanBFSLevel->getTmpSourceNodeExpression()->getInternalIDProperty()->getUniqueName();
    auto internalIDExprDest =
        logicalScanBFSLevel->getTmpDestNodeExpression()->getInternalIDProperty()->getUniqueName();
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSimpleRecursiveJoinToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto* logicalSimpleRecursiveJoin = (LogicalSimpleRecursiveJoin*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto* scanBFSLevel = (ScanBFSLevel*)prevOperator->getChild(0)->getChild(0);
    auto inSchema = logicalSimpleRecursiveJoin->getChild(0)->getSchema();
    auto nodeIDVectorDataPos = DataPos(inSchema->getExpressionPos(
        *logicalSimpleRecursiveJoin->getNodesAfterExtendExpression()->getInternalIDProperty()));
    return std::move(
        std::make_unique<SimpleRecursiveJoin>(scanBFSLevel->getSimpleRecursiveJoinGlobalState(),
            nodeIDVectorDataPos, std::move(prevOperator), getOperatorID(),
            logicalSimpleRecursiveJoin->getExpressionsForPrinting()));
}

std::unique_ptr<ResultCollector> PlanMapper::appendResultCollector(
    const binder::expression_vector& expressionsToCollect, const Schema& schema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<std::pair<DataPos, DataType>> payloadsPosAndType;
    std::vector<bool> isPayloadFlat;
    for (auto& expression : expressionsToCollect) {
        auto expressionName = expression->getUniqueName();
        auto dataPos = DataPos(schema.getExpressionPos(*expression));
        auto isFlat = schema.getGroup(expressionName)->isFlat();
        payloadsPosAndType.emplace_back(dataPos, expression->dataType);
        isPayloadFlat.push_back(isFlat);
    }
    auto sharedState = std::make_shared<FTableSharedState>();
    return make_unique<ResultCollector>(std::make_unique<ResultSetDescriptor>(schema),
        payloadsPosAndType, isPayloadFlat, sharedState, std::move(prevOperator), getOperatorID(),
        binder::ExpressionUtil::toString(expressionsToCollect));
}

} // namespace processor
} // namespace kuzu
