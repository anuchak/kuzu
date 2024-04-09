#include "planner/logical_plan/extend/logical_csr_build.h"
#include "processor/operator/csr_index_build.h"
#include "processor/operator/scan/scan_rel_table.h"
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
    auto& relsStore = storageManager.getRelsStore();
    auto relDataDirection =
        ExtendDirectionUtils::getRelDataDirection(logicalCSRBuild->getDirection());
    auto relTableID = logicalCSRBuild->getRel()->getSingleTableID();
    auto relTableDataType =
        relsStore.isSingleMultiplicityInDirection(relDataDirection, relTableID) ?
            storage::RelTableDataType::COLUMNS :
            storage::RelTableDataType::LISTS;
    auto relData = relsStore.getRelTable(relTableID)->getDirectedTableData(relDataDirection);
    std::vector<common::property_id_t> propertyIds;
    for (auto& property : logicalCSRBuild->getRel()->getPropertyExpressions()) {
        auto propertyExpression = reinterpret_cast<binder::PropertyExpression*>(property.get());
        propertyIds.push_back(propertyExpression->hasPropertyID(relTableID) ?
                                  propertyExpression->getPropertyID(relTableID) :
                                  common::INVALID_PROPERTY_ID);
    }
    auto relStats = relsStore.getRelsStatistics().getRelStatistics(relTableID);
    auto scanInfo = std::make_unique<RelTableScanInfo>(
        relTableDataType, relData, relStats, std::move(propertyIds));
    auto headers = scanInfo->tableData->getAdjLists()->getHeaders();
    return std::make_unique<CSRIndexBuild>(std::move(resultSetDescriptor), commonNodeTableID,
        commonEdgeTableID, boundNodeVectorPos, nbrNodeVectorPos, relIDVectorPos, sharedState,
        headers, std::move(child), getOperatorID(), logicalCSRBuild->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
