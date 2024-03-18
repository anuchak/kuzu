#include "binder/expression_visitor.h"
#include "planner/join_order/cost_model.h"
#include "planner/logical_plan/extend/logical_csr_build.h"
#include "planner/logical_plan/extend/logical_extend.h"
#include "planner/logical_plan/extend/logical_recursive_extend.h"
#include "planner/logical_plan/logical_node_label_filter.h"
#include "planner/logical_plan/scan/logical_scan_node.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

static bool extendHasAtMostOneNbrGuarantee(RelExpression& rel, NodeExpression& boundNode,
    ExtendDirection direction, const catalog::Catalog& catalog) {
    if (boundNode.isMultiLabeled()) {
        return false;
    }
    if (rel.isMultiLabeled()) {
        return false;
    }
    if (direction == ExtendDirection::BOTH) {
        return false;
    }
    auto relDirection = ExtendDirectionUtils::getRelDataDirection(direction);
    return catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
        rel.getSingleTableID(), relDirection);
}

static std::unordered_set<table_id_t> getBoundNodeTableIDSet(
    const RelExpression& rel, ExtendDirection extendDirection, const catalog::Catalog& catalog) {
    std::unordered_set<table_id_t> result;
    for (auto tableID : rel.getTableIDs()) {
        auto tableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(tableID);
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            result.insert(tableSchema->getBoundTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            result.insert(tableSchema->getBoundTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            result.insert(tableSchema->getBoundTableID(RelDataDirection::FWD));
            result.insert(tableSchema->getBoundTableID(RelDataDirection::BWD));
        } break;
        default:
            throw NotImplementedException("getBoundNodeTableIDSet");
        }
    }
    return result;
}

static std::unordered_set<table_id_t> getNbrNodeTableIDSet(
    const RelExpression& rel, ExtendDirection extendDirection, const catalog::Catalog& catalog) {
    std::unordered_set<table_id_t> result;
    for (auto tableID : rel.getTableIDs()) {
        auto tableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(tableID);
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            result.insert(tableSchema->getNbrTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            result.insert(tableSchema->getNbrTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            result.insert(tableSchema->getNbrTableID(RelDataDirection::FWD));
            result.insert(tableSchema->getNbrTableID(RelDataDirection::BWD));
        } break;
        default:
            throw NotImplementedException("getNbrNodeTableIDSet");
        }
    }
    return result;
}

void QueryPlanner::appendNonRecursiveExtend(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
    ExtendDirection direction, const expression_vector& properties, LogicalPlan& plan) {
    auto boundNodeTableIDSet = getBoundNodeTableIDSet(*rel, direction, catalog);
    if (boundNode->getNumTableIDs() > boundNodeTableIDSet.size()) {
        appendNodeLabelFilter(boundNode->getInternalIDProperty(), boundNodeTableIDSet, plan);
    }
    auto hasAtMostOneNbr = extendHasAtMostOneNbrGuarantee(*rel, *boundNode, direction, catalog);
    auto extend = make_shared<LogicalExtend>(
        boundNode, nbrNode, rel, direction, properties, hasAtMostOneNbr, plan.getLastOperator());
    appendFlattens(extend->getGroupsPosToFlatten(), plan);
    extend->setChild(0, plan.getLastOperator());
    extend->computeFactorizedSchema();
    // update cost
    plan.setCost(CostModel::computeExtendCost(plan));
    // update cardinality. Note that extend does not change cardinality.
    if (!hasAtMostOneNbr) {
        auto extensionRate = cardinalityEstimator->getExtensionRate(*rel, *boundNode);
        auto group = extend->getSchema()->getGroup(nbrNode->getInternalIDProperty());
        group->setMultiplier(extensionRate);
    }
    plan.setLastOperator(std::move(extend));
    auto nbrNodeTableIDSet = getNbrNodeTableIDSet(*rel, direction, catalog);
    if (nbrNodeTableIDSet.size() > nbrNode->getNumTableIDs()) {
        appendNodeLabelFilter(nbrNode->getInternalIDProperty(), nbrNode->getTableIDsSet(), plan);
    }
}

void QueryPlanner::appendRecursiveExtend(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
    ExtendDirection direction, LogicalPlan& plan) {
    auto recursiveInfo = rel->getRecursiveInfo();
    appendAccumulate(AccumulateType::REGULAR, plan);
    // Create recursive plan
    // auto recursivePlan = std::make_unique<LogicalPlan>();

    // CREATE THE CSR INDEX CREATION PLAN HERE, INSTEAD OF RECURSIVE SUB-PLAN
    auto csrIndexPlan = std::make_unique<LogicalPlan>();
    createCSRIndexPlan(recursiveInfo->node, recursiveInfo->nodeCopy, rel, recursiveInfo->rel,
        direction, recursiveInfo->predicates, *csrIndexPlan);
    // Create recursive extend
    if (boundNode->getNumTableIDs() > recursiveInfo->node->getNumTableIDs()) {
        appendNodeLabelFilter(
            boundNode->getInternalIDProperty(), recursiveInfo->node->getTableIDsSet(), plan);
    }
    auto extend = std::make_shared<LogicalRecursiveExtend>(boundNode, nbrNode, rel, direction,
        RecursiveJoinType::TRACK_PATH, plan.getLastOperator(), csrIndexPlan->getLastOperator());
    appendFlattens(extend->getGroupsPosToFlatten(), plan);
    extend->setChild(0, plan.getLastOperator());
    extend->computeFactorizedSchema();
    // Create path node property scan plan
    auto pathNodePropertyScanPlan = std::make_unique<LogicalPlan>();
    createPathNodePropertyScanPlan(recursiveInfo->node, *pathNodePropertyScanPlan);
    // Create path rel property scan plan
    auto pathRelPropertyScanPlan = std::make_unique<LogicalPlan>();
    createPathRelPropertyScanPlan(recursiveInfo->node, recursiveInfo->nodeCopy, recursiveInfo->rel,
        direction, *pathRelPropertyScanPlan);
    // Create path property probe
    auto pathPropertyProbe = std::make_shared<LogicalPathPropertyProbe>(rel, extend,
        pathNodePropertyScanPlan->getLastOperator(), pathRelPropertyScanPlan->getLastOperator());
    pathPropertyProbe->computeFactorizedSchema();
    // Check for sip
    auto ratio = plan.getCardinality() / pathRelPropertyScanPlan->getCardinality();
    if (ratio > PlannerKnobs::SIP_RATIO) {
        pathPropertyProbe->setSIP(SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD);
    }
    // Update cost
    auto extensionRate = cardinalityEstimator->getExtensionRate(*rel, *boundNode);
    plan.setCost(CostModel::computeRecursiveExtendCost(rel->getUpperBound(), extensionRate, plan));
    // Update cardinality
    auto hasAtMostOneNbr = extendHasAtMostOneNbrGuarantee(*rel, *boundNode, direction, catalog);
    if (!hasAtMostOneNbr) {
        auto group = pathPropertyProbe->getSchema()->getGroup(nbrNode->getInternalIDProperty());
        group->setMultiplier(extensionRate);
    }
    plan.setLastOperator(std::move(pathPropertyProbe));
}

// TODO: Currently hardcoding the property to be used as cost for computing weighted shortest path.
// TODO: The property name that has "weight" in its name is being picked. It should be added as a
// TODO: parameter in the query and then added in the "recursiveRel" expression parameter.
// CHANGING THIS FUNCTION TO ENABLE CSR BUILDING PLAN INSTEAD OF RECURSIVE SUBPLAN
// Plan will be ScanNode -> ScanRelList -> CSRBuild and this plan will be a child
void QueryPlanner::createCSRIndexPlan(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> recursiveNode, std::shared_ptr<RelExpression>& relExpression,
    std::shared_ptr<RelExpression> recursiveRel, ExtendDirection direction,
    const expression_vector& predicates, LogicalPlan& plan) {
    // Create a scan operator for bound node (src)
    auto scan = std::make_shared<LogicalScanNode>(boundNode);
    scan->computeFactorizedSchema();
    plan.setCardinality(cardinalityEstimator->estimateScanNode(scan.get()));
    plan.setLastOperator(std::move(scan));

    // Create a scan rel (extend) operator for bound node (src) to generate the nbr nodes
    expression_set propertiesSet;
    propertiesSet.insert(recursiveRel->getInternalIDProperty());
    // Todo: This is a workaround to ensure we scan the weight property of the edge relationships.
    // Todo: When we add the weight property to the propertiesSet, while computing schema of the
    // Todo: operator the properties are added to the scope of the schema.
    if (relExpression->getRelType() == QueryRelType::WSHORTEST) {
        for (auto& expression : recursiveRel->getPropertyExpressions()) {
            auto propertyExpression = (PropertyExpression*)expression.get();
            if (propertyExpression->getPropertyName().find("weight") != std::string::npos) {
                propertiesSet.insert(propertyExpression->copy());
            }
        }
    }
    for (auto& predicate : predicates) {
        auto expressionCollector = std::make_unique<binder::ExpressionCollector>();
        for (auto& property : expressionCollector->collectPropertyExpressions(predicate)) {
            propertiesSet.insert(property);
        }
    }
    expression_vector properties;
    for (auto& property : propertiesSet) {
        properties.push_back(property);
    }
    appendNonRecursiveExtend(boundNode, recursiveNode, recursiveRel, direction, properties, plan);
    appendFilters(predicates, plan);

    // Create the CSR Build operator that builds the CSR Index
    auto logicalCSRBuild = std::make_unique<LogicalCSRBuild>(LogicalOperatorType::CSR_BUILD,
        boundNode, recursiveNode, recursiveRel, direction, plan.getLastOperator());
    plan.setLastOperator(std::move(logicalCSRBuild));
}

void QueryPlanner::createPathNodePropertyScanPlan(
    std::shared_ptr<NodeExpression> recursiveNode, LogicalPlan& plan) {
    appendScanNodeID(recursiveNode, plan);
    expression_vector properties;
    for (auto& property : recursiveNode->getPropertyExpressions()) {
        properties.push_back(property->copy());
    }
    appendScanNodeProperties(properties, recursiveNode, plan);
    auto expressionsToProject = properties;
    expressionsToProject.push_back(recursiveNode->getInternalIDProperty());
    expressionsToProject.push_back(recursiveNode->getLabelExpression());
    appendProjection(expressionsToProject, plan);
}

void QueryPlanner::createPathRelPropertyScanPlan(std::shared_ptr<NodeExpression> recursiveNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> recursiveRel,
    ExtendDirection direction, LogicalPlan& plan) {
    appendScanNodeID(recursiveNode, plan);
    expression_vector properties;
    for (auto& property : recursiveRel->getPropertyExpressions()) {
        properties.push_back(property->copy());
    }
    appendNonRecursiveExtend(recursiveNode, nbrNode, recursiveRel, direction, properties, plan);
    auto expressionsToProject = properties;
    expressionsToProject.push_back(recursiveRel->getLabelExpression());
    appendProjection(expressionsToProject, plan);
}

void QueryPlanner::appendNodeLabelFilter(std::shared_ptr<Expression> nodeID,
    std::unordered_set<table_id_t> tableIDSet, LogicalPlan& plan) {
    auto filter = std::make_shared<LogicalNodeLabelFilter>(
        std::move(nodeID), std::move(tableIDSet), plan.getLastOperator());
    filter->computeFactorizedSchema();
    plan.setLastOperator(std::move(filter));
}

} // namespace planner
} // namespace kuzu
