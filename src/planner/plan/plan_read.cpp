#include "binder/expression_visitor.h"
#include "binder/query/reading_clause/bound_gds_call.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "common/enums/join_type.h"
#include "function/gds_function.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::planReadingClause(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& prevPlans) {
    auto readingClauseType = readingClause.getClauseType();
    switch (readingClauseType) {
    case ClauseType::MATCH: {
        planMatchClause(readingClause, prevPlans);
    } break;
    case ClauseType::UNWIND: {
        planUnwindClause(readingClause, prevPlans);
    } break;
    case ClauseType::TABLE_FUNCTION_CALL: {
        planTableFunctionCall(readingClause, prevPlans);
    } break;
    case ClauseType::GDS_CALL: {
        planGDSCall(readingClause, prevPlans);
    } break;
    case ClauseType::LOAD_FROM: {
        planLoadFrom(readingClause, prevPlans);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planMatchClause(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto& boundMatchClause = readingClause.constCast<BoundMatchClause>();
    auto queryGraphCollection = boundMatchClause.getQueryGraphCollection();
    auto predicates = boundMatchClause.getConjunctivePredicates();
    switch (boundMatchClause.getMatchClauseType()) {
    case MatchClauseType::MATCH: {
        if (plans.size() == 1 && plans[0]->isEmpty()) {
            auto info = QueryGraphPlanningInfo();
            info.predicates = predicates;
            info.hint = boundMatchClause.getHint();
            plans = enumerateQueryGraphCollection(*queryGraphCollection, info);
        } else {
            for (auto& plan : plans) {
                planRegularMatch(*queryGraphCollection, predicates, *plan);
            }
        }
    } break;
    case MatchClauseType::OPTIONAL_MATCH: {
        for (auto& plan : plans) {
            expression_vector corrExprs;
            if (!plan->isEmpty()) {
                corrExprs =
                    getCorrelatedExprs(*queryGraphCollection, predicates, plan->getSchema());
            }
            planOptionalMatch(*queryGraphCollection, predicates, corrExprs, *plan);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planUnwindClause(const BoundReadingClause& boundReadingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        if (plan->isEmpty()) { // UNWIND [1, 2, 3, 4] AS x RETURN x
            appendDummyScan(*plan);
        }
        appendUnwind(boundReadingClause, *plan);
    }
}

static bool hasExternalDependency(const std::shared_ptr<Expression>& expression,
    const std::unordered_set<std::string>& variableNameSet) {
    auto collector = ExpressionCollector();
    for (auto& name : collector.getDependentVariableNames(expression)) {
        if (!variableNameSet.contains(name)) {
            return true;
        }
    }
    return false;
}

static void splitPredicates(const expression_vector& outputExprs,
    const expression_vector& predicates, expression_vector& predicatesToPull,
    expression_vector& predicatesToPush) {
    std::unordered_set<std::string> columnNameSet;
    for (auto& column : outputExprs) {
        columnNameSet.insert(column->getUniqueName());
    }
    for (auto& predicate : predicates) {
        if (hasExternalDependency(predicate, columnNameSet)) {
            predicatesToPull.push_back(predicate);
        } else {
            predicatesToPush.push_back(predicate);
        }
    }
}

void Planner::planTableFunctionCall(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    expression_vector predicatesToPull;
    expression_vector predicatesToPush;
    splitPredicates(call.getOutExprs(), call.getConjunctivePredicates(), predicatesToPull,
        predicatesToPush);
    for (auto& plan : plans) {
        planReadOp(getTableFunctionCall(readingClause), predicatesToPush, *plan);
        if (!predicatesToPull.empty()) {
            appendFilters(predicatesToPull, *plan);
        }
    }
}

void Planner::planGDSCall(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto& call = readingClause.constCast<BoundGDSCall>();
    expression_vector predicatesToPull;
    expression_vector predicatesToPush;
    splitPredicates(call.getInfo().outExpressions, call.getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);
    auto bindData = call.getInfo().func->ptrCast<function::GDSFunction>()->gds->getBindData();
    if (bindData->hasNodeInput()) {
        auto& node = bindData->nodeInput->constCast<NodeExpression>();
        expression_vector joinConditions;
        joinConditions.push_back(node.getInternalID());
        for (auto& plan : plans) {
            auto probePlan = LogicalPlan();
            appendGDSCall(readingClause, probePlan);
            // This is messing up the sideways information passing (SIP) mask set up.
            // Hardcoding this value here, to ensure SIP is not set to PROHIBIT.
            probePlan.setCardinality(plan->getCardinality());
            // call compute schema separately here, instead of inside appendGDSCall
            probePlan.getLastOperator()->computeFactorizedSchema();
            if (!predicatesToPush.empty()) {
                appendFilters(predicatesToPush, probePlan);
            }
            appendHashJoin(joinConditions, JoinType::INNER, probePlan, *plan, *plan);
        }
    } else {
        for (auto& plan : plans) {
            appendGDSCall(readingClause, *plan.get());
            planReadOp(plan->getLastOperator(), predicatesToPush, *plan);
        }
    }
    for (auto& plan : plans) {
        if (!predicatesToPull.empty()) {
            appendFilters(predicatesToPull, *plan);
        }
    }
}

void Planner::planLoadFrom(const BoundReadingClause& readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto& loadFrom = readingClause.constCast<BoundLoadFrom>();
    expression_vector predicatesToPull;
    expression_vector predicatesToPush;
    splitPredicates(loadFrom.getInfo()->columns, loadFrom.getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);
    for (auto& plan : plans) {
        auto op = getScanFile(loadFrom.getInfo());
        planReadOp(std::move(op), predicatesToPush, *plan);
        if (!predicatesToPull.empty()) {
            appendFilters(predicatesToPull, *plan);
        }
    }
}

void Planner::planReadOp(std::shared_ptr<LogicalOperator> op, const expression_vector& predicates,
    LogicalPlan& plan) {
    op->computeFactorizedSchema();
    if (!plan.isEmpty()) {
        auto tmpPlan = LogicalPlan();
        tmpPlan.setLastOperator(std::move(op));
        if (!predicates.empty()) {
            appendFilters(predicates, tmpPlan);
        }
        appendCrossProduct(AccumulateType::REGULAR, plan, tmpPlan, plan);
    } else {
        plan.setLastOperator(std::move(op));
        if (!predicates.empty()) {
            appendFilters(predicates, plan);
        }
    }
}

} // namespace planner
} // namespace kuzu
