#pragma once

#include "base_logical_operator.h"
#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalExtend : public LogicalOperator {
public:
    LogicalExtend(shared_ptr<NodeExpression> boundNode, shared_ptr<NodeExpression> nbrNode,
        shared_ptr<RelExpression> rel, RelDirection direction, bool extendToNewGroup,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, boundNode{std::move(boundNode)}, nbrNode{std::move(
                                                                                  nbrNode)},
          rel{std::move(rel)}, direction{direction}, extendToNewGroup{extendToNewGroup} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

    string getExpressionsForPrinting() const override {
        return boundNode->getRawName() + (direction == RelDirection::FWD ? "->" : "<-") +
               nbrNode->getRawName();
    }

    inline virtual void computeSchema(Schema& schema) {
        auto boundGroupPos = schema.getGroupPos(boundNode->getInternalIDPropertyName());
        uint32_t nbrGroupPos = 0u;
        if (!extendToNewGroup) {
            nbrGroupPos = boundGroupPos;
        } else {
            assert(schema.getGroup(boundGroupPos)->isFlat());
            nbrGroupPos = schema.createGroup();
        }
        schema.insertToGroupAndScope(nbrNode->getInternalIDProperty(), nbrGroupPos);
    }

    inline shared_ptr<NodeExpression> getBoundNode() const { return boundNode; }
    inline shared_ptr<NodeExpression> getNbrNode() const { return nbrNode; }
    inline shared_ptr<RelExpression> getRel() const { return rel; }
    inline RelDirection getDirection() const { return direction; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExtend>(
            boundNode, nbrNode, rel, direction, extendToNewGroup, children[0]->copy());
    }

protected:
    shared_ptr<NodeExpression> boundNode;
    shared_ptr<NodeExpression> nbrNode;
    shared_ptr<RelExpression> rel;
    RelDirection direction;
    // When extend might increase cardinality (i.e. n * m), we extend to a new factorization group.
    bool extendToNewGroup;
};

class LogicalGenericExtend : public LogicalExtend {
public:
    LogicalGenericExtend(shared_ptr<NodeExpression> boundNode, shared_ptr<NodeExpression> nbrNode,
        shared_ptr<RelExpression> rel, RelDirection direction, bool extendToNewGroup,
        expression_vector properties, shared_ptr<LogicalOperator> child)
        : LogicalExtend(std::move(boundNode), std::move(nbrNode), std::move(rel), direction,
              extendToNewGroup, std::move(child)),
          properties{std::move(properties)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_GENERIC_EXTEND;
    }

    inline void computeSchema(Schema& schema) override {
        LogicalExtend::computeSchema(schema);
        auto nbrNodePos = schema.getGroupPos(nbrNode->getInternalIDPropertyName());
        for (auto& property : properties) {
            schema.insertToGroupAndScope(property, nbrNodePos);
        }
    }

    inline expression_vector getProperties() const { return properties; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalGenericExtend>(
            boundNode, nbrNode, rel, direction, extendToNewGroup, properties, children[0]->copy());
    }

private:
    expression_vector properties;
};

} // namespace planner
} // namespace kuzu
