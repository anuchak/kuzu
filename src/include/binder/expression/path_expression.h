#include "expression.h"
#include "node_expression.h"
#include "rel_expression.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace binder {

class PathExpression : public Expression {
public:
    PathExpression(common::DataTypeID dataTypeID, const std::string& uniqueName,
        std::vector<std::shared_ptr<NodeExpression>> nodeExpressions,
        std::vector<std::shared_ptr<RelExpression>> relExpressions)
        : Expression{common::VARIABLE, dataTypeID, uniqueName}, variableName{uniqueName},
          nodeExpressions{std::move(nodeExpressions)}, relExpressions{std::move(relExpressions)} {}

    const std::string& getVariableName() const { return variableName; }

    const std::shared_ptr<NodeExpression>& getSrcExpression() const { return nodeExpressions[0]; }

    const std::shared_ptr<RelExpression>& getRelExpression() const { return relExpressions[0]; }

    const std::shared_ptr<NodeExpression>& getDestExpression() const {
        return nodeExpressions[nodeExpressions.size() - 1];
    }

    std::shared_ptr<Expression> getPathLengthExpression() const { return pathLengthExpression; }

    void setPathLengthExpression(const std::shared_ptr<Expression>& pathLengthExpression_) {
        pathLengthExpression = pathLengthExpression_;
    }

    std::string toString() const override { return variableName; }

    std::unique_ptr<Expression> copy() const override {
        auto pathExpression = std::make_unique<PathExpression>(
            dataType.getTypeID(), uniqueName, nodeExpressions, relExpressions);
        pathExpression->setPathLengthExpression(pathLengthExpression);
        return std::move(pathExpression);
    }

private:
    std::string variableName;
    std::vector<std::shared_ptr<NodeExpression>> nodeExpressions;
    std::vector<std::shared_ptr<RelExpression>> relExpressions;
    std::shared_ptr<Expression> pathLengthExpression;
};
} // namespace binder
} // namespace kuzu
