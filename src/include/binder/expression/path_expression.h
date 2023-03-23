#include "expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

class PathExpression : public Expression {
public:
    PathExpression(common::DataTypeID dataTypeID, const std::string& uniqueName,
        std::shared_ptr<Expression> srcExpression, std::shared_ptr<Expression> relExpression,
        std::shared_ptr<Expression> destExpression)
        : Expression{common::VARIABLE, dataTypeID, uniqueName}, variableName{uniqueName},
          srcExpression{std::move(srcExpression)}, relExpression{std::move(relExpression)},
          destExpression{std::move(destExpression)} {}

    const std::string& getVariableName() const { return variableName; }

    const std::shared_ptr<Expression>& getSrcExpression() const { return srcExpression; }

    const std::shared_ptr<Expression>& getRelExpression() const { return relExpression; }

    const std::shared_ptr<Expression>& getDestExpression() const { return destExpression; }

    std::shared_ptr<Expression> getPathLengthExpression() const { return pathLengthExpression; }

    void setPathLengthExpression(const std::shared_ptr<Expression>& pathLengthExpression_) {
        pathLengthExpression = pathLengthExpression_;
    }

    std::string toString() const override { return variableName; }

    std::unique_ptr<Expression> copy() const override {
        auto pathExpression = std::make_unique<PathExpression>(
            dataType.getTypeID(), uniqueName, srcExpression, relExpression, destExpression);
        pathExpression->setPathLengthExpression(pathLengthExpression);
        return std::move(pathExpression);
    }

private:
    std::string variableName;
    std::shared_ptr<Expression> srcExpression;
    std::shared_ptr<Expression> relExpression;
    std::shared_ptr<Expression> destExpression;
    std::shared_ptr<Expression> pathLengthExpression;
};
} // namespace binder
} // namespace kuzu
