#pragma once

#include "common/enums/accumulate_type.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCrossProduct : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::CROSS_PRODUCT;

public:
    LogicalCrossProduct(common::AccumulateType accumulateType,
        std::shared_ptr<LogicalOperator> probeChild, std::shared_ptr<LogicalOperator> buildChild)
        : LogicalOperator{type_, std::move(probeChild), std::move(buildChild)},
          accumulateType{accumulateType} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    inline common::AccumulateType getAccumulateType() const { return accumulateType; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCrossProduct>(accumulateType, children[0]->copy(),
            children[1]->copy());
    }

private:
    common::AccumulateType accumulateType;
};

} // namespace planner
} // namespace kuzu
