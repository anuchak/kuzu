#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"
#include "scan_semi_join.h"

namespace kuzu {
namespace processor {

class SimpleRecursiveJoin : public PhysicalOperator {

public:
    SimpleRecursiveJoin(uint32_t id, const std::string& paramsString,
        std::shared_ptr<SimpleRecursiveJoinSharedState> simpleRecursiveJoinSharedState,
        const DataPos& srcNodesDataPos, const DataPos& destNodeDataPos)
        : PhysicalOperator(PhysicalOperatorType::RECURSIVE_SCAN_SEMI_JOIN, id, paramsString),
          simpleRecursiveJoinSharedState{std::move(simpleRecursiveJoinSharedState)},
          srcNodesDataPos{srcNodesDataPos}, destNodeDataPos{destNodeDataPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SimpleRecursiveJoin>(
            id, paramsString, simpleRecursiveJoinSharedState, srcNodesDataPos, destNodeDataPos);
    }

private:
    DataPos srcNodesDataPos;
    DataPos destNodeDataPos;
    std::shared_ptr<common::ValueVector> srcValVector;
    std::shared_ptr<common::ValueVector> destValVector;
    std::shared_ptr<SimpleRecursiveJoinSharedState> simpleRecursiveJoinSharedState;
};

} // namespace processor
} // namespace kuzu
