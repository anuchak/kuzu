#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"
#include "scan_bfs_level.h"

namespace kuzu {
namespace processor {

class SimpleRecursiveJoin : public PhysicalOperator {

public:
    SimpleRecursiveJoin(uint32_t id, const std::string& paramsString,
        std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinSharedState,
        const DataPos& destNodeDataPos, const DataPos& nodeIDVectorDataPos)
        : PhysicalOperator(PhysicalOperatorType::SCAN_BFS_LEVEL, id, paramsString),
          simpleRecursiveJoinGlobalState{std::move(simpleRecursiveJoinSharedState)},
          destNodeDataPos{destNodeDataPos}, inputNodeIDDataPos{nodeIDVectorDataPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SimpleRecursiveJoin>(
            id, paramsString, simpleRecursiveJoinGlobalState, destNodeDataPos, inputNodeIDDataPos);
    }

private:
    std::thread::id threadID;
    DataPos destNodeDataPos;
    DataPos inputNodeIDDataPos;
    std::shared_ptr<common::ValueVector> inputNodeIDVector;
    std::shared_ptr<common::ValueVector> destValVector;
    std::unordered_set<common::offset_t> destNodeOffsets;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};

} // namespace processor
} // namespace kuzu
