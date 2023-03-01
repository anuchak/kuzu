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
        const DataPos& dstNodeDataPos, const DataPos& nodeIDVectorDataPos)
        : PhysicalOperator(PhysicalOperatorType::SCAN_BFS_LEVEL, id, paramsString),
          simpleRecursiveJoinGlobalState{std::move(simpleRecursiveJoinSharedState)},
          dstNodeDataPos{dstNodeDataPos}, inputNodeIDDataPos{nodeIDVectorDataPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SimpleRecursiveJoin>(
            id, paramsString, simpleRecursiveJoinGlobalState, dstNodeDataPos, inputNodeIDDataPos);
    }

private:
    std::thread::id threadID;
    DataPos dstNodeDataPos;
    DataPos inputNodeIDDataPos;
    std::shared_ptr<common::ValueVector> inputNodeIDVector;
    std::shared_ptr<common::ValueVector> dstValVector;
    std::unordered_set<common::offset_t> dstNodeOffsets;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};

} // namespace processor
} // namespace kuzu
