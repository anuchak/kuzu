#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"
#include "scan_semi_join.h"

namespace kuzu {
namespace processor {

class SimpleRecursiveJoin : public PhysicalOperator {

public:
    SimpleRecursiveJoin(uint32_t id, const std::string& paramsString, storage::AdjLists* adjList,
        storage::Lists* relPropertyLists,
        std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinSharedState,
        const DataPos& destNodeDataPos)
        : PhysicalOperator(PhysicalOperatorType::RECURSIVE_SCAN_SEMI_JOIN, id, paramsString),
          adjLists{adjList}, relPropertyLists{relPropertyLists},
          simpleRecursiveJoinGlobalState{std::move(simpleRecursiveJoinSharedState)},
          destNodeDataPos{destNodeDataPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    void extendNode(common::offset_t parentNodeOffset, std::unique_ptr<BFSLevel>& bfsLevel,
        SingleSrcSPState** singleSrcSPState);

    void addToNextFrontier(common::offset_t parentNodeOffset, std::unique_ptr<BFSLevel>& bfsLevel,
        SingleSrcSPState** singleSrcSPState);

    bool getNextBatchOfChildNodes();

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SimpleRecursiveJoin>(id, paramsString, adjLists, relPropertyLists,
            simpleRecursiveJoinGlobalState, destNodeDataPos);
    }

private:
    std::thread::id threadID;
    DataPos destNodeDataPos;
    std::shared_ptr<common::ValueVector> adjNodeIDVector;
    std::shared_ptr<common::ValueVector> relIDVector;
    std::shared_ptr<storage::ListSyncState> listSyncState;
    std::shared_ptr<storage::ListHandle> listHandle;
    storage::AdjLists* adjLists;
    storage::Lists* relPropertyLists;
    std::shared_ptr<storage::ListHandle> listHandles;
    std::shared_ptr<common::ValueVector> destValVector;
    std::unordered_set<common::offset_t> destNodeOffsets;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};

} // namespace processor
} // namespace kuzu
