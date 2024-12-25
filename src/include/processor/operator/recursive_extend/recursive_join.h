#pragma once

#include "bfs_state.h"
#include "common/enums/extend_direction.h"
#include "common/enums/query_rel_type.h"
#include "frontier_scanner.h"
#include "morsel_dispatcher.h"
#include "planner/operator/extend/recursive_join_type.h"
#include "processor/operator/mask.h"
#include "processor/operator/physical_operator.h"
#include <graph/on_disk_graph.h>

namespace kuzu {
namespace processor {

class OffsetScanNodeTable;

struct RecursiveJoinSharedState {
    std::shared_ptr<MorselDispatcher> morselDispatcher;
    std::shared_ptr<FTableScanSharedState> inputFTableSharedState;
    std::vector<std::unique_ptr<NodeOffsetLevelSemiMask>> semiMasks;
    std::unique_ptr<graph::OnDiskGraph> diskGraph;

    explicit RecursiveJoinSharedState(std::shared_ptr<MorselDispatcher> morselDispatcher,
        std::shared_ptr<FTableScanSharedState> inputFTableSharedState,
        std::vector<std::unique_ptr<NodeOffsetLevelSemiMask>> semiMasks,
        std::unique_ptr<graph::OnDiskGraph> diskGraph)
        : morselDispatcher{std::move(morselDispatcher)},
          inputFTableSharedState{std::move(inputFTableSharedState)},
          semiMasks{std::move(semiMasks)}, diskGraph{std::move(diskGraph)} {}

    inline common::SchedulerType getSchedulerType() { return morselDispatcher->getSchedulerType(); }

    inline std::pair<GlobalSSSPState, SSSPLocalState> getBFSMorsel(
        const std::vector<common::ValueVector*>& vectorsToScan,
        const std::vector<ft_col_idx_t>& colIndicesToScan, common::ValueVector* srcNodeIDVector,
        BaseBFSState* bfsMorsel, common::QueryRelType queryRelType,
        planner::RecursiveJoinType recursiveJoinType) {
        return morselDispatcher->getBFSMorsel(inputFTableSharedState, vectorsToScan,
            colIndicesToScan, srcNodeIDVector, bfsMorsel, queryRelType, recursiveJoinType);
    }

    inline int64_t writeDstNodeIDAndPathLength(
        const std::vector<common::ValueVector*>& vectorsToScan,
        const std::vector<ft_col_idx_t>& colIndicesToScan, common::table_id_t tableID,
        std::unique_ptr<BaseBFSState>& bfsMorsel, RecursiveJoinVectors* vectors) {
        return morselDispatcher->writeDstNodeIDAndPathLength(inputFTableSharedState, vectorsToScan,
            colIndicesToScan, tableID, bfsMorsel, vectors);
    }
};

struct RecursiveJoinDataInfo {
    // Join input info.
    DataPos srcNodePos;
    // Join output info.
    DataPos dstNodePos;
    std::unordered_set<common::table_id_t> dstNodeTableIDs;
    DataPos pathLengthPos;
    // Recursive join info.
    std::unique_ptr<ResultSetDescriptor> localResultSetDescriptor;
    DataPos recursiveSrcNodeIDPos;
    DataPos recursiveNodePredicateExecFlagPos;
    DataPos recursiveDstNodeIDPos;
    std::unordered_set<common::table_id_t> recursiveDstNodeTableIDs;
    DataPos recursiveEdgeIDPos;
    // Path info
    DataPos pathPos;
    std::unordered_map<common::table_id_t, std::string> tableIDToName;

    RecursiveJoinDataInfo() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(RecursiveJoinDataInfo);

private:
    RecursiveJoinDataInfo(const RecursiveJoinDataInfo& other) {
        srcNodePos = other.srcNodePos;
        dstNodePos = other.dstNodePos;
        dstNodeTableIDs = other.dstNodeTableIDs;
        pathLengthPos = other.pathLengthPos;
        localResultSetDescriptor = other.localResultSetDescriptor->copy();
        recursiveSrcNodeIDPos = other.recursiveSrcNodeIDPos;
        recursiveNodePredicateExecFlagPos = other.recursiveNodePredicateExecFlagPos;
        recursiveDstNodeIDPos = other.recursiveDstNodeIDPos;
        recursiveDstNodeTableIDs = other.recursiveDstNodeTableIDs;
        recursiveEdgeIDPos = other.recursiveEdgeIDPos;
        pathPos = other.pathPos;
        tableIDToName = other.tableIDToName;
    }
};

struct RecursiveJoinInfo {
    RecursiveJoinDataInfo dataInfo;
    uint8_t lowerBound;
    uint8_t upperBound;
    common::QueryRelType queryRelType;
    planner::RecursiveJoinType joinType;
    common::ExtendDirection direction;

    RecursiveJoinInfo() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(RecursiveJoinInfo);

private:
    RecursiveJoinInfo(const RecursiveJoinInfo& other) {
        dataInfo = other.dataInfo.copy();
        lowerBound = other.lowerBound;
        upperBound = other.upperBound;
        queryRelType = other.queryRelType;
        joinType = other.joinType;
        direction = other.direction;
    }
};

class RecursiveJoin : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::RECURSIVE_JOIN;

public:
    RecursiveJoin(RecursiveJoinInfo info, std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::vector<DataPos>& vectorsToScanPos, std::vector<ft_col_idx_t>& colIndicesToScan,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<PhysicalOperator> recursiveRoot, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)},
          recursiveRoot{std::move(recursiveRoot)}, vectorsToScanPos{vectorsToScanPos},
          colIndicesToScan{colIndicesToScan} {}

    std::vector<NodeSemiMask*> getSemiMask() const;

    inline RecursiveJoinSharedState* getSharedState() const { return sharedState.get(); }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<RecursiveJoin>(info.copy(), sharedState, vectorsToScanPos,
            colIndicesToScan, children[0]->clone(), id, recursiveRoot->clone(), printInfo->copy());
    }

private:
    void initLocalRecursivePlan(ExecutionContext* context);

    void populateTargetDstNodes(ExecutionContext* context);

    bool scanOutput();

    bool computeBFS(ExecutionContext* context);

    bool doBFSnThreadkMorsel(ExecutionContext* context);

    bool doBFSnThreadkMorselAdaptive(ExecutionContext* context);

    void computeBFSnThreadkMorsel(ExecutionContext* context);

    // Compute BFS for a given src node.
    void computeBFSOneThreadOneMorsel(ExecutionContext* context);

    void updateVisitedNodes(common::nodeID_t boundNodeID);

private:
    RecursiveJoinInfo info;
    std::shared_ptr<RecursiveJoinSharedState> sharedState;

    // Local recursive plan
    std::unique_ptr<graph::NbrScanState> nbrScanState;

    std::unique_ptr<ResultSet> localResultSet;
    std::unique_ptr<PhysicalOperator> recursiveRoot;
    OffsetScanNodeTable* recursiveSource;

    std::unique_ptr<RecursiveJoinVectors> vectors;
    std::unique_ptr<FrontiersScanner> frontiersScanner;
    std::unique_ptr<TargetDstNodes> targetDstNodes;

    /// NEW MEMBERS BEING ADDED
    std::vector<DataPos> vectorsToScanPos;
    std::vector<common::ValueVector*> vectorsToScan;
    std::vector<ft_col_idx_t> colIndicesToScan;
    std::unique_ptr<BaseBFSState> bfsState;
};

} // namespace processor
} // namespace kuzu
