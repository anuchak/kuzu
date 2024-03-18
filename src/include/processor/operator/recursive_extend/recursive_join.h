#pragma once

#include <utility>

#include "bfs_scheduler.h"
#include "bfs_state.h"
#include "common/query_rel_type.h"
#include "frontier_scanner.h"
#include "planner/logical_plan/extend/recursive_join_type.h"
#include "processor/operator/csr_index_build.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

class ScanFrontier;

struct RecursiveJoinSharedState {
    std::shared_ptr<MorselDispatcher> morselDispatcher;
    std::shared_ptr<FactorizedTableScanSharedState> inputFTableSharedState;
    std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks;
    std::shared_ptr<csrIndexSharedState> csrSharedState;

    RecursiveJoinSharedState(std::shared_ptr<MorselDispatcher> morselDispatcher,
        std::shared_ptr<FactorizedTableScanSharedState> inputFTableSharedState,
        std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks)
        : morselDispatcher{std::move(morselDispatcher)},
          inputFTableSharedState{std::move(inputFTableSharedState)}, semiMasks{
                                                                         std::move(semiMasks)} {}

    inline common::SchedulerType getSchedulerType() { return morselDispatcher->getSchedulerType(); }

    inline std::pair<GlobalSSSPState, SSSPLocalState> getBFSMorsel(
        const std::vector<common::ValueVector*>& vectorsToScan,
        const std::vector<ft_col_idx_t>& colIndicesToScan, common::ValueVector* srcNodeIDVector,
        BaseBFSMorsel* bfsMorsel, common::QueryRelType queryRelType,
        planner::RecursiveJoinType recursiveJoinType) {
        return morselDispatcher->getBFSMorsel(inputFTableSharedState, vectorsToScan,
            colIndicesToScan, srcNodeIDVector, bfsMorsel, queryRelType, recursiveJoinType);
    }

    inline int64_t writeDstNodeIDAndPathLength(
        const std::vector<common::ValueVector*>& vectorsToScan,
        const std::vector<ft_col_idx_t>& colIndicesToScan, common::table_id_t tableID,
        std::unique_ptr<BaseBFSMorsel>& bfsMorsel, RecursiveJoinVectors* vectors) {
        return morselDispatcher->writeDstNodeIDAndPathLength(
            inputFTableSharedState, vectorsToScan, colIndicesToScan, tableID, bfsMorsel, vectors);
    }
};

struct RecursiveJoinDataInfo {
    // Join input info.
    DataPos srcNodePos;
    // Join output info.
    DataPos dstNodePos;
    std::unordered_set<common::table_id_t> dstNodeTableIDs;
    DataPos pathLengthPos;
    DataPos pathCostPos;
    // Recursive join info.
    /*std::unique_ptr<ResultSetDescriptor> localResultSetDescriptor;
    DataPos recursiveDstNodeIDPos;
    std::unordered_set<common::table_id_t> recursiveDstNodeTableIDs;
    DataPos recursiveEdgeIDPos;
    DataPos recursiveEdgePropertyPos;*/
    // Path info
    DataPos pathPos;

    RecursiveJoinDataInfo(const DataPos& srcNodePos, const DataPos& dstNodePos,
        std::unordered_set<common::table_id_t> dstNodeTableIDs, const DataPos& pathLengthPos,
        const DataPos& pathCostPos, /*std::unique_ptr<ResultSetDescriptor> localResultSetDescriptor,
        const DataPos& recursiveDstNodeIDPos,
        std::unordered_set<common::table_id_t> recursiveDstNodeTableIDs,
        const DataPos& recursiveEdgeIDPos, const DataPos& recursiveEdgePropertyPos,*/
        const DataPos& pathPos)
        : srcNodePos{srcNodePos}, dstNodePos{dstNodePos}, dstNodeTableIDs{std::move(
                                                              dstNodeTableIDs)},
          pathLengthPos{pathLengthPos}, pathCostPos{pathCostPos},
          /*localResultSetDescriptor{std::move(localResultSetDescriptor)},
recursiveDstNodeIDPos{recursiveDstNodeIDPos}, recursiveDstNodeTableIDs{std::move(
                                  recursiveDstNodeTableIDs)},
recursiveEdgeIDPos{recursiveEdgeIDPos},
recursiveEdgePropertyPos{recursiveEdgePropertyPos},*/
          pathPos{pathPos} {}

    inline std::unique_ptr<RecursiveJoinDataInfo> copy() {
        return std::make_unique<RecursiveJoinDataInfo>(srcNodePos, dstNodePos, dstNodeTableIDs,
            pathLengthPos, pathCostPos, /*localResultSetDescriptor->copy(), recursiveDstNodeIDPos,
            recursiveDstNodeTableIDs, recursiveEdgeIDPos, recursiveEdgePropertyPos,*/
            pathPos);
    }
};

class RecursiveJoin : public PhysicalOperator {
public:
    // Recursive Join child operators on one side (child[0]) and CSR Build Index on other (child[1])
    RecursiveJoin(uint8_t lowerBound, uint8_t upperBound, common::QueryRelType queryRelType,
        planner::RecursiveJoinType joinType, std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<RecursiveJoinDataInfo> dataInfo, std::vector<DataPos>& vectorsToScanPos,
        std::vector<ft_col_idx_t>& colIndicesToScan,
        std::unique_ptr<PhysicalOperator> recursiveJoinSide,
        std::unique_ptr<PhysicalOperator> csrBuildSide, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN, std::move(recursiveJoinSide),
              std::move(csrBuildSide), id, paramsString},
          lowerBound{lowerBound}, upperBound{upperBound}, queryRelType{queryRelType},
          joinType{joinType}, sharedState{std::move(sharedState)}, dataInfo{std::move(dataInfo)},
          vectorsToScanPos{vectorsToScanPos}, colIndicesToScan{colIndicesToScan} {}

    RecursiveJoin(uint8_t lowerBound, uint8_t upperBound, common::QueryRelType queryRelType,
        planner::RecursiveJoinType joinType, std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<RecursiveJoinDataInfo> dataInfo, std::vector<DataPos>& vectorsToScanPos,
        std::vector<ft_col_idx_t>& colIndicesToScan,
        std::unique_ptr<PhysicalOperator> recursiveJoinSide, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN, std::move(recursiveJoinSide), id,
              paramsString},
          lowerBound{lowerBound}, upperBound{upperBound}, queryRelType{queryRelType},
          joinType{joinType}, sharedState{std::move(sharedState)}, dataInfo{std::move(dataInfo)},
          vectorsToScanPos{vectorsToScanPos}, colIndicesToScan{colIndicesToScan} {}

    inline RecursiveJoinSharedState* getSharedState() const { return sharedState.get(); }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<RecursiveJoin>(lowerBound, upperBound, queryRelType, joinType,
            sharedState, dataInfo->copy(), vectorsToScanPos, colIndicesToScan, children[0]->clone(),
            id, paramsString);
    }

private:
    void initLocalRecursivePlan(ExecutionContext* context);

    void populateTargetDstNodes();

    bool scanOutput();

    bool computeBFS(ExecutionContext* context);

    bool doBFSnThreadkMorsel(ExecutionContext* context);

    void computeBFSnThreadkMorsel(ExecutionContext* context);

    // Compute BFS for a given src node.
    void computeBFSOneThreadOneMorsel(ExecutionContext* context);

    void updateVisitedNodes(common::nodeID_t boundNodeID);

private:
    uint8_t lowerBound;
    uint8_t upperBound;
    common::QueryRelType queryRelType;
    planner::RecursiveJoinType joinType;

    std::shared_ptr<RecursiveJoinSharedState> sharedState;
    std::unique_ptr<RecursiveJoinDataInfo> dataInfo;

    // Local recursive plan
    // THIS WILL NOT BE USED FOR CSR INDEX BUILD PoC
    /*std::unique_ptr<ResultSet> localResultSet;
    std::unique_ptr<PhysicalOperator> recursiveRoot;
    ScanFrontier* scanFrontier;*/

    std::unique_ptr<RecursiveJoinVectors> vectors;
    std::unique_ptr<FrontiersScanner> frontiersScanner;
    std::unique_ptr<TargetDstNodes> targetDstNodes;

    /// NEW MEMBERS BEING ADDED
    std::vector<DataPos> vectorsToScanPos;
    std::vector<common::ValueVector*> vectorsToScan;
    std::vector<ft_col_idx_t> colIndicesToScan;
    std::unique_ptr<BaseBFSMorsel> bfsMorsel;
};

} // namespace processor
} // namespace kuzu
