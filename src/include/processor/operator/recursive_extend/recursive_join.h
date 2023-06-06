#pragma once

#include "bfs_state.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/result_collector.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class ScanFrontier : public PhysicalOperator {
public:
    ScanFrontier(DataPos nodeIDVectorPos, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramsString},
          nodeIDVectorPos{nodeIDVectorPos} {}

    inline bool isSource() const override { return true; }

    inline void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override {
        nodeIDVector = resultSet->getValueVector(nodeIDVectorPos);
    }

    bool getNextTuplesInternal(kuzu::processor::ExecutionContext* context) override;

    void setNodeID(common::nodeID_t nodeID) {
        nodeIDVector->setValue<common::nodeID_t>(0, nodeID);
        hasExecuted = false;
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanFrontier>(nodeIDVectorPos, id, paramsString);
    }

private:
    DataPos nodeIDVectorPos;
    std::shared_ptr<common::ValueVector> nodeIDVector;
    bool hasExecuted;
};

struct RecursiveJoinSharedState {
    std::shared_ptr<FTableSharedState> inputFTableSharedState;
    std::unique_ptr<NodeOffsetSemiMask> semiMask;

    RecursiveJoinSharedState(
        std::shared_ptr<FTableSharedState> inputFTableSharedState, storage::NodeTable* nodeTable)
        : inputFTableSharedState{std::move(inputFTableSharedState)} {
        semiMask = std::make_unique<NodeOffsetSemiMask>(nodeTable);
    }
};

class BaseRecursiveJoin : public PhysicalOperator {
public:
    BaseRecursiveJoin(uint8_t lowerBound, uint8_t upperBound, storage::NodeTable* nodeTable,
        std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::vector<DataPos> vectorsToScanPos, std::vector<ft_col_idx_t> colIndicesToScan,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& distanceVectorPos, const DataPos& tmpDstNodeIDVectorPos,
        std::shared_ptr<MorselDispatcher> morselDispatcher, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString,
        std::unique_ptr<PhysicalOperator> recursiveRoot)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN, std::move(child), id,
              paramsString},
          lowerBound{lowerBound}, upperBound{upperBound}, nodeTable{nodeTable},
          sharedState{std::move(sharedState)}, vectorsToScanPos{std::move(vectorsToScanPos)},
          colIndicesToScan{std::move(colIndicesToScan)}, srcNodeIDVectorPos{srcNodeIDVectorPos},
          dstNodeIDVectorPos{dstNodeIDVectorPos}, distanceVectorPos{distanceVectorPos},
          tmpDstNodeIDVectorPos{tmpDstNodeIDVectorPos},
          morselDispatcher{std::move(morselDispatcher)}, recursiveRoot{std::move(recursiveRoot)} {}

    BaseRecursiveJoin(uint8_t lowerBound, uint8_t upperBound, storage::NodeTable* nodeTable,
        std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::vector<DataPos> vectorsToScanPos, std::vector<ft_col_idx_t> colIndicesToScan,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& distanceVectorPos, const DataPos& tmpDstNodeIDVectorPos,
        std::shared_ptr<MorselDispatcher> morselDispatcher, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> recursiveRoot)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN, id, paramsString},
          lowerBound{lowerBound}, upperBound{upperBound}, nodeTable{nodeTable},
          sharedState{std::move(sharedState)}, vectorsToScanPos{std::move(vectorsToScanPos)},
          colIndicesToScan{std::move(colIndicesToScan)}, srcNodeIDVectorPos{srcNodeIDVectorPos},
          dstNodeIDVectorPos{dstNodeIDVectorPos}, distanceVectorPos{distanceVectorPos},
          tmpDstNodeIDVectorPos{tmpDstNodeIDVectorPos},
          morselDispatcher{std::move(morselDispatcher)}, recursiveRoot{std::move(recursiveRoot)} {}

    virtual ~BaseRecursiveJoin() = default;

    static inline DataPos getTmpSrcNodeVectorPos() { return DataPos{0, 0}; }

    inline NodeSemiMask* getSemiMask() const { return sharedState->semiMask.get(); }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override = 0;

private:
    std::unique_ptr<ResultSet> getLocalResultSet();

    void initLocalRecursivePlan(ExecutionContext* context);

    virtual bool scanOutput() = 0;

    // Compute BFS for a given src node.
    bool computeBFS(ExecutionContext* context);

    int fetchBFSMorselFromDispatcher(ExecutionContext* context);

    void extend(ExecutionContext* context);

protected:
    uint32_t threadIdx;
    uint8_t lowerBound;
    uint8_t upperBound;
    storage::NodeTable* nodeTable;
    std::shared_ptr<RecursiveJoinSharedState> sharedState;
    std::vector<DataPos> vectorsToScanPos;
    std::vector<ft_col_idx_t> colIndicesToScan;
    DataPos srcNodeIDVectorPos;
    DataPos dstNodeIDVectorPos;
    DataPos distanceVectorPos;
    DataPos tmpDstNodeIDVectorPos;

    // Local recursive plan
    std::unique_ptr<ResultSet> localResultSet;
    std::unique_ptr<PhysicalOperator> recursiveRoot;
    ScanFrontier* scanFrontier;

    // Vectors
    std::vector<common::ValueVector*> vectorsToScan;
    std::shared_ptr<common::ValueVector> srcNodeIDVector;
    std::shared_ptr<common::ValueVector> dstNodeIDVector;
    std::shared_ptr<common::ValueVector> distanceVector;
    std::shared_ptr<common::ValueVector> tmpDstNodeIDVector; // temporary recursive join result.

    std::unique_ptr<BaseBFSMorsel> bfsMorsel;
    std::shared_ptr<MorselDispatcher> morselDispatcher;
};

} // namespace processor
} // namespace kuzu
