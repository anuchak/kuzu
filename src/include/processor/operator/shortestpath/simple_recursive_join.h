#pragma once

#include <utility>

#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"
#include "scan_bfs_level.h"

namespace kuzu {
namespace processor {

/*
 * The SimpleRecursiveJoin class reads from the inputNodeIDVector which holds the nodes written by
 * ScanRelTableList after extending nodes in a BFSLevel. ScanBFSLevel -> ScanRelTableLists ->
 * SimpleRecursiveJoin returns BFS level distances for a single src + multiple dst's at a
 * time. Different threads will be assigned sets of single src + multiple dst computations. If
 * there is no path from a single src to another dst then that dst is unreachable and not part of
 * the final output. If no dst is reachable from a src then that src is also discarded from the
 * final output i.e no distance is reported for it since no dst is reachable.
 */
class SimpleRecursiveJoin : public PhysicalOperator {

public:
    SimpleRecursiveJoin(uint8_t bfsLowerBound, uint8_t bfsUpperBound,
        std::shared_ptr<SimpleRecursiveJoinGlobalState>& simpleRecursiveJoinSharedState,
        const DataPos& nodeIDVectorDataPos, std::vector<DataPos> srcDstNodeIDDataPos,
        const DataPos& dstDistanceVectorDataPos, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator(
              PhysicalOperatorType::SIMPLE_RECURSIVE_JOIN, std::move(child), id, paramsString),
          bfsLowerBound{bfsLowerBound}, bfsUpperBound{bfsUpperBound},
          inputNodeIDDataPos{nodeIDVectorDataPos},
          simpleRecursiveJoinGlobalState{simpleRecursiveJoinSharedState},
          srcDstNodeIDDataPos{std::move(srcDstNodeIDDataPos)}, dstDistanceVectorDataPos{
                                                                   dstDistanceVectorDataPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    bool writeDistToOutputVector(SSSPMorsel* ssspMorsel);

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SimpleRecursiveJoin>(bfsLowerBound, bfsUpperBound,
            simpleRecursiveJoinGlobalState, inputNodeIDDataPos, srcDstNodeIDDataPos,
            dstDistanceVectorDataPos, children[0]->clone(), id, paramsString);
    }

private:
    uint8_t bfsLowerBound;
    uint8_t bfsUpperBound;
    std::thread::id threadID;
    DataPos inputNodeIDDataPos;
    std::shared_ptr<common::ValueVector> inputNodeIDVector;
    std::vector<DataPos> srcDstNodeIDDataPos;
    std::vector<std::shared_ptr<common::ValueVector>> srcDstNodeIDValueVectors;
    DataPos dstDistanceVectorDataPos;
    std::shared_ptr<common::ValueVector> dstDistances;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};

} // namespace processor
} // namespace kuzu
