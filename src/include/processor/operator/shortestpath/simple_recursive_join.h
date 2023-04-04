#pragma once

#include <utility>

#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"
#include "scan_bfs_level.h"

namespace kuzu {
namespace processor {

/*
 * The SimpleRecursiveJoin class reads from the inputIDVector which holds the nodes written by
 * ScanRelTableList after extending nodes in a BFSLevel. ScanBFSLevel -> ScanRelTableLists ->
 * SimpleRecursiveJoin returns BFS level distances for a single src + multiple dst's at a
 * time. Different threads will be assigned sets of single src + multiple dst computations. If
 * there is no path from a single src to another dst then that dst is unreachable and not part of
 * the final output. If no dst is reachable from a src then that src is also discarded from the
 * final output i.e no distance is reported for it since no dst is reachable.
 */
class SimpleRecursiveJoin : public Sink {

public:
    SimpleRecursiveJoin(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        uint8_t lowerBound, uint8_t upperBound, const DataPos& dstIDPos,
        std::shared_ptr<SimpleRecursiveJoinGlobalState>& simpleRecursiveJoinSharedState,
        const DataPos& inputIDPos, const DataPos& dstDistancesPos,
        std::shared_ptr<FTableSharedState> sharedState,
        std::vector<std::pair<DataPos, common::DataType>>& payloadsPosAndType,
        std::vector<bool>& isPayloadFlat, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : Sink(std::move(resultSetDescriptor), PhysicalOperatorType::SIMPLE_RECURSIVE_JOIN,
              std::move(child), id, paramsString),
          lowerBound{lowerBound}, upperBound{upperBound}, dstIDPos{dstIDPos},
          isPayloadFlat{isPayloadFlat}, inputIDPos{inputIDPos}, sharedState{std::move(sharedState)},
          payloadsPosAndType{payloadsPosAndType}, dstDistancesPos{dstDistancesPos},
          simpleRecursiveJoinGlobalState{simpleRecursiveJoinSharedState} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    uint16_t writeDistToOutputVector(SSSPMorsel* ssspMorsel);

    void executeInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SimpleRecursiveJoin>(resultSetDescriptor->copy(), lowerBound,
            upperBound, dstIDPos, simpleRecursiveJoinGlobalState, inputIDPos, dstDistancesPos,
            sharedState, payloadsPosAndType, isPayloadFlat, children[0]->clone(), id, paramsString);
    }

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

    std::unique_ptr<FactorizedTableSchema> populateTableSchema();

private:
    uint8_t lowerBound;
    uint8_t upperBound;
    std::thread::id threadID;
    DataPos dstIDPos;
    DataPos dstDistancesPos;
    DataPos inputIDPos;
    std::shared_ptr<common::ValueVector> inputIDVector;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
    std::shared_ptr<FTableSharedState> sharedState;
    std::unique_ptr<FactorizedTable> localFTable;
    std::vector<std::pair<DataPos, common::DataType>> payloadsPosAndType;
    std::vector<bool> isPayloadFlat;
    std::vector<common::ValueVector*> vectorsToCollect;
};

} // namespace processor
} // namespace kuzu
