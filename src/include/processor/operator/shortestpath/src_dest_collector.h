#pragma once

#include <utility>

#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct BFSScanMorsel {
public:
    BFSScanMorsel(
        std::shared_ptr<FactorizedTable> table, uint64_t startTupleIdx, uint64_t numTuples)
        : table{std::move(table)}, startTupleIdx{startTupleIdx}, numTuples{numTuples} {}

    inline uint64_t getNumTuples() const { return numTuples; }

    inline uint64_t getStartIdx() const { return startTupleIdx; }

    std::shared_ptr<FactorizedTable> getFTable() { return table; }

private:
    std::shared_ptr<FactorizedTable> table;
    uint64_t startTupleIdx;
    uint64_t numTuples;
};

class BFSFTableSharedState {
public:
    void initTableIfNecessary(
        storage::MemoryManager* memoryManager, std::unique_ptr<FactorizedTableSchema> tableSchema);

    void mergeLocalTable(FactorizedTable& localTable);

    inline std::shared_ptr<FactorizedTable> getTable() { return table; }

    inline uint64_t getMaxMorselSize() {
        std::lock_guard<std::mutex> lck{mtx};
        return table->hasUnflatCol() ? 1 : common::DEFAULT_VECTOR_CAPACITY;
    }
    std::unique_ptr<BFSScanMorsel> getMorsel(uint64_t maxMorselSize);

private:
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> table;
    uint64_t nextTupleIdxToScan = 0u;
};

class SrcDestCollector : public Sink {
public:
    SrcDestCollector(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::pair<DataPos, common::DataType> srcPosAndType,
        std::pair<DataPos, common::DataType> destPosAndType,
        std::vector<std::pair<DataPos, common::DataType>> payloadsPosAndType,
        std::vector<bool> isPayloadFlat, std::shared_ptr<BFSFTableSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::SRC_DEST_COLLECTOR,
              std::move(child), id, paramsString},
          srcPosAndType{std::move(srcPosAndType)}, destPosAndType{std::move(destPosAndType)},
          payloadsPosAndType{std::move(payloadsPosAndType)},
          isPayloadFlat{std::move(isPayloadFlat)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SrcDestCollector>(resultSetDescriptor->copy(), srcPosAndType,
            destPosAndType, payloadsPosAndType, isPayloadFlat, sharedState, children[0]->clone(),
            id, paramsString);
    }

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

    std::unique_ptr<FactorizedTableSchema> populateTableSchema();

private:
    std::pair<DataPos, common::DataType> srcPosAndType;
    std::pair<DataPos, common::DataType> destPosAndType;
    std::vector<std::pair<DataPos, common::DataType>> payloadsPosAndType;
    std::vector<bool> isPayloadFlat;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToCollect;
    std::shared_ptr<BFSFTableSharedState> sharedState;
    std::unique_ptr<FactorizedTable> localTable;
};

} // namespace processor
} // namespace kuzu