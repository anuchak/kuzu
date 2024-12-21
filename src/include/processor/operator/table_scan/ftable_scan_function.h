#pragma once

#include "function/function.h"
#include "function/table/bind_data.h"
#include "processor/result/factorized_table.h"
#include <function/table/scan_functions.h>

namespace kuzu {
namespace processor {

struct FTableScanMorsel {
    uint64_t startTupleIdx;
    uint64_t numTuples;

    FTableScanMorsel(uint64_t startTupleIdx, uint64_t numTuples)
        : startTupleIdx{startTupleIdx}, numTuples{numTuples} {}
};

struct FTableScanSharedState final : public function::BaseScanSharedState {
    std::shared_ptr<FactorizedTable> table;
    uint64_t morselSize;
    common::offset_t nextTupleIdx;

    FTableScanSharedState(std::shared_ptr<FactorizedTable> table, uint64_t morselSize)
        : BaseScanSharedState{}, table{std::move(table)}, morselSize{morselSize}, nextTupleIdx{0} {}

    FTableScanMorsel getMorsel() {
        std::unique_lock lck{lock};
        auto numTuplesToScan = std::min(morselSize, table->getNumTuples() - nextTupleIdx);
        auto morsel = FTableScanMorsel(nextTupleIdx, numTuplesToScan);
        nextTupleIdx += numTuplesToScan;
        return morsel;
    }

    uint64_t getNumRows() const override {
        KU_ASSERT(table->getNumTuples() == table->getTotalNumFlatTuples());
        return table->getNumTuples();
    }
};

struct FTableScanBindData : public function::TableFuncBindData {
    std::shared_ptr<FactorizedTable> table;
    std::vector<ft_col_idx_t> columnIndices;
    uint64_t morselSize;

    FTableScanBindData(std::shared_ptr<FactorizedTable> table,
        std::vector<ft_col_idx_t> columnIndices, uint64_t morselSize)
        : table{std::move(table)}, columnIndices{std::move(columnIndices)}, morselSize{morselSize} {
    }
    FTableScanBindData(const FTableScanBindData& other)
        : function::TableFuncBindData{other}, table{other.table},
          columnIndices{other.columnIndices}, morselSize{other.morselSize} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<FTableScanBindData>(*this);
    }
};

struct FTableScan {
    static constexpr const char* name = "READ_FTABLE";

    static function::function_set getFunctionSet();
};

} // namespace processor
} // namespace kuzu
