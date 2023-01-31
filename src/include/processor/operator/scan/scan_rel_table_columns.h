#pragma once

#include "processor/operator/filtering_operator.h"
#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

class ScanRelTableColumns : public ScanRelTable, public SelVectorOverWriter {
public:
    ScanRelTableColumns(DirectedRelTableData* tableData, vector<uint32_t> propertyIds,
        const DataPos& inNodeIDVectorPos, vector<DataPos> outputVectorsPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : ScanRelTable{inNodeIDVectorPos, std::move(outputVectorsPos),
              PhysicalOperatorType::SCAN_REL_TABLE_COLUMNS, std::move(child), id, paramsString},
          tableData{tableData} {
        scanState =
            make_unique<RelTableScanState>(std::move(propertyIds), RelTableDataType::COLUMNS);
    }

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanRelTableColumns>(tableData, scanState->propertyIds,
            inNodeIDVectorPos, outputVectorsPos, children[0]->clone(), id, paramsString);
    }

private:
    DirectedRelTableData* tableData;
    unique_ptr<RelTableScanState> scanState;
};

} // namespace processor
} // namespace kuzu
