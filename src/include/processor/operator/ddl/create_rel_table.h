#pragma once

#include "processor/operator/ddl/create_table.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace processor {

class CreateRelTable : public CreateTable {
public:
    CreateRelTable(Catalog* catalog, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes, RelMultiplicity relMultiplicity,
        table_id_t srcTableID, table_id_t dstTableID, const DataPos& outputPos, uint32_t id,
        const string& paramsString, RelsStatistics* relsStatistics)
        : CreateTable{PhysicalOperatorType::CREATE_REL_TABLE, catalog, std::move(tableName),
              std::move(propertyNameDataTypes), outputPos, id, paramsString},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          relsStatistics{relsStatistics} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTable>(catalog, tableName, propertyNameDataTypes,
            relMultiplicity, srcTableID, dstTableID, outputPos, id, paramsString, relsStatistics);
    }

private:
    RelMultiplicity relMultiplicity;
    table_id_t srcTableID;
    table_id_t dstTableID;
    RelsStatistics* relsStatistics;
};

} // namespace processor
} // namespace kuzu
