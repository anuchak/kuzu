#pragma once

#include "base_logical_operator.h"
#include "logical_create_table.h"

namespace kuzu {
namespace planner {

class LogicalCreateRelTable : public LogicalCreateTable {
public:
    LogicalCreateRelTable(string tableName, vector<PropertyNameDataType> propertyNameDataTypes,
        RelMultiplicity relMultiplicity, table_id_t srcTableID, table_id_t dstTableID,
        shared_ptr<Expression> outputExpression)
        : LogicalCreateTable{LogicalOperatorType::CREATE_REL_TABLE, std::move(tableName),
              std::move(propertyNameDataTypes), std::move(outputExpression)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}

    inline RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline table_id_t getSrcTableID() const { return srcTableID; }

    inline table_id_t getDstTableID() const { return dstTableID; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRelTable>(tableName, propertyNameDataTypes, relMultiplicity,
            srcTableID, dstTableID, outputExpression);
    }

private:
    RelMultiplicity relMultiplicity;
    table_id_t srcTableID;
    table_id_t dstTableID;
};

} // namespace planner
} // namespace kuzu
