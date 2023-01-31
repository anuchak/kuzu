#pragma once

#include "bound_create_table.h"

namespace kuzu {
namespace binder {

class BoundCreateRelClause : public BoundCreateTable {
public:
    BoundCreateRelClause(string tableName, vector<PropertyNameDataType> propertyNameDataTypes,
        RelMultiplicity relMultiplicity, table_id_t srcTableID, table_id_t dstTableID)
        : BoundCreateTable{StatementType::CREATE_REL_CLAUSE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}

    inline RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline table_id_t getSrcTableID() const { return srcTableID; }

    inline table_id_t getDstTableID() const { return dstTableID; }

private:
    RelMultiplicity relMultiplicity;
    table_id_t srcTableID;
    table_id_t dstTableID;
};

} // namespace binder
} // namespace kuzu
