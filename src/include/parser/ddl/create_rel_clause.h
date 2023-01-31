#pragma once

#include "parser/ddl/create_table.h"

namespace kuzu {
namespace parser {

using namespace std;

class CreateRelClause : public CreateTable {
public:
    CreateRelClause(string tableName, vector<pair<string, string>> propertyNameDataTypes,
        string relMultiplicity, string srcTableName, string dstTableName)
        : CreateTable{StatementType::CREATE_REL_CLAUSE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          relMultiplicity{std::move(relMultiplicity)}, srcTableName{std::move(srcTableName)},
          dstTableName{std::move(dstTableName)} {}

    inline string getRelMultiplicity() const { return relMultiplicity; }

    inline string getSrcTableName() const { return srcTableName; }

    inline string getDstTableName() const { return dstTableName; }

private:
    string relMultiplicity;
    string srcTableName;
    string dstTableName;
};

} // namespace parser
} // namespace kuzu
