#pragma once

#include "common/types/types.h"
#include "processor/result/factorized_table.h"
#include "processor/result/flat_tuple.h"
#include "query_summary.h"

namespace kuzu {
namespace main {

struct DataTypeInfo {
public:
    DataTypeInfo(common::DataTypeID typeID, std::string name)
        : typeID{typeID}, name{std::move(name)} {}

    common::DataTypeID typeID;
    std::string name;
    std::vector<std::unique_ptr<DataTypeInfo>> childrenTypesInfo;

    static std::unique_ptr<DataTypeInfo> getInfoForDataType(
        const common::DataType& type, const std::string& name);
};

/**
 * @brief QueryResult stores the result of a query execution.
 */
class QueryResult {
    friend class Connection;

public:
    QueryResult() = default;
    /**
     * @brief Creates a QueryResult object.
     * @param preparedSummary stores compiling time and query options.
     */
    explicit QueryResult(const PreparedSummary& preparedSummary) {
        querySummary = std::make_unique<QuerySummary>();
        querySummary->setPreparedSummary(preparedSummary);
    }

    /**
     * @return query is executed successfully or not.
     */
    inline bool isSuccess() const { return success; }

    /**
     * @return error message of the query execution if the query fails.
     */
    inline std::string getErrorMessage() const { return errMsg; }

    /**
     * @return number of columns in query result.
     */
    inline size_t getNumColumns() const { return columnDataTypes.size(); }

    /**
     * @return name of each column in query result.
     */
    inline std::vector<std::string> getColumnNames() { return columnNames; }

    /**
     * @return dataType of each column in query result.
     */
    inline std::vector<common::DataType> getColumnDataTypes() { return columnDataTypes; }

    /**
     * @return num of tuples in query result.
     */
    inline uint64_t getNumTuples() {
        return querySummary->getIsExplain() ? 0 : factorizedTable->getTotalNumFlatTuples();
    }

    /**
     * @return query summary which stores the execution time, compiling time, plan and query
     * options.
     */
    inline QuerySummary* getQuerySummary() const { return querySummary.get(); }

    /**
     * @return dataTypeInfo of each column.
     */
    std::vector<std::unique_ptr<DataTypeInfo>> getColumnTypesInfo();

    /**
     * @brief initializes the result table and iterator.
     */
    void initResultTableAndIterator(std::shared_ptr<processor::FactorizedTable> factorizedTable_,
        const binder::expression_vector& columns,
        const std::vector<binder::expression_vector>& expressionToCollectPerColumn);

    /**
     * @return whether there are more tuples to read.
     */
    bool hasNext();

    /**
     * @return next flat tuple in the query result.
     */
    std::shared_ptr<processor::FlatTuple> getNext();

    /**
     * @brief writes the query result to a csv file.
     * @param fileName name of the csv file.
     * @param delimiter delimiter of the csv file.
     * @param escapeCharacter escape character of the csv file.
     * @param newline newline character of the csv file.
     */
    void writeToCSV(const std::string& fileName, char delimiter = ',', char escapeCharacter = '"',
        char newline = '\n');

    // TODO: interfaces below should be removed
    // used in shell to walk the result twice (first time getting maximum column width)
    inline void resetIterator() { iterator->resetState(); }

private:
    void validateQuerySucceed();

private:
    // execution status
    bool success = true;
    std::string errMsg;

    // header information
    std::vector<std::string> columnNames;
    std::vector<common::DataType> columnDataTypes;
    // data
    std::shared_ptr<processor::FactorizedTable> factorizedTable;
    std::unique_ptr<processor::FlatTupleIterator> iterator;
    std::shared_ptr<processor::FlatTuple> tuple;

    // execution statistics
    std::unique_ptr<QuerySummary> querySummary;
};

} // namespace main
} // namespace kuzu
