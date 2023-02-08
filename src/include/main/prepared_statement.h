#pragma once

#include "query_summary.h"

namespace kuzu::testing {
class TestHelper;
}

namespace kuzu::transaction {
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
} // namespace kuzu::transaction

namespace kuzu {
namespace main {

/**
 * @brief A prepared statement is a parameterized query which can avoid planning the same query for
 * repeated execution. Parameters are indicated through dollar symbol $ and are injected as a
 * std::pair where the first entry is parameter name and second entry is parameter value.
 */
class PreparedStatement {
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::testing::TestHelper;
    friend class kuzu::transaction::TinySnbDDLTest;
    friend class kuzu::transaction::TinySnbCopyCSVTransactionTest;

public:
    /**
     * @brief DDL and COPY_CSV statements are automatically wrapped in a transaction and committed.
     * As such, they cannot be part of an active transaction.
     * @return The prepared statement is allowed to be part of an active transaction.
     */
    inline bool allowActiveTransaction() const {
        return !common::StatementTypeUtils::isDDLOrCopyCSV(statementType);
    }

    /**
     * @brief Checks whether the query has been prepared successfully.
     * @return The query is prepared successfully or not.
     */
    inline bool isSuccess() const { return success; }

    /**
     * @brief Gets the error message if the query is not prepared successfully.
     * @return The error message if the query is not prepared successfully.
     */
    inline std::string getErrorMessage() const { return errMsg; }

    /**
     * @brief Checks whether the query is read-only.
     * @return The prepared statement is read-only or not.
     */
    inline bool isReadOnly() const { return readOnly; }

    /**
     * @brief Gets the expressions for generating query results.
     * @return Expression for generating query results.
     */
    inline binder::expression_vector getExpressionsToCollect() {
        return statementResult->getExpressionsToCollect();
    }

private:
    common::StatementType statementType;
    bool success = true;
    bool readOnly = false;
    std::string errMsg;
    PreparedSummary preparedSummary;
    std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
    std::unique_ptr<binder::BoundStatementResult> statementResult;
    std::vector<std::unique_ptr<planner::LogicalPlan>> logicalPlans;
};

} // namespace main
} // namespace kuzu
