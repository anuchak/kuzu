#pragma once

#include "ddl.h"
#include "expression_evaluator/base_evaluator.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class AddProperty : public DDL {
public:
    AddProperty(Catalog* catalog, table_id_t tableID, string propertyName, DataType dataType,
        unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator,
        StorageManager& storageManager, const DataPos& outputPos, uint32_t id,
        const string& paramsString)
        : DDL{PhysicalOperatorType::DROP_PROPERTY, catalog, outputPos, id, paramsString},
          tableID{tableID}, propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          expressionEvaluator{std::move(expressionEvaluator)}, storageManager{storageManager} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        DDL::initLocalStateInternal(resultSet, context);
        expressionEvaluator->init(*resultSet, context->memoryManager);
    }

    void executeDDLInternal() override;

    uint8_t* getDefaultVal();

    string getOutputMsg() override { return {"Add Succeed."}; }

protected:
    inline bool isDefaultValueNull() const {
        auto expressionVector = expressionEvaluator->resultVector;
        return expressionVector->isNull(expressionVector->state->selVector->selectedPositions[0]);
    }

protected:
    table_id_t tableID;
    string propertyName;
    DataType dataType;
    unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator;
    StorageManager& storageManager;
};

} // namespace processor
} // namespace kuzu
