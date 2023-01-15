#pragma once

#include "add_property.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class AddRelProperty;

using fill_in_mem_lists_function_t = std::function<void(AddRelProperty*, InMemLists* inMemLists,
    uint8_t* defaultVal, PageByteCursor& pageByteCursor, node_offset_t nodeOffset,
    list_header_t header, uint64_t posInList)>;
using create_property_file_function_t = std::function<void(AddRelProperty*, table_id_t boundTableID,
    RelDirection direction, property_id_t propertyID, RelStatistics* relStatistics)>;

class AddRelProperty : public AddProperty {
public:
    AddRelProperty(Catalog* catalog, table_id_t tableID, string propertyName, DataType dataType,
        unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator,
        StorageManager& storageManager, const DataPos& outputPos, uint32_t id,
        const string& paramsString)
        : AddProperty(catalog, tableID, std::move(propertyName), std::move(dataType),
              std::move(expressionEvaluator), storageManager, outputPos, id, paramsString) {}

    void executeDDLInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AddRelProperty>(catalog, tableID, propertyName, dataType,
            expressionEvaluator->clone(), storageManager, outputPos, id, paramsString);
    }

private:
    inline void fillInMemListsWithNonOverflowValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, node_offset_t nodeOffset, list_header_t header,
        uint64_t posInList) {
        inMemLists->setElement(header, nodeOffset, posInList, defaultVal);
    }

    void createFileForColumnProperty(table_id_t boundTableID, RelDirection direction,
        property_id_t propertyID, RelStatistics* relStatistics);

    void createFileForListsProperty(table_id_t boundTableID, RelDirection direction,
        property_id_t propertyID, RelStatistics* relStatistics);

    void fillInMemListsWithStrValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, node_offset_t nodeOffset, list_header_t header,
        uint64_t posInList);

    void fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
        PageByteCursor& pageByteCursor, node_offset_t nodeOffset, list_header_t header,
        uint64_t posInList);

    fill_in_mem_lists_function_t getFillInMemListsFunc();

    void fillInMemListWithDefaultVal(InMemLists* inMemLists, uint8_t* defaultVal,
        node_offset_t maxNodeOffset, AdjLists* adjList);
};

} // namespace processor
} // namespace kuzu
