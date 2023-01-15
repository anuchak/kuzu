#include "processor/operator/ddl/add_property.h"

#include "storage/in_mem_storage_structure/in_mem_column.h"

namespace kuzu {
namespace processor {

void AddProperty::executeDDLInternal() {
    expressionEvaluator->evaluate();
    catalog->addProperty(tableID, propertyName, dataType);
}

uint8_t* AddProperty::getDefaultVal() {
    auto expressionVector = expressionEvaluator->resultVector;
    assert(expressionEvaluator->resultVector->dataType == dataType);
    auto posInExpressionVector = expressionVector->state->selVector->selectedPositions[0];
    return expressionVector->getData() +
           expressionVector->getNumBytesPerValue() * posInExpressionVector;
}

void AddProperty::fillInMemColumnWithStrValFunc(InMemColumn* inMemColumn, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, node_offset_t nodeOffset) {
    auto strVal = *reinterpret_cast<ku_string_t*>(defaultVal);
    if (strVal.len > ku_string_t::SHORT_STR_LENGTH) {
        inMemColumn->getInMemOverflowFile()->copyStringOverflow(
            pageByteCursor, reinterpret_cast<uint8_t*>(strVal.overflowPtr), &strVal);
    }
    inMemColumn->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&strVal));
}

void AddProperty::fillInMemColumnWithListValFunc(InMemColumn* inMemColumn, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, node_offset_t nodeOffset) {
    auto listVal = *reinterpret_cast<ku_list_t*>(defaultVal);
    inMemColumn->getInMemOverflowFile()->copyListOverflow(
        pageByteCursor, &listVal, dataType.childType.get());
    inMemColumn->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&listVal));
}

fill_in_mem_column_function_t AddProperty::getFillInMemColumnFunc() {
    switch (dataType.typeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        return &AddProperty::fillInMemColumnWithNonOverflowValFunc;
    }
    case STRING: {
        return &AddProperty::fillInMemColumnWithStrValFunc;
    }
    case LIST: {
        return &AddProperty::fillInMemColumnWithListValFunc;
    }
    default: {
        assert(false);
    }
    }
}

void AddProperty::fillInMemColumWithDefaultVal(
    InMemColumn* inMemColumn, uint8_t* defaultVal, node_offset_t maxNodeOffset) {
    if (maxNodeOffset != UINT64_MAX) {
        PageByteCursor pageByteCursor{};
        auto fillInMemColumnOP = getFillInMemColumnFunc();
        for (node_offset_t nodeOffset = 0; nodeOffset <= maxNodeOffset; nodeOffset++) {
            fillInMemColumnOP(this, inMemColumn, defaultVal, pageByteCursor, nodeOffset);
        }
    }
}

} // namespace processor
} // namespace kuzu
