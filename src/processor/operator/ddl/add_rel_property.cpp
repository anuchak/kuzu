#include "processor/operator/ddl/add_rel_property.h"

namespace kuzu {
namespace processor {

void AddRelProperty::executeDDLInternal() {
    AddProperty::executeDDLInternal();
    auto tableSchema = catalog->getWriteVersion()->getRelTableSchema(tableID);
    auto propertyID = tableSchema->getPropertyID(propertyName);
    auto property = tableSchema->getProperty(propertyID);
    auto relStatistics =
        storageManager.getRelsStore().getRelsStatistics().getRelStatistics(tableID);
    for (auto direction : REL_DIRECTIONS) {
        create_property_file_function_t createPropertyFileFunc =
            tableSchema->isSingleMultiplicityInDirection(direction) ?
                &AddRelProperty::createFileForColumnProperty :
                &AddRelProperty::createFileForListsProperty;
        for (auto boundTableID : tableSchema->getUniqueBoundTableIDs(direction)) {
            createPropertyFileFunc(this, boundTableID, direction, propertyID, relStatistics);
        }
    }
    storageManager.getRelsStore().getRelTable(tableID)->addProperty(property, tableSchema);
}

void AddRelProperty::createFileForColumnProperty(table_id_t boundTableID, RelDirection direction,
    property_id_t propertyID, RelStatistics* relStatistics) {
    auto inMemColumn = InMemColumnFactory::getInMemPropertyColumn(
        StorageUtils::getRelPropertyColumnFName(storageManager.getDirectory(), tableID,
            boundTableID, direction, propertyID, DBFileType::ORIGINAL),
        dataType, relStatistics->getNumTuples());
    if (!isDefaultValueNull()) {
        auto nodeStatistics = storageManager.getNodesStore()
                                  .getNodesStatisticsAndDeletedIDs()
                                  .getNodeStatisticsAndDeletedIDs(boundTableID);
        fillInMemColumWithDefaultVal(
            inMemColumn.get(), getDefaultVal(), nodeStatistics->getMaxNodeOffset());
    }
    inMemColumn->saveToFile();
}

void AddRelProperty::createFileForListsProperty(table_id_t boundTableID, RelDirection direction,
    property_id_t propertyID, RelStatistics* relStatistics) {
    auto inMemList = InMemListsFactory::getInMemPropertyLists(
        StorageUtils::getRelPropertyListsFName(storageManager.getDirectory(), tableID, boundTableID,
            direction, propertyID, DBFileType::ORIGINAL),
        dataType, relStatistics->getNumTuples());
    auto boundNodeStatistics = storageManager.getNodesStore()
                                   .getNodesStatisticsAndDeletedIDs()
                                   .getNodeStatisticsAndDeletedIDs(boundTableID);
    // Note: we need the listMetadata to get the num of elements in a large list, and headers to
    // get the num of elements in a small list as well as determine whether a list is large or
    // small. All property lists share the same listHeader which is stored in the adjList.
    auto adjLists = storageManager.getRelsStore().getAdjLists(direction, boundTableID, tableID);
    inMemList->initListsMetadataAndAllocatePages(boundNodeStatistics->getNumTuples(),
        adjLists->getHeaders().get(), &adjLists->getListsMetadata());
    if (!isDefaultValueNull()) {
        fillInMemListWithDefaultVal(inMemList.get(), getDefaultVal(),
            boundNodeStatistics->getMaxNodeOffset(),
            storageManager.getRelsStore().getAdjLists(direction, boundTableID, tableID));
    }
    inMemList->saveToFile();
}

void AddRelProperty::fillInMemListsWithStrValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, node_offset_t nodeOffset, list_header_t header,
    uint64_t posInList) {
    auto strVal = *(ku_string_t*)defaultVal;
    inMemLists->getInMemOverflowFile()->copyStringOverflow(
        pageByteCursor, reinterpret_cast<uint8_t*>(strVal.overflowPtr), &strVal);
    inMemLists->setElement(header, nodeOffset, posInList, reinterpret_cast<uint8_t*>(&strVal));
}

void AddRelProperty::fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, node_offset_t nodeOffset, list_header_t header,
    uint64_t posInList) {
    auto listVal = *reinterpret_cast<ku_list_t*>(defaultVal);
    inMemLists->getInMemOverflowFile()->copyListOverflow(
        pageByteCursor, &listVal, dataType.childType.get());
    inMemLists->setElement(header, nodeOffset, posInList, reinterpret_cast<uint8_t*>(&listVal));
}

fill_in_mem_lists_function_t AddRelProperty::getFillInMemListsFunc() {
    switch (dataType.typeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        return &AddRelProperty::fillInMemListsWithNonOverflowValFunc;
    }
    case STRING: {
        return &AddRelProperty::fillInMemListsWithStrValFunc;
    }
    case LIST: {
        return &AddRelProperty::fillInMemListsWithListValFunc;
    }
    default: {
        assert(false);
    }
    }
}

void AddRelProperty::fillInMemListWithDefaultVal(
    InMemLists* inMemLists, uint8_t* defaultVal, node_offset_t maxNodeOffset, AdjLists* adjList) {
    if (maxNodeOffset != UINT64_MAX) {
        PageByteCursor pageByteCursor{};
        auto fillInMemListsFunc = getFillInMemListsFunc();
        for (node_offset_t nodeOffset = 0; nodeOffset <= maxNodeOffset; nodeOffset++) {
            auto header = adjList->getHeaders()->getHeader(nodeOffset);
            auto numElementsInList = adjList->getNumElementsFromListHeader(nodeOffset);
            for (auto elementOffset = 0u; elementOffset < numElementsInList; elementOffset++) {
                fillInMemListsFunc(this, inMemLists, defaultVal, pageByteCursor, nodeOffset, header,
                    numElementsInList - elementOffset);
            }
        }
    }
}

} // namespace processor
} // namespace kuzu
