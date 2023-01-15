#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal() {
    AddProperty::executeDDLInternal();
    auto tableSchema = catalog->getWriteVersion()->getTableSchema(tableID);
    auto propertyID = tableSchema->getPropertyID(propertyName);
    auto property = tableSchema->getProperty(propertyID);
    auto nodeStatistics = storageManager.getNodesStore()
                              .getNodesStatisticsAndDeletedIDs()
                              .getNodeStatisticsAndDeletedIDs(tableID);
    auto inMemColumn = InMemColumnFactory::getInMemPropertyColumn(
        StorageUtils::getNodePropertyColumnFName(
            storageManager.getDirectory(), tableID, propertyID, DBFileType::ORIGINAL),
        dataType, nodeStatistics->getNumTuples());
    if (!isDefaultValueNull()) {
        fillInMemColumWithDefaultVal(
            inMemColumn.get(), getDefaultVal(), nodeStatistics->getMaxNodeOffset());
    }
    inMemColumn->saveToFile();
    storageManager.getNodesStore().getNodeTable(tableID)->addProperty(property);
}

} // namespace processor
} // namespace kuzu
