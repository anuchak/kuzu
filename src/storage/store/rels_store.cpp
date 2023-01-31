#include "storage/store/rels_store.h"

namespace kuzu {
namespace storage {

RelsStore::RelsStore(
    const Catalog& catalog, BufferManager& bufferManager, MemoryManager& memoryManager, WAL* wal)
    : relsStatistics{wal->getDirectory()} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        relTables[tableIDSchema.first] =
            make_unique<RelTable>(catalog, tableIDSchema.first, bufferManager, memoryManager, wal);
    }
}

pair<vector<AdjLists*>, vector<AdjColumn*>> RelsStore::getAdjListsAndColumns(
    const table_id_t boundTableID) const {
    vector<AdjLists*> adjListsRetVal;
    for (auto& [_, relTable] : relTables) {
        auto adjListsForRel = relTable->getAllAdjLists(boundTableID);
        adjListsRetVal.insert(adjListsRetVal.end(), adjListsForRel.begin(), adjListsForRel.end());
    }
    vector<AdjColumn*> adjColumnsRetVal;
    for (auto& [_, relTable] : relTables) {
        auto adjColumnsForRel = relTable->getAllAdjColumns(boundTableID);
        adjColumnsRetVal.insert(
            adjColumnsRetVal.end(), adjColumnsForRel.begin(), adjColumnsForRel.end());
    }
    return make_pair(adjListsRetVal, adjColumnsRetVal);
}

} // namespace storage
} // namespace kuzu
