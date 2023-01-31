#pragma once

#include "catalog/catalog.h"
#include "common/utils.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

enum class RelTableDataType : uint8_t {
    COLUMNS = 0,
    LISTS = 1,
};

class ListsUpdateIteratorsForDirection {
public:
    inline ListsUpdateIterator* getListUpdateIteratorForAdjList() {
        return listUpdateIteratorForAdjList.get();
    }

    inline unordered_map<property_id_t, unique_ptr<ListsUpdateIterator>>&
    getListsUpdateIteratorsForPropertyLists() {
        return listUpdateIteratorsForPropertyLists;
    }

    void addListUpdateIteratorForAdjList(unique_ptr<ListsUpdateIterator>);

    void addListUpdateIteratorForPropertyList(
        property_id_t propertyID, unique_ptr<ListsUpdateIterator> listsUpdateIterator);

    void doneUpdating();

private:
    unique_ptr<ListsUpdateIterator> listUpdateIteratorForAdjList;
    unordered_map<property_id_t, unique_ptr<ListsUpdateIterator>>
        listUpdateIteratorsForPropertyLists;
};

struct RelTableScanState {
public:
    explicit RelTableScanState(vector<property_id_t> propertyIds, RelTableDataType relTableDataType)
        : relTableDataType{relTableDataType}, propertyIds{std::move(propertyIds)} {
        if (relTableDataType == RelTableDataType::LISTS) {
            syncState = make_unique<ListSyncState>();
            // The first listHandle is for adj lists.
            listHandles.resize(this->propertyIds.size() + 1);
            for (auto i = 0u; i < this->propertyIds.size() + 1; i++) {
                listHandles[i] = make_unique<ListHandle>(*syncState);
            }
        }
    }

    bool hasMoreAndSwitchSourceIfNecessary() const {
        return relTableDataType == RelTableDataType::LISTS &&
               syncState->hasMoreAndSwitchSourceIfNecessary();
    }

    RelTableDataType relTableDataType;
    vector<property_id_t> propertyIds;
    // sync state between adj and property lists
    unique_ptr<ListSyncState> syncState;
    vector<unique_ptr<ListHandle>> listHandles;
};

class DirectedRelTableData {
public:
    DirectedRelTableData(table_id_t tableID, table_id_t boundTableID, RelDirection direction,
        ListsUpdatesStore* listsUpdatesStore, BufferManager& bufferManager,
        bool isSingleMultiplicityInDirection)
        : tableID{tableID}, boundTableID{boundTableID}, direction{direction},
          listsUpdatesStore{listsUpdatesStore}, bufferManager{bufferManager},
          isSingleMultiplicityInDirection{isSingleMultiplicityInDirection} {}

    inline uint32_t getNumPropertyLists() { return propertyLists.size(); }
    // Returns the list offset of the given relID if the relID stored as list in the current
    // direction, otherwise it returns UINT64_MAX.
    inline list_offset_t getListOffset(nodeID_t nodeID, int64_t relID) {
        return adjLists != nullptr ?
                   ((RelIDList*)getPropertyLists(RelTableSchema::INTERNAL_REL_ID_PROPERTY_IDX))
                       ->getListOffset(nodeID.offset, relID) :
                   UINT64_MAX;
    }
    inline AdjColumn* getAdjColumn() const { return adjColumn.get(); }
    inline AdjLists* getAdjLists() const { return adjLists.get(); }
    inline bool isSingleMultiplicity() const { return isSingleMultiplicityInDirection; }

    void initializeData(RelTableSchema* tableSchema, WAL* wal);
    void initializeColumns(RelTableSchema* tableSchema, BufferManager& bufferManager, WAL* wal);
    void initializeLists(RelTableSchema* tableSchema, BufferManager& bufferManager, WAL* wal);
    Column* getPropertyColumn(property_id_t propertyID);
    Lists* getPropertyLists(property_id_t propertyID);

    inline void scan(Transaction* transaction, RelTableScanState& scanState,
        const shared_ptr<ValueVector>& inNodeIDVector,
        vector<shared_ptr<ValueVector>>& outputVectors) {
        if (scanState.relTableDataType == RelTableDataType::COLUMNS) {
            scanColumns(transaction, scanState, inNodeIDVector, outputVectors);
        } else {
            scanLists(transaction, scanState, inNodeIDVector, outputVectors);
        }
    }
    inline bool isBoundTable(table_id_t tableID) const { return tableID == boundTableID; }

    void insertRel(const shared_ptr<ValueVector>& boundVector,
        const shared_ptr<ValueVector>& nbrVector,
        const vector<shared_ptr<ValueVector>>& relPropertyVectors);
    void deleteRel(const shared_ptr<ValueVector>& boundVector);
    void updateRel(const shared_ptr<ValueVector>& boundVector, property_id_t propertyID,
        const shared_ptr<ValueVector>& propertyVector);
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates);
    unique_ptr<ListsUpdateIteratorsForDirection> getListsUpdateIteratorsForDirection();
    void removeProperty(property_id_t propertyID);
    void addProperty(Property& property, WAL* wal);
    void batchInitEmptyRelsForNewNodes(
        const RelTableSchema* relTableSchema, uint64_t numNodesInTable, const string& directory);

private:
    void scanColumns(Transaction* transaction, RelTableScanState& scanState,
        const shared_ptr<ValueVector>& inNodeIDVector,
        vector<shared_ptr<ValueVector>>& outputVectors);
    void scanLists(Transaction* transaction, RelTableScanState& scanState,
        const shared_ptr<ValueVector>& inNodeIDVector,
        vector<shared_ptr<ValueVector>>& outputVectors);

private:
    unordered_map<property_id_t, unique_ptr<Column>> propertyColumns;
    unique_ptr<AdjColumn> adjColumn;
    unordered_map<property_id_t, unique_ptr<Lists>> propertyLists;
    unique_ptr<AdjLists> adjLists;
    table_id_t tableID;
    table_id_t boundTableID;
    RelDirection direction;
    ListsUpdatesStore* listsUpdatesStore;
    BufferManager& bufferManager;
    bool isSingleMultiplicityInDirection;
};

class RelTable {
public:
    RelTable(const catalog::Catalog& catalog, table_id_t tableID, BufferManager& bufferManager,
        MemoryManager& memoryManager, WAL* wal);

    void initializeData(RelTableSchema* tableSchema);

    inline Column* getPropertyColumn(RelDirection relDirection, property_id_t propertyId) {
        return relDirection == FWD ? fwdRelTableData->getPropertyColumn(propertyId) :
                                     bwdRelTableData->getPropertyColumn(propertyId);
    }
    inline Lists* getPropertyLists(RelDirection relDirection, property_id_t propertyId) {
        return relDirection == FWD ? fwdRelTableData->getPropertyLists(propertyId) :
                                     bwdRelTableData->getPropertyLists(propertyId);
    }
    inline uint32_t getNumPropertyLists(RelDirection relDirection) {
        return relDirection == FWD ? fwdRelTableData->getNumPropertyLists() :
                                     bwdRelTableData->getNumPropertyLists();
    }
    inline AdjColumn* getAdjColumn(RelDirection relDirection) {
        return relDirection == FWD ? fwdRelTableData->getAdjColumn() :
                                     bwdRelTableData->getAdjColumn();
    }
    inline AdjLists* getAdjLists(RelDirection relDirection) {
        return relDirection == FWD ? fwdRelTableData->getAdjLists() :
                                     bwdRelTableData->getAdjLists();
    }
    inline table_id_t getRelTableID() const { return tableID; }
    inline DirectedRelTableData* getDirectedTableData(RelDirection relDirection) {
        return relDirection == FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    }
    inline void removeProperty(property_id_t propertyID) {
        fwdRelTableData->removeProperty(propertyID);
        bwdRelTableData->removeProperty(propertyID);
    }
    inline bool isSingleMultiplicityInDirection(RelDirection relDirection) {
        return relDirection == FWD ? fwdRelTableData->isSingleMultiplicity() :
                                     bwdRelTableData->isSingleMultiplicity();
    }

    vector<AdjLists*> getAllAdjLists(table_id_t boundTableID);
    vector<AdjColumn*> getAllAdjColumns(table_id_t boundTableID);

    void prepareCommitOrRollbackIfNecessary(bool isCommit);
    void checkpointInMemoryIfNecessary();
    void rollbackInMemoryIfNecessary();

    void insertRel(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector,
        const vector<shared_ptr<ValueVector>>& relPropertyVectors);
    void deleteRel(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector);
    void updateRel(const shared_ptr<ValueVector>& srcNodeIDVector,
        const shared_ptr<ValueVector>& dstNodeIDVector, const shared_ptr<ValueVector>& relIDVector,
        const shared_ptr<ValueVector>& propertyVector, uint32_t propertyID);
    void initEmptyRelsForNewNode(nodeID_t& nodeID);
    void batchInitEmptyRelsForNewNodes(
        const RelTableSchema* relTableSchema, uint64_t numNodesInTable);
    void addProperty(Property property);

private:
    inline void addToUpdatedRelTables() { wal->addToUpdatedRelTables(tableID); }
    inline void clearListsUpdatesStore() { listsUpdatesStore->clear(); }
    static void appendInMemListToLargeListOP(
        ListsUpdateIterator* listsUpdateIterator, offset_t nodeOffset, InMemList& inMemList);
    static void updateListOP(
        ListsUpdateIterator* listsUpdateIterator, offset_t nodeOffset, InMemList& inMemList);
    void performOpOnListsWithUpdates(const std::function<void(Lists*)>& opOnListsWithUpdates,
        const std::function<void()>& opIfHasUpdates);
    unique_ptr<ListsUpdateIteratorsForDirection> getListsUpdateIteratorsForDirection(
        RelDirection relDirection) const;
    void prepareCommitForDirection(RelDirection relDirection);
    void prepareCommitForListWithUpdateStoreDataOnly(AdjLists* adjLists, offset_t nodeOffset,
        ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, RelDirection relDirection,
        ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection,
        const std::function<void(ListsUpdateIterator* listsUpdateIterator, offset_t,
            InMemList& inMemList)>& opOnListsUpdateIterators);
    void prepareCommitForList(AdjLists* adjLists, offset_t nodeOffset,
        ListsUpdatesForNodeOffset* listsUpdatesForNodeOffset, RelDirection relDirection,
        ListsUpdateIteratorsForDirection* listsUpdateIteratorsForDirection);

private:
    table_id_t tableID;
    unique_ptr<DirectedRelTableData> fwdRelTableData;
    unique_ptr<DirectedRelTableData> bwdRelTableData;
    unique_ptr<ListsUpdatesStore> listsUpdatesStore;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
