#pragma once

#include "copy_structures_arrow.h"
#include "storage/index/hash_index.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace storage {

class CopyRelArrow : public CopyStructuresArrow {

public:
    CopyRelArrow(CopyDescription& copyDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog,
        map<table_id_t, offset_t> maxNodeOffsetsPerNodeTable, BufferManager* bufferManager,
        table_id_t tableID, RelsStatistics* relsStatistics);

    ~CopyRelArrow() override = default;

    uint64_t copy();

    void saveToFile() override;

    enum class PopulateTaskType { populateAdjColumnsAndCountRelsInAdjListsTask, populateListsTask };

    static std::string getTaskTypeName(PopulateTaskType populateTaskType);

private:
    void initializeColumnsAndLists();

    void initializeColumns(RelDirection relDirection);

    void initializeLists(RelDirection relDirection);

    void initAdjListsHeaders();

    void initListsMetadata();

    void initializePkIndexes(table_id_t nodeTableID, BufferManager& bufferManager);

    arrow::Status executePopulateTask(PopulateTaskType populateTaskType);

    arrow::Status populateFromCSV(PopulateTaskType populateTaskType);

    arrow::Status populateFromArrow(PopulateTaskType populateTaskType);

    arrow::Status populateFromParquet(PopulateTaskType populateTaskType);

    void populateAdjColumnsAndCountRelsInAdjLists();

    void populateLists();

    // We store rel properties with overflows, e.g., strings or lists, in
    // InMemColumn/ListsWithOverflowFile (e.g., InMemStringLists). When loading these properties
    // from csv, we first save the overflow pointers of the ku_list_t or ku_string_t in temporary
    // "unordered" InMemOverflowFiles in an unordered format, instead of the InMemOverflowFiles of
    // the actual InMemColumn/ListsWithOverflowFile. In these temporary unordered
    // InMemOverflowFiles, the string of nodeOffset100's overflow data may be stored before
    // nodeOffset10's overflow data). This is because the each thread gets a chunk of the csv file
    // storing rels but rel files are not necessarily sorted by source or destination node IDs.
    // Therefore, after populating an InMemColumn/ListWithOverflowFile we have to do two things: (1)
    // we need to copy over the data in these temporary unordered InMemOverflowFile to the
    // InMemOverflowFiles of the InMemColumn/ListsWithOverflowFile. (2) To increase the performance
    // of scanning these overflow files, we also sort the overflow pointers based on nodeOffsets, so
    // when scanning rels of consecutive nodes, the overflows of these rels appear consecutively on
    // disk.
    void sortAndCopyOverflowValues();

    template<typename T>
    static void inferTableIDsAndOffsets(const vector<shared_ptr<T>>& batchColumns,
        vector<nodeID_t>& nodeIDs, vector<DataType>& nodeIDTypes,
        const map<table_id_t, unique_ptr<PrimaryKeyIndex>>& pkIndexes, Transaction* transaction,
        const Catalog& catalog, int64_t blockOffset, int64_t& colIndex);

    template<typename T>
    static void putPropsOfLineIntoColumns(CopyRelArrow* copier,
        vector<PageByteCursor>& inMemOverflowFileCursors, const vector<shared_ptr<T>>& batchColumns,
        const vector<nodeID_t>& nodeIDs, int64_t blockOffset, int64_t& colIndex,
        CopyDescription& copyDescription);

    template<typename T>
    static void putPropsOfLineIntoLists(CopyRelArrow* copier,
        vector<PageByteCursor>& inMemOverflowFileCursors, const vector<shared_ptr<T>>& batchColumns,
        const vector<nodeID_t>& nodeIDs, const vector<uint64_t>& reversePos, int64_t blockOffset,
        int64_t& colIndex, CopyDescription& copyDescription);

    static void copyStringOverflowFromUnorderedToOrderedPages(ku_string_t* kuStr,
        PageByteCursor& unorderedOverflowCursor, PageByteCursor& orderedOverflowCursor,
        InMemOverflowFile* unorderedOverflowFile, InMemOverflowFile* orderedOverflowFile);

    static void copyListOverflowFromUnorderedToOrderedPages(ku_list_t* kuList,
        const DataType& dataType, PageByteCursor& unorderedOverflowCursor,
        PageByteCursor& orderedOverflowCursor, InMemOverflowFile* unorderedOverflowFile,
        InMemOverflowFile* orderedOverflowFile);

    // Concurrent tasks.
    template<typename T>
    static void populateAdjColumnsAndCountRelsInAdjListsTask(uint64_t blockId,
        uint64_t blockStartRelID, CopyRelArrow* copier, const vector<shared_ptr<T>>& batchColumns,
        CopyDescription& copyDescription);

    template<typename T>
    static void populateListsTask(uint64_t blockId, uint64_t blockStartRelID, CopyRelArrow* copier,
        const vector<shared_ptr<T>>& batchColumns, CopyDescription& copyDescription);

    static void sortOverflowValuesOfPropertyColumnTask(const DataType& dataType,
        offset_t offsetStart, offset_t offsetEnd, InMemColumn* propertyColumn,
        InMemOverflowFile* unorderedInMemOverflowFile, InMemOverflowFile* orderedInMemOverflowFile);

    static void sortOverflowValuesOfPropertyListsTask(const DataType& dataType,
        offset_t offsetStart, offset_t offsetEnd, InMemAdjLists* adjLists,
        InMemLists* propertyLists, InMemOverflowFile* unorderedInMemOverflowFile,
        InMemOverflowFile* orderedInMemOverflowFile);

private:
    const map<table_id_t, offset_t> maxNodeOffsetsPerTable;
    RelTableSchema* relTableSchema;
    RelsStatistics* relsStatistics;
    unique_ptr<Transaction> dummyReadOnlyTrx;
    map<table_id_t, unique_ptr<PrimaryKeyIndex>> pkIndexes;
    atomic<uint64_t> numRels = 0;
    vector<unique_ptr<atomic_uint64_vec_t>> directionTableListSizes{2};
    vector<unique_ptr<InMemAdjColumn>> directionTableAdjColumns{2};
    vector<unordered_map<property_id_t, unique_ptr<InMemColumn>>> directionTablePropertyColumns{2};
    vector<unique_ptr<InMemAdjLists>> directionTableAdjLists{2};
    vector<unordered_map<property_id_t, unique_ptr<InMemLists>>> directionTablePropertyLists{2};
    unordered_map<property_id_t, unique_ptr<InMemOverflowFile>> overflowFilePerPropertyID;
};

} // namespace storage
} // namespace kuzu
