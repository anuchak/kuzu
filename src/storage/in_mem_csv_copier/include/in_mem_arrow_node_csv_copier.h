#pragma once

#include "in_mem_structures_csv_copier.h"

#include "src/storage/index/include/hash_index_builder.h"
#include "src/storage/store/include/nodes_statistics_and_deleted_ids.h"

#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

namespace kuzu {
namespace storage {

class InMemArrowNodeCSVCopier : public InMemStructuresCSVCopier {

public:
    InMemArrowNodeCSVCopier(CSVDescription& csvDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs);

    ~InMemArrowNodeCSVCopier() override = default;

    uint64_t copy();
    void saveToFile() override;

private:
    arrow::Status initializeArrow(const string& arrowFilePath);
    void initializeColumnsAndList();
    vector<string> countLinesPerBlockAndParseUnstrPropertyNames(uint64_t numStructuredProperties);
    template<typename T>
    void populateColumnsAndCountUnstrPropertyListSizes();
    void calcUnstrListsHeadersAndMetadata();
    void populateUnstrPropertyLists();

    static void calcLengthOfUnstrPropertyLists(
        CSVReader& reader, node_offset_t nodeOffset, InMemUnstructuredLists* unstrPropertyLists);
    static void putUnstrPropsOfALineToLists(CSVReader& reader, node_offset_t nodeOffset,
        PageByteCursor& inMemOverflowFileCursor,
        unordered_map<string, uint64_t>& unstrPropertiesNameToIdMap,
        InMemUnstructuredLists* unstrPropertyLists);

    static void putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>>& columns,
        const vector<Property>& properties, vector<PageByteCursor>& overflowCursors,
        CSVReader& reader, uint64_t nodeOffset);
    template<typename T>
    static void addIDsToIndex(InMemColumn* column, HashIndexBuilder<T>* hashIndex,
        node_offset_t startOffset, uint64_t numValues);
    template<typename T>
    static void populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
        node_offset_t startOffset, uint64_t numValues);
    static void skipFirstRowIfNecessary(
        uint64_t blockId, const CSVDescription& csvDescription, CSVReader& reader);

    // Concurrent tasks.
    // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
    // Instead, it is the index in the structured columns that we expect it to appear.
    template<typename T>
    static void populateColumnsAndCountUnstrPropertyListSizesTask(uint64_t primaryKeyPropertyIdx,
        uint64_t blockId, uint64_t offsetStart, HashIndexBuilder<T>* pkIndex,
        InMemArrowNodeCSVCopier* copier);
    static void populateUnstrPropertyListsTask(
        uint64_t blockId, node_offset_t nodeOffsetStart, InMemArrowNodeCSVCopier* copier);

    void calculateArrowNumBlocks(const string& arrowFilePath, string tableName);

protected:
    string arrowFilePath;
    std::shared_ptr<arrow::io::ReadableFile> infile;

private:
    NodeTableSchema* nodeTableSchema;
    uint64_t numNodes;
    vector<unique_ptr<InMemColumn>> structuredColumns;
    unique_ptr<InMemUnstructuredLists> unstrPropertyLists;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
};

} // namespace storage
} // namespace kuzu
