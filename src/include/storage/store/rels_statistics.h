#pragma once

#include <map>

#include "storage/storage_utils.h"
#include "table_statistics.h"

namespace kuzu {
namespace storage {

class RelsStatistics;
class RelStatistics : public TableStatistics {
    friend class RelsStatistics;

public:
    RelStatistics() : TableStatistics{0 /* numTuples */}, nextRelOffset{0} {}
    RelStatistics(uint64_t numRels, offset_t nextRelOffset)
        : TableStatistics{numRels}, nextRelOffset{nextRelOffset} {}

    inline offset_t getNextRelOffset() { return nextRelOffset; }

private:
    offset_t nextRelOffset;
};

// Manages the disk image of the numRels and numRelsPerDirectionBoundTable.
class RelsStatistics : public TablesStatistics {

public:
    // Should only be used by saveInitialRelsStatisticsToFile to start a database from an empty
    // directory.
    RelsStatistics() : TablesStatistics{} {};
    // Should be used when an already loaded database is started from a directory.
    explicit RelsStatistics(const string& directory) : TablesStatistics{} {
        logger->info("Initializing {}.", "RelsStatistics");
        readFromFile(directory);
        logger->info("Initialized {}.", "RelsStatistics");
    }

    // Should only be used by tests.
    explicit RelsStatistics(
        unordered_map<table_id_t, unique_ptr<RelStatistics>> relStatisticPerTable_);

    static inline void saveInitialRelsStatisticsToFile(const string& directory) {
        make_unique<RelsStatistics>()->saveToFile(
            directory, DBFileType::ORIGINAL, TransactionType::READ_ONLY);
    }

    inline RelStatistics* getRelStatistics(table_id_t tableID) const {
        auto& tableStatisticPerTable =
            tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable;
        return (RelStatistics*)tableStatisticPerTable[tableID].get();
    }

    void setNumRelsForTable(table_id_t relTableID, uint64_t numRels);

    void updateNumRelsByValue(table_id_t relTableID, int64_t value);

    offset_t getNextRelOffset(Transaction* transaction, table_id_t tableID);

protected:
    inline string getTableTypeForPrinting() const override { return "RelsStatistics"; }

    inline unique_ptr<TableStatistics> constructTableStatistic(TableSchema* tableSchema) override {
        return make_unique<RelStatistics>();
    }

    inline unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) override {
        return make_unique<RelStatistics>(*(RelStatistics*)tableStatistics);
    }

    inline string getTableStatisticsFilePath(
        const string& directory, DBFileType dbFileType) override {
        return StorageUtils::getRelsStatisticsFilePath(directory, dbFileType);
    }

    inline void increaseNextRelOffset(table_id_t relTableID, uint64_t numTuples) {
        ((RelStatistics*)tablesStatisticsContentForWriteTrx->tableStatisticPerTable.at(relTableID)
                .get())
            ->nextRelOffset += numTuples;
    }

    unique_ptr<TableStatistics> deserializeTableStatistics(
        uint64_t numTuples, uint64_t& offset, FileInfo* fileInfo, uint64_t tableID) override;

    void serializeTableStatistics(
        TableStatistics* tableStatistics, uint64_t& offset, FileInfo* fileInfo) override;
};

} // namespace storage
} // namespace kuzu
