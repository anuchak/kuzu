#pragma once

// TODO: Consider using forward declaration
#include "common/configs.h"
#include "common/logging_level_utils.h"
#include "processor/processor.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_manager.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace testing {
class BaseGraphTest;
} // namespace testing
} // namespace kuzu

namespace kuzu {
namespace main {

/**
 * @brief Stores buffer pool size and max number of threads.
 */
struct SystemConfig {

    /**
     * @brief Creates a SystemConfig object.
     * @param bufferPoolSize Buffer pool size in bytes.
     * @note defaultPageBufferPoolSize and largePageBufferPoolSize are calculated based on the
     * DEFAULT_PAGES_BUFFER_RATIO and LARGE_PAGES_BUFFER_RATIO constants in StorageConfig.
     */
    explicit SystemConfig(
        uint64_t bufferPoolSize = common::StorageConfig::DEFAULT_BUFFER_POOL_SIZE);

    uint64_t defaultPageBufferPoolSize;
    uint64_t largePageBufferPoolSize;

    uint64_t maxNumThreads = std::thread::hardware_concurrency();
};

/**
 * @brief Stores databasePath.
 */
struct DatabaseConfig {
    /**
     * @brief Creates a DatabaseConfig object.
     * @param databasePath Path to store the database files.
     */
    explicit DatabaseConfig(std::string databasePath) : databasePath{std::move(databasePath)} {}

    std::string databasePath;
};

/**
 * @brief Database class is the main class of the KuzuDB. It manages all database configurations and
 * files.
 */
class Database {
    friend class EmbeddedShell;
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::testing::BaseGraphTest;

public:
    /**
     * @brief Creates a Database object with default buffer pool size and max num threads.
     * @param databaseConfig Database configurations(database path).
     */
    explicit Database(const DatabaseConfig& databaseConfig)
        : Database{databaseConfig, SystemConfig()} {}

    /**
     * @brief Creates a Database object.
     * @param databaseConfig Database configurations(database path).
     * @param systemConfig System configurations(buffer pool size and max num threads).
     */
    explicit Database(const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig);

    /**
     * @brief Sets the logging level of the database instance.
     * @param loggingLevel New logging level. (Supported logging levels are: info, debug, err).
     */
    void setLoggingLevel(spdlog::level::level_enum loggingLevel);

    /**
     * @brief Resizes the buffer pool size of the database instance.
     * @param newSize New buffer pool size in bytes.
     * @throws BufferManagerException if the new size is smaller than the current buffer manager
     * size.
     */
    void resizeBufferManager(uint64_t newSize);

    ~Database() = default;

private:
    // TODO(Semih): This is refactored here for now to be able to test transaction behavior
    // in absence of the frontend support. Consider moving this code to connection.cpp.
    // Commits and checkpoints a write transaction or rolls that transaction back. This involves
    // either replaying the WAL and either redoing or undoing and in either case at the end WAL is
    // cleared.
    // skipCheckpointForTestingRecovery is used to simulate a failure before checkpointing in tests.
    void commitAndCheckpointOrRollback(transaction::Transaction* writeTransaction, bool isCommit,
        bool skipCheckpointForTestingRecovery = false);

    void initDBDirAndCoreFilesIfNecessary() const;
    void initLoggers();

    inline void checkpointAndClearWAL() {
        checkpointOrRollbackAndClearWAL(false /* is not recovering */, true /* isCheckpoint */);
    }
    inline void rollbackAndClearWAL() {
        checkpointOrRollbackAndClearWAL(
            false /* is not recovering */, false /* rolling back updates */);
    }
    void recoverIfNecessary();
    void checkpointOrRollbackAndClearWAL(bool isRecovering, bool isCheckpoint);

private:
    DatabaseConfig databaseConfig;
    SystemConfig systemConfig;
    std::unique_ptr<storage::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<catalog::Catalog> catalog;
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
    std::unique_ptr<storage::WAL> wal;
    std::shared_ptr<spdlog::logger> logger;
};

} // namespace main
} // namespace kuzu
