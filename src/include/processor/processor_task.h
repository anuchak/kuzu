#pragma once

#include "common/task_system/task.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

class ProcessorTask : public common::Task {
    friend class QueryProcessor;

public:
    ProcessorTask(Sink* sink, ExecutionContext* executionContext);

    void run() override;
    void finalizeIfNecessary() override;

    inline void setSharedStateInitialized() { sharedStateInitialized = true; }

    inline void setNumberOfTaskThreads(uint64_t threads) {
        maxNumThreads = threads;
    }

    uint64_t getWorkNoLock() override {
        const auto numThreads = numThreadsRegistered ? numThreadsRegistered : 1;
        return sink->getWork() / numThreads;
    }

    inline Sink* getSink() { return sink; }

private:
    static std::unique_ptr<ResultSet> populateResultSet(Sink* op,
        storage::MemoryManager* memoryManager);

private:
    bool sharedStateInitialized;
    Sink* sink;
    ExecutionContext* executionContext;
};

} // namespace processor
} // namespace kuzu
