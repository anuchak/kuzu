#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "common/api.h"
#include "common/constants.h"
#include "common/timer.h"
#include "main/kuzu_fwd.h"

namespace kuzu {

namespace binder {
class Binder;
}

namespace main {

struct ActiveQuery {
    explicit ActiveQuery();
    std::atomic<bool> interrupted;
    common::Timer timer;

    void reset();
};

/**
 * @brief Contain client side configuration. We make profiler associated per query, so profiler is
 * not maintained in client context.
 */
class ClientContext {
    friend class Connection;
    friend class binder::Binder;
    friend class testing::TinySnbDDLTest;
    friend class testing::TinySnbCopyCSVTransactionTest;
    friend class ThreadsSetting;
    friend class TimeoutSetting;
    friend class VarLengthExtendMaxDepthSetting;

public:
    explicit ClientContext();

    ~ClientContext() = default;

    inline common::SchedulerType getBFSSchedulerType() { return bfsSchedulerType; }

    inline uint64_t getMaxActiveBFSSharedState() { return maxActiveBFSSharedState; }

    inline void interrupt() { activeQuery.interrupted = true; }

    bool isInterrupted() const { return activeQuery.interrupted; }

    inline bool isTimeOut() {
        return isTimeOutEnabled() && activeQuery.timer.getElapsedTimeInMS() > timeoutInMS;
    }

    inline bool isTimeOutEnabled() const { return timeoutInMS != 0; }

    void startTimingIfEnabled();

    std::string getCurrentSetting(std::string optionName);

private:
    inline void resetActiveQuery() { activeQuery.reset(); }

    uint64_t numThreadsForExecution;
    common::SchedulerType bfsSchedulerType;
    /// ADDING THIS FOR EASIER TESTING ONLY
    uint64_t maxActiveBFSSharedState;
    ActiveQuery activeQuery;
    uint64_t timeoutInMS;
    uint32_t varLengthExtendMaxDepth;
};

} // namespace main
} // namespace kuzu
