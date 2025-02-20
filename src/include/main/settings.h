#pragma once

#include "common/types/value/value.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include <common/exception/runtime.h>
#include <common/task_system/task_scheduler.h>

namespace kuzu {
namespace main {

struct ThreadsSetting {
    static constexpr const char* name = "threads";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->numThreads = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->numThreads);
    }
};

struct ConcurrentBFSSetting {
    static constexpr const char* name = "concurrent_bfs";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->maxConcurrentBFS = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->maxConcurrentBFS);
    }
};

struct RJBFSMorselSizeSetting {
    static constexpr const char* name = "bfs_morsel";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->recursiveJoinBFSMorselSize =
            parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->recursiveJoinBFSMorselSize);
    }
};

struct SchedulerPolicy {
    static constexpr const char* name = "scheduler_policy";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        auto schedulerPolicy = parameter.getValue<std::string>();
        if (schedulerPolicy == "1T1S") {
            context->getClientConfigUnsafe()->bfsSchedulerType = common::OneThreadOneMorsel;
        } else if (schedulerPolicy == "nTkS") {
            context->getClientConfigUnsafe()->bfsSchedulerType = common::nThreadkMorsel;
        } else if (schedulerPolicy == "nTkSAdaptive") {
            context->getClientConfigUnsafe()->bfsSchedulerType = common::nThreadkMorselAdaptive;
        } else {
            throw common::RuntimeException(
                common::stringFormat("Invalid scheduler policy %s", schedulerPolicy));
        }
    }
    static common::Value getSetting(ClientContext* context) {
        auto schedulerPolicy = context->getClientConfig()->bfsSchedulerType;
        if (schedulerPolicy == common::OneThreadOneMorsel) {
            return common::Value("1T1S");
        }
        if (schedulerPolicy == common::nThreadkMorsel) {
            return common::Value("nTkS");
        }
        if (schedulerPolicy == common::nThreadkMorselAdaptive) {
            return common::Value("nTkSAdaptive");
        }
        return common::Value("no policy set");
    }
};

struct WorkerPoolSetting {
    static constexpr const char* name = "worker_pool";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->setWorkerPoolSize(parameter.getValue<int64_t>());
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getWorkerPoolSize());
    }
};

struct TimeoutSetting {
    static constexpr const char* name = "timeout";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->timeoutInMS = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->timeoutInMS);
    }
};

struct ProgressBarSetting {
    static constexpr const char* name = "progress_bar";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->enableProgressBar = parameter.getValue<bool>();
        context->getProgressBar()->toggleProgressBarPrinting(parameter.getValue<bool>());
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->enableProgressBar);
    }
};

struct ProgressBarTimerSetting {
    static constexpr const char* name = "progress_bar_time";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->showProgressAfter = parameter.getValue<int64_t>();
        context->getProgressBar()->setShowProgressAfter(parameter.getValue<int64_t>());
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->showProgressAfter);
    }
};

struct VarLengthExtendMaxDepthSetting {
    static constexpr const char* name = "var_length_extend_max_depth";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->varLengthMaxDepth = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->varLengthMaxDepth);
    }
};

struct EnableSemiMaskSetting {
    static constexpr const char* name = "enable_semi_mask";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->enableSemiMask = parameter.getValue<bool>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->enableSemiMask);
    }
};

struct EnableZoneMapSetting {
    static constexpr const char* name = "enable_zone_map";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->enableZoneMap = parameter.getValue<bool>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->enableZoneMap);
    }
};

struct HomeDirectorySetting {
    static constexpr const char* name = "home_directory";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->homeDirectory = parameter.getValue<std::string>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value::createValue(context->getClientConfig()->homeDirectory);
    }
};

struct FileSearchPathSetting {
    static constexpr const char* name = "file_search_path";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->fileSearchPath = parameter.getValue<std::string>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value::createValue(context->getClientConfig()->fileSearchPath);
    }
};

struct RecursivePatternSemanticSetting {
    static constexpr const char* name = "recursive_pattern_semantic";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        auto input = parameter.getValue<std::string>();
        context->getClientConfigUnsafe()->recursivePatternSemantic =
            common::PathSemanticUtils::fromString(input);
    }
    static common::Value getSetting(ClientContext* context) {
        auto result = common::PathSemanticUtils::toString(
            context->getClientConfig()->recursivePatternSemantic);
        return common::Value::createValue(result);
    }
};

struct RecursivePatternFactorSetting {
    static constexpr const char* name = "recursive_pattern_factor";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->recursivePatternCardinalityScaleFactor =
            parameter.getValue<std::int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value::createValue(
            context->getClientConfig()->recursivePatternCardinalityScaleFactor);
    }
};

struct EnableMVCCSetting {
    static constexpr const char* name = "enable_multi_writes";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType().getLogicalTypeID() == common::LogicalTypeID::BOOL);
        // TODO: This is a temporary solution to make tests of multiple write transactions easier.
        context->getDBConfigUnsafe()->enableMultiWrites = parameter.getValue<bool>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getDBConfig()->enableMultiWrites);
    }
};

} // namespace main
} // namespace kuzu
