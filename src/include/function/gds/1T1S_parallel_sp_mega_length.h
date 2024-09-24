#pragma once

#include "processor/execution_context.h"
#include "function/gds/gds.h"
#include "function/gds/parallel_utils.h"

namespace kuzu {
namespace function {

class _1T1SParallelMegaShortestPath {
public:
    _1T1SParallelMegaShortestPath(ExecutionContext *executionContext, GDSCallSharedState *sharedState,
        GDSBindData *bindData, ParallelUtils *parallelUtils) : executionContext{executionContext},
          sharedState{sharedState}, bindData{bindData}, parallelUtils{parallelUtils} {}

    void exec();

public:
    ExecutionContext *executionContext;
    GDSCallSharedState *sharedState;
    GDSBindData *bindData;
    ParallelUtils *parallelUtils;
};
} // namespace function
} // namespace kuzu
