#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct WeaklyConnectedComponentsFunction {
    static constexpr const char* name = "WEAKLY_CONNECTED_COMPONENT";

    static function_set getFunctionSet();
};

struct ShortestPathsFunction {
    static constexpr const char* name = "SHORTEST_PATHS";

    static function_set getFunctionSet();
};

struct PageRankFunction {
    static constexpr const char* name = "PAGE_RANK";

    static function_set getFunctionSet();
};

struct ParallelSPLengthsFunction {
    static constexpr const char* name = "ParallelSPLength";

    static function_set getFunctionSet();
};

struct ParallelMSBFSLengthsFunction {
    static constexpr const char* name = "ParallelMSBFSLength";

    static function_set getFunctionSet();
};

struct ParallelASPLengthsFunction {
    static constexpr const char* name = "ParallelASPLength";

    static function_set getFunctionSet();
};

struct ParallelVarlenLengthsFunction {
    static constexpr const char* name = "ParallelVarlenLength";

    static function_set getFunctionSet();
};

struct ParallelSPPathsFunction {
    static constexpr const char* name = "ParallelSPPath";

    static function_set getFunctionSet();
};

struct ParallelASPPathsFunction {
    static constexpr const char* name = "ParallelASPPath";

    static function_set getFunctionSet();
};

struct ParallelVarlenPathsFunction {
    static constexpr const char* name = "ParallelVarlenPath";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
