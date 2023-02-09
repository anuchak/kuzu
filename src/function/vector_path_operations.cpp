#include "function/path/vector_path_operations.h"
#include "function/string/operations/length_operation.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>> PathLengthVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(PATH_LENGTH_FUNC_NAME,
        std::vector<DataTypeID>{LOGICAL_PATH}, INT64,
        UnaryExecFunction<ku_string_t, int64_t, operation::Length>, false /* isVarLength */));
    return definitions;
}

}
}
