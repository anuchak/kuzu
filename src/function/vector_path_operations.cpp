#include "function/path/vector_path_operations.h"

#include "function/path/operations/path_length_operation.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>>
PathLengthVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(PATH_LENGTH_FUNC_NAME,
        std::vector<DataTypeID>{LIST}, INT64,
        UnaryExecFunction<ku_list_t, int64_t, operation::PathLength>, false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
