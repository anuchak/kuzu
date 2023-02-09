#include "function/path/vector_path_operations.h"

#include "function/path/operations/path_creation_operation.h"
#include "function/path/operations/path_length_operation.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>>
PathLengthVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(PATH_LENGTH_FUNC_NAME,
        std::vector<DataTypeID>{PHYSICAL_PATH}, INT64,
        UnaryExecFunction<ku_path_t, int64_t, operation::PathLength>, false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
PathCreationVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(PATH_CREATION_FUNC_NAME,
        std::vector<DataTypeID>{LIST}, PHYSICAL_PATH,
        UnaryExecFunction<ku_list_t, ku_path_t, operation::PathCreation>, false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
