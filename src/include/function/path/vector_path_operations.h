#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct VectorPathOperations : public VectorOperations {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryExecFunction(const std::shared_ptr<common::ValueVector>& params,
        common::ValueVector& result) {
        UnaryOperationExecutor::execute<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params, result);
    }
};

struct PathLengthVectorOperation : public VectorPathOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct PathCreationVectorOperation : public VectorPathOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace kuzu