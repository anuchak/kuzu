#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct VectorPathOperations : public VectorOperations {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::executeString<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }
};

struct PathLengthVectorOperation : public VectorPathOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

}
}