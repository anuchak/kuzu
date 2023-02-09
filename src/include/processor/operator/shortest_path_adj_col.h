#pragma once

#include "binder/expression/expression.h"
#include "common/vector/value_vector_utils.h"
#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/base_shortest_path.h"
#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class ShortestPathAdjCol : public BaseShortestPath {
public:
    ShortestPathAdjCol(const DataPos& srcDataPos, const DataPos& destDataPos, storage::Column* columns,
        uint64_t lowerBound, uint64_t upperBound, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : BaseShortestPath{PhysicalOperatorType::SHORTEST_PATH_ADJ_LIST, srcDataPos, destDataPos,
              lowerBound, upperBound, move(child), id, paramsString},
          col{columns} {}

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ShortestPathAdjCol>(srcDataPos, destDataPos, col, lowerBound, upperBound,
            children[0]->clone(), id, paramsString);
    }

private:
    storage::Column* col;
};

} // namespace processor
} // namespace kuzu
