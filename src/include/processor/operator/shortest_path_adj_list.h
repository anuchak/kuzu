#pragma once

#include <utility>

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

class ShortestPathAdjList : public BaseShortestPath {
public:
    ShortestPathAdjList(const DataPos& srcDataPos, const DataPos& destDataPos,
        storage::AdjLists* adjList, storage::Lists* relPropertyLists, uint64_t lowerBound,
        uint64_t upperBound, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : BaseShortestPath{PhysicalOperatorType::SHORTEST_PATH_ADJ_LIST, srcDataPos, destDataPos,
              lowerBound, upperBound, move(child), id, paramsString},
          listSyncState{std::make_shared<storage::ListSyncState>()},
          listHandle{std::make_shared<storage::ListHandle>(*listSyncState)}, lists{adjList},
          relPropertyLists{std::move(relPropertyLists)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    bool computeShortestPath(uint64_t currIdx, uint64_t destIdx);

    bool addToNextFrontier(common::offset_t parentNodeOffset, common::offset_t destNodeOffset);

    bool getNextBatchOfChildNodes();

    bool extendToNextFrontier(common::offset_t destNodeOffset);

    void printShortestPath(common::offset_t destNodeOffset);

    void resetFrontier();

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ShortestPathAdjList>(srcDataPos, destDataPos, lists, relPropertyLists,
            lowerBound, upperBound, children[0]->clone(), id, paramsString);
    }

private:
    std::shared_ptr<common::ValueVector> adjNodeIDVector;
    std::shared_ptr<common::ValueVector> relIDVector;
    std::shared_ptr<storage::ListSyncState> listSyncState;
    std::shared_ptr<storage::ListHandle> listHandle;
    storage::AdjLists* lists;
    storage::Lists* relPropertyLists;
    std::shared_ptr<storage::ListHandle> listHandles;
};

} // namespace processor
} // namespace kuzu
