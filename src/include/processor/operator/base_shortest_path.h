#pragma once

#include "queue"

#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/lists/lists.h"
#include "common/types/internal_id_t.h"

namespace kuzu {
namespace processor {

struct NodeState {
    uint64_t parentNodeID;
    uint64_t relParentID;

    NodeState(uint64_t parentNodeID, uint64_t relParentID)
        : parentNodeID{parentNodeID}, relParentID{relParentID} {}
};

class BaseShortestPath : public PhysicalOperator {
public:
    BaseShortestPath(PhysicalOperatorType physicalOperatorType, const DataPos& srcDataPos,
        const DataPos& destDataPos, uint64_t lowerBound, uint64_t upperBound,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{physicalOperatorType, move(child), id, paramsString},
          srcDataPos{srcDataPos}, destDataPos{destDataPos}, lowerBound{lowerBound},
          upperBound{upperBound}, currFrontier{std::make_shared<std::vector<common::offset_t>>()},
          nextFrontier{std::make_shared<std::vector<common::offset_t>>()},
          bfsVisitedNodesMap{std::map<uint64_t, std::unique_ptr<NodeState>>()} {}

    virtual bool getNextTuplesInternal() = 0;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    virtual std::unique_ptr<PhysicalOperator> clone() = 0;

protected:
    DataPos srcDataPos;
    DataPos destDataPos;
    uint64_t lowerBound;
    uint64_t upperBound;
    std::shared_ptr<common::ValueVector> srcValueVector;
    std::shared_ptr<common::ValueVector> destValueVector;
    std::map<uint64_t, std::unique_ptr<NodeState>> bfsVisitedNodesMap;
    std::shared_ptr<std::vector<common::offset_t>> currFrontier;
    std::shared_ptr<std::vector<common::offset_t>> nextFrontier;
};

} // namespace processor
} // namespace kuzu
