#pragma once

#include "bfs_state.h"
#include "common/exception/not_implemented.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
class ShortestPathState : public BaseBFSState {
public:
    ShortestPathState(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes,
        uint64_t bfsMorselSize,
        const std::unordered_map<common::table_id_t, std::string>& tableIDToName)
        : BaseBFSState{upperBound, lowerBound, targetDstNodes, bfsMorselSize, tableIDToName},
          numVisitedDstNodes{0}, startScanIdx{0u}, endScanIdx{0u} {}

    ~ShortestPathState() override = default;

    inline bool isComplete() final {
        return isCurrentFrontierEmpty() || isUpperBoundReached() || isAllDstReached();
    }

    inline bool getRecursiveJoinType() final { return TRACK_PATH; }

    inline void resetState() final {
        BaseBFSState::resetState();
        numVisitedDstNodes = 0;
        visited.clear();
    }

    inline void markSrc(common::nodeID_t nodeID) final {
        visited.insert(nodeID);
        if (targetDstNodes->contains(nodeID)) {
            numVisitedDstNodes++;
        }
        currentFrontier->addNodeWithMultiplicity(nodeID, 1);
    }

    inline void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::nodeID_t relID, uint64_t /*multiplicity*/) final {
        if (visited.contains(nbrNodeID)) {
            return;
        }
        visited.insert(nbrNodeID);
        if (targetDstNodes->contains(nbrNodeID)) {
            numVisitedDstNodes++;
        }
        if constexpr (TRACK_PATH) {
            nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
        } else {
            nextFrontier->addNodeWithMultiplicity(nbrNodeID, 1);
        }
    }

    inline uint64_t getNumVisitedDstNodes() { return numVisitedDstNodes; }

    /// This is used for nTkSCAS scheduler case (no tracking of path + single label case)
    inline void reset(uint64_t startScanIdx_, uint64_t endScanIdx_,
        BFSSharedState* bfsSharedState_) override {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        bfsSharedState = bfsSharedState_;
        numVisitedDstNodes = 0u;
    }

    // For Shortest Path, multiplicity is always 0
    inline uint64_t getBoundNodeMultiplicity(common::offset_t offset) override { return 0u; }

    inline common::offset_t getNextNodeOffset() override {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        return bfsSharedState->bfsLevelNodeOffsets[startScanIdx++];
    }

    void addToLocalNextBFSLevel(RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity,
        uint64_t boundNodeOffset) override;

    // For Shortest Path, this function will always return false, because there is no nodeID
    // multiplicity. Each distance morsel will exactly fit into ValueVector size perfectly.
    inline bool hasMoreToWrite() override { return false; }

    inline std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() override {
        throw common::NotImplementedException("");
    }

    int64_t writeToVector(const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
        RecursiveJoinVectors* vectors) override;

private:
    inline bool isAllDstReached() const {
        return numVisitedDstNodes == targetDstNodes->getNumNodes();
    }

private:
    uint64_t numVisitedDstNodes;
    common::node_id_set_t visited;

    /// These will be used for [Single Label, Track None] to track starting, ending index of morsel.
    uint64_t startScanIdx;
    uint64_t endScanIdx;
};

} // namespace processor
} // namespace kuzu
