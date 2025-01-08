#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
class AllShortestPathState : public BaseBFSState {
public:
    AllShortestPathState(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes,
        uint64_t bfsMorselSize, const std::unordered_map<common::table_id_t, std::string>& tableIDToName)
        : BaseBFSState{upperBound, lowerBound, targetDstNodes, bfsMorselSize, tableIDToName}, minDistance{0},
          numVisitedDstNodes{0}, prevDistMorselStartEndIdx{0u, 0u},
          localEdgeListSegment{std::vector<edgeListSegment*>()}, hasMorePathToWrite{false} {}

    ~AllShortestPathState() override = default;

    inline bool getRecursiveJoinType() final { return TRACK_PATH; }

    inline bool isComplete() final {
        return isCurrentFrontierEmpty() || isUpperBoundReached() ||
               isAllDstReachedWithMinDistance();
    }

    inline void resetState() final {
        BaseBFSState::resetState();
        minDistance = 0;
        numVisitedDstNodes = 0;
        visitedNodeToDistance.clear();
    }

    inline void markSrc(common::nodeID_t nodeID) override {
        visitedNodeToDistance.insert({nodeID, -1});
        if (targetDstNodes->contains(nodeID)) {
            numVisitedDstNodes++;
        }
        currentFrontier->addNodeWithMultiplicity(nodeID, 1);
    }

    void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) final {
        if (!visitedNodeToDistance.contains(nbrNodeID)) {
            visitedNodeToDistance.insert({nbrNodeID, (int64_t)currentLevel});
            if (targetDstNodes->contains(nbrNodeID)) {
                minDistance = currentLevel;
                numVisitedDstNodes++;
            }
            if constexpr (TRACK_PATH) {
                nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
            } else {
                nextFrontier->addNodeWithMultiplicity(nbrNodeID, multiplicity);
            }
        } else if (currentLevel <= visitedNodeToDistance.at(nbrNodeID)) {
            if constexpr (TRACK_PATH) {
                nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
            } else {
                nextFrontier->addNodeWithMultiplicity(nbrNodeID, multiplicity);
            }
        }
    }

    /// Following functions added for nTkS scheduler
    inline uint64_t getNumVisitedDstNodes() { return numVisitedDstNodes; }
    inline uint64_t getNumVisitedNonDstNodes() { return numVisitedNonDstNodes; }
    inline void reset(
        uint64_t startScanIdx_, uint64_t endScanIdx_, BFSSharedState* bfsSharedState_) override {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        bfsSharedState = bfsSharedState_;
        numVisitedDstNodes = 0u;
        numVisitedNonDstNodes = 0;
        if (TRACK_PATH && nodeBuffer.empty()) {
            nodeBuffer = std::vector<edgeListAndLevel*>(31u, nullptr);
            relBuffer = std::vector<edgeList*>(31u, nullptr);
        }
    }

    // For Shortest Path, multiplicity is always 0
    inline uint64_t getBoundNodeMultiplicity(common::offset_t offset) override {
        if (bfsSharedState->nodeIDToMultiplicity.empty()) {
            return 0u;
        }
        return bfsSharedState->nodeIDToMultiplicity[offset];
    }

    inline common::offset_t getNextNodeOffset() override {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        if (bfsSharedState->isSparseFrontier) {
            return bfsSharedState->sparseFrontier[startScanIdx++];
        }
        return bfsSharedState->denseFrontier[startScanIdx++];
        // return bfsSharedState->bfsLevelNodeOffsets[startScanIdx++];
    }

    void addToLocalNextBFSLevel(RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity,
        unsigned long boundNodeOffset) override;

    inline bool hasMoreToWrite() override {
        return prevDistMorselStartEndIdx.first < prevDistMorselStartEndIdx.second;
    }

    inline std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() override {
        return {prevDistMorselStartEndIdx.first,
            prevDistMorselStartEndIdx.second - prevDistMorselStartEndIdx.first};
    }

    int64_t writeToVector(
        const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
        RecursiveJoinVectors* vectors) override;

    inline std::vector<edgeListSegment*>& getLocalEdgeListSegments() {
        return localEdgeListSegment;
    }

private:
    inline bool isAllDstReachedWithMinDistance() const {
        return numVisitedDstNodes == targetDstNodes->getNumNodes() && currentLevel > minDistance;
    }

private:
    uint32_t minDistance; // Min distance to add dst nodes that have been reached.
    uint64_t numVisitedDstNodes;
    uint64_t numVisitedNonDstNodes;
    common::node_id_map_t<int64_t> visitedNodeToDistance;

    /// NEW ADDITION for [Single Label, Track None] to track start, end index of morsel.
    uint64_t startScanIdx;
    uint64_t endScanIdx;
    std::pair<uint64_t, uint64_t> prevDistMorselStartEndIdx;
    /// For [Single Label, Track Path] case only.
    std::vector<edgeListSegment*> localEdgeListSegment;
    std::vector<edgeListAndLevel*> nodeBuffer;
    std::vector<edgeList*> relBuffer;
    bool hasMorePathToWrite;
};

} // namespace processor
} // namespace kuzu
