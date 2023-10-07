#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct VariableLengthMorsel : public BaseBFSMorsel {
    VariableLengthMorsel(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound},
          localEdgeListSegment{std::vector<edgeListSegment*>()}, hasMorePathToWrite{false} {}
    ~VariableLengthMorsel() override = default;

    inline bool getRecursiveJoinType() final { return TRACK_PATH; }

    inline void resetState() final { BaseBFSMorsel::resetState(); }

    inline bool isComplete() final { return isCurrentFrontierEmpty() || isUpperBoundReached(); }

    inline void markSrc(common::nodeID_t nodeID) final {
        currentFrontier->addNodeWithMultiplicity(nodeID, 1 /* multiplicity */);
    }

    inline void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) final {
        if constexpr (TRACK_PATH) {
            nextFrontier->addEdge(boundNodeID, nbrNodeID, relID);
        } else {
            nextFrontier->addNodeWithMultiplicity(nbrNodeID, multiplicity);
        }
    }

    inline void reset(
        uint64_t startScanIdx_, uint64_t endScanIdx_, BFSSharedState* bfsSharedState_) override {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        bfsSharedState = bfsSharedState_;
        if (TRACK_PATH && nodeBuffer.empty()) {
            nodeBuffer = std::vector<edgeListAndLevel*>(31u, nullptr);
            relBuffer = std::vector<edgeList*>(31u, nullptr);
        }
    }

    inline uint64_t getBoundNodeMultiplicity(common::offset_t nodeOffset) override {
        if (!bfsSharedState->nodeIDMultiplicityToLevel.empty()) {
            auto topEntry = bfsSharedState->nodeIDMultiplicityToLevel[nodeOffset];
            while (topEntry && topEntry->bfsLevel != bfsSharedState->currentLevel) {
                topEntry = topEntry->next;
            }
            return topEntry->multiplicity;
        }
        return 0u;
    }

    void addToLocalNextBFSLevel(RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity,
        unsigned long boundNodeOffset) override;

    inline common::offset_t getNextNodeOffset() override {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        return bfsSharedState->bfsLevelNodeOffsets[startScanIdx++];
    }

    inline bool hasMoreToWrite() override {
        return prevDistMorselStartEndIdx.first < prevDistMorselStartEndIdx.second;
    }

    inline std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() override {
        return {prevDistMorselStartEndIdx.first,
            prevDistMorselStartEndIdx.second - prevDistMorselStartEndIdx.first};
    }

    int64_t writeToVector(
        const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
        RecursiveJoinVectors* vectors) override;

    inline std::vector<edgeListSegment*>& getLocalEdgeListSegments() {
        return localEdgeListSegment;
    }

private:
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
