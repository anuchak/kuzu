#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct WeightedShortestPathMorsel : public BaseBFSMorsel {
public:
    WeightedShortestPathMorsel(uint8_t upperBound, uint8_t lowerBound,
        TargetDstNodes* targetDstNodes, bool isSingleThread, common::offset_t maxOffset,
        RecursiveJoinVectors* recursiveJoinVectors)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound}, startScanIdx{0u},
          endScanIdx{0u}, tableID{UINT64_MAX}, isSingleThread{isSingleThread}, maxOffset{maxOffset},
          vectors{recursiveJoinVectors}, prevWriteEndIdx{UINT64_MAX} {}

    ~WeightedShortestPathMorsel() override = default;

    inline bool getRecursiveJoinType() final { return TRACK_PATH; }

    inline bool isComplete() final {
        if (wBFSLevelNodes.empty()) {
            prevWriteEndIdx = 0u; // Set the index here to 0 to indicate that we are in write phase
            return true;
        }
        if (isUpperBoundReached()) {
            prevWriteEndIdx = 0u; // Set the index here to 0 to indicate that we are in write phase
            return true;
        }
        return false;
    }

    // This will be used for 1 Thread - 1 Weighted Shortest Path
    inline void resetState() final {
        currentLevel = 0u;
        nextNodeIdxToExtend = 0;
        auto totalDestinations = targetDstNodes->getNumNodes();
        prevWriteEndIdx = UINT64_MAX;
        if (visitedNodes.empty()) {
            wBFSLevelNodes = std::vector<common::nodeID_t>();
            visitedNodes = std::vector<uint8_t>(maxOffset + 1);
            pathCost = std::vector<int64_t>(maxOffset + 1, INT64_MAX);
            pathLength = std::vector<uint8_t>(maxOffset + 1, 0u);
        } else {
            wBFSLevelNodes.clear();
            std::fill(pathCost.begin(), pathCost.end(), INT64_MAX);
            std::fill(pathLength.begin(), pathLength.end(), 0u);
        }
        if (totalDestinations == (maxOffset + 1) || totalDestinations == 0u) {
            // All node offsets are destinations hence mark all as not visited destinations.
            std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
        } else {
            std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED);
            for (auto& dstOffset : targetDstNodes->getNodeIDs()) {
                visitedNodes[dstOffset.offset] = NOT_VISITED_DST;
            }
        }
    }

    inline void markSrc(common::nodeID_t nodeID) override {
        wBFSLevelNodes.push_back(nodeID);
        pathCost[nodeID.offset] = 0;
        pathLength[nodeID.offset] = 0;
        offsetPrevPathCost.push_back(0);
        tableID = nodeID.tableID;
        if (targetDstNodes->contains(nodeID)) {
            visitedNodes[nodeID.offset] = VISITED_DST;
        } else {
            visitedNodes[nodeID.offset] = NOT_VISITED_DST;
        }
    }

    common::nodeID_t getNextNodeID() override {
        if (nextNodeIdxToExtend == wBFSLevelNodes.size()) {
            return common::nodeID_t{common::INVALID_OFFSET, common::INVALID_TABLE_ID};
        }
        return wBFSLevelNodes[nextNodeIdxToExtend++];
    }

    void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) final;

    inline void finalizeCurrentLevel() override {
        currentLevel++;
        nextNodeIdxToExtend = 0u;
        if (currentLevel < upperBound) {
            wBFSLevelNodes.clear();
            offsetPrevPathCost.clear();
            for (auto i = 0u; i < (maxOffset + 1); i++) {
                if (visitedNodes[i] == VISITED_NEW) {
                    visitedNodes[i] = VISITED;
                    wBFSLevelNodes.emplace_back(i, tableID);
                    offsetPrevPathCost.push_back(pathCost[i]);
                } else if (visitedNodes[i] == VISITED_DST_NEW) {
                    visitedNodes[i] = VISITED_DST;
                    wBFSLevelNodes.emplace_back(i, tableID);
                    offsetPrevPathCost.push_back(pathCost[i]);
                }
            }
        }
    }

    // This will be used for n Thread - k Weighted Shortest Path
    inline void reset(
        uint64_t startScanIdx_, uint64_t endScanIdx_, BFSSharedState* bfsSharedState_) override {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        bfsSharedState = bfsSharedState_;
    }

    // For Shortest Path, multiplicity is always 0, gets called for n Threads, k wBFS case
    inline uint64_t getBoundNodeMultiplicity(common::offset_t offset) override { return 0u; }

    // Same as above, gets called for 1 Thread, 1 wBFS case
    inline uint64_t getMultiplicity(common::nodeID_t nodeID) const override { return 0; }

    inline common::offset_t getNextNodeOffset() override {
        if (startScanIdx == endScanIdx) {
            return common::INVALID_OFFSET;
        }
        return bfsSharedState->bfsLevelNodeOffsets[startScanIdx++];
    }

    void addToLocalNextBFSLevel(RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity,
        unsigned long boundNodeOffset) override;

    // For shortest path, morsel size will always be 2048, fits into a ValueVector.
    inline bool hasMoreToWrite() override { return false; }

    inline std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() override {
        throw common::NotImplementedException("");
    }

    int64_t writeToVector(
        const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
        RecursiveJoinVectors* vectors_) override;

private:
    // These are to be used for 1 Thread - 1 Weighted Shortest Path source
    std::vector<common::nodeID_t> wBFSLevelNodes;
    std::vector<uint8_t> visitedNodes;
    std::vector<int64_t> pathCost;
    std::vector<uint8_t> pathLength;
    std::vector<int64_t> offsetPrevPathCost;
    common::table_id_t tableID; // TEMP - to keep track of the table ID of the node table
    uint64_t prevWriteEndIdx;

    // These are to be used for n Threads - k Weighted Shortest Path source
    uint64_t startScanIdx;
    uint64_t endScanIdx;

    // These are common and will be used for both
    bool isSingleThread; // If Bellman Ford is being executed by 1 thread or multiple threads.
    common::offset_t maxOffset;
    RecursiveJoinVectors* vectors;
};

} // namespace processor
} // namespace kuzu
