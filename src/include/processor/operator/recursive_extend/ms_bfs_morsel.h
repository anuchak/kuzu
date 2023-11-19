#pragma once

#include "bfs_state.h"

namespace kuzu {
namespace processor {

template<bool TRACK_PATH>
struct MSBFSMorsel : public BaseBFSMorsel {
public:
    MSBFSMorsel(uint8_t upperBound, uint8_t lowerBound, common::offset_t maxOffset,
        TargetDstNodes* targetDstNodes)
        : BaseBFSMorsel{targetDstNodes, upperBound, lowerBound}, maxOffset{maxOffset},
          dstReachedMask{0llu}, totalSources{0u}, dstLaneCount{0u}, curSrcIdx{0u},
          hasMoreDst{false}, srcNodeDataChunkSelectedPositions{nullptr}, lastDstOffsetWritten{0u} {
        visit = new uint8_t [maxOffset + 1]{0llu};
        seen = new uint8_t [maxOffset + 1]{0llu};
        next = new uint8_t [maxOffset + 1]{0llu};
    }

    ~MSBFSMorsel() override {
        delete[] visit;
        delete[] seen;
        delete[] next;
    }

    inline bool getRecursiveJoinType() final { return TRACK_PATH; }

    inline bool isComplete() final {
        /**
         * 1) Is current frontier empty ? As in the visit vector does not have 1 for even 1 element.
         * 2) Is upper bound reached ? The bfs level is equal to the upper bound of query.
         * 3) Are all the destinations reached ? The seen[dstOffset] has to be 1 or else not reached
         * for at least 1 BFS.
         */
        if (currentLevel == upperBound) {
            return true;
        }
        auto totalCount = targetDstNodes->getNumNodes();
        auto count = 0u;
        for (auto dstOffset = 0u; dstOffset < (maxOffset + 1); dstOffset++) {
            if (seen[dstOffset] != dstReachedMask)
                return false;
            count++;
        }
        return totalCount == count;
    }

    inline void updateBFSLevel() { currentLevel++; }

    inline void resetState() final {
        BaseBFSMorsel::resetState();
        memset(visit, 0llu, sizeof(uint8_t) * (maxOffset + 1));
        memset(seen, 0llu, sizeof(uint8_t) * (maxOffset + 1));
        memset(next, 0llu, sizeof(uint8_t) * (maxOffset + 1));
        dstReachedMask = 0llu;
        totalSources = 0u;
        dstLaneCount = 0u;
        hasMoreDst = false;
        curSrcIdx = 0u;
        srcNodeDataChunkSelectedPositions = nullptr;
        lastDstOffsetWritten = 0u;
    }

    inline void markSrc(common::nodeID_t nodeID) override {
        visit[nodeID.offset] = (1llu << totalSources++);
        dstReachedMask |= visit[nodeID.offset];
    }

    void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) final {
        // This one possibly does not have to be implemented.
    }

    inline uint64_t getBoundNodeMultiplicity(common::offset_t offset) override { return 0; }

    inline common::offset_t getNextNodeOffset() override { return 0; }

    void reset(
        uint64_t startScanIdx, uint64_t endScanIdx, BFSSharedState* bfsSharedState) override {}

    void addToLocalNextBFSLevel(RecursiveJoinVectors* vectors, uint64_t boundNodeMultiplicity,
        unsigned long boundNodeOffset) override {
        throw common::NotImplementedException{"This function is not supported for MS-BFS Morsel "
                                              "and TRACK_PATH recursive join type."};
    }

    inline bool hasMoreToWrite() override {
        // This might be needed because we will be writing (64 * total target destinations) in the
        // Value Vectors and that may be > 2048 (value vector capacity).
        return hasMoreDst;
    }

    inline std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() override {
        // Similarly this might be needed to be implemented to keep track midway writing to the
        // Value Vectors.
    }

    int64_t writeToVector(common::table_id_t tableID, RecursiveJoinVectors* recursiveJoinVectors);

    int64_t writeToVector(
        const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
        RecursiveJoinVectors* vectors) override {
        throw common::NotImplementedException{"This function is not supported for MS-BFS Morsel "
                                              "and TRACK_PATH recursive join type."};
    }

public:
    uint64_t maxOffset;
    common::sel_t* srcNodeDataChunkSelectedPositions; // keep track of the selected positions
    // FOR MS-BFS PoC - To test reachability in queries of Recursive Join
    // VISIT array stands for the current frontier, the nodes set to 1 are the ones we will visit.
    // SEEN array stands for the globally already seen nodes that cannot be visited again.
    // NEXT array stands for the next frontier, the ones which need to be visited as part of next
    // level.
    uint8_t * visit;
    uint8_t * seen;
    uint8_t * next;

private:
    uint64_t dstReachedMask;
    uint64_t totalSources; // total sources in MS-BFS morsel
    uint64_t dstLaneCount; // track lane for which destinations being written to value vector
    bool hasMoreDst; // track whether from the previous source some destinations are still left to
                     // write
    uint64_t curSrcIdx; // track the source idx from the selection positions vector for which
                        // destinations being written
    uint64_t lastDstOffsetWritten; // The last destination offset which was written to ValueVector.
};

} // namespace processor
} // namespace kuzu
