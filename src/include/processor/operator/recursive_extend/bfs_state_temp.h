#pragma once

#include "processor/operator/mask.h"
#include "processor/operator/result_collector.h"

namespace kuzu {
namespace processor {

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
};

// SSSPMorsel States
enum SSSPLocalState {
    MORSEL_EXTEND_IN_PROGRESS,
    MORSEL_DISTANCE_WRITE_IN_PROGRESS,
    MORSEL_COMPLETE
};

// Global States for MorselDispatcher
enum GlobalSSSPState { IN_PROGRESS, IN_PROGRESS_ALL_SRC_SCANNED, COMPLETE };

struct BaseBFSMorsel;

struct SSSPMorsel {
public:
    SSSPMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : mutex{std::mutex()}, ssspLocalState{MORSEL_EXTEND_IN_PROGRESS}, currentLevel{0u},
          nextScanStartIdx{0u}, numVisitedNodes{0u}, visitedNodes{std::vector<uint8_t>(
                                                         maxNodeOffset_ + 1, NOT_VISITED)},
          distance{std::vector<uint16_t>(maxNodeOffset_ + 1, 0u)}, nodeMask{std::vector<uint8_t>(
                                                                       maxNodeOffset_ + 1, 0u)},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsActiveOnMorsel{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u},
          lvlStartTimeInMillis{0u}, startTimeInMillis{0u}, distWriteStartTimeInMillis{0u} {}

    void reset(std::vector<common::offset_t>& targetDstNodeOffsets);

    SSSPLocalState getBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel);

    bool finishBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel);

    // If BFS has completed.
    bool isComplete(uint64_t numDstNodesToVisit);
    // Mark src as visited.
    void markSrc(const std::vector<common::offset_t>& targetDstNodeOffsets);

    void moveNextLevelAsCurrentLevel();

    std::pair<uint64_t, int64_t> getDstDistanceMorsel();

public:
    std::mutex mutex;
    SSSPLocalState ssspLocalState;
    // Level state
    uint8_t currentLevel;
    uint64_t nextScanStartIdx;
    // Visited state
    uint64_t numVisitedNodes;
    std::vector<uint8_t> visitedNodes;
    std::vector<uint16_t> distance;
    std::vector<uint8_t> nodeMask;
    std::vector<common::offset_t> bfsLevelNodeOffsets;
    // Offset of src node.
    common::offset_t srcOffset;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    uint32_t numThreadsActiveOnMorsel;
    uint64_t nextDstScanStartIdx;
    uint64_t inputFTableTupleIdx;
    uint64_t lvlStartTimeInMillis;
    uint64_t startTimeInMillis;
    uint64_t distWriteStartTimeInMillis;
};

struct BaseBFSMorsel {

public:
    explicit BaseBFSMorsel(
        common::offset_t maxOffset, NodeOffsetSemiMask* semiMask, SSSPMorsel* ssspMorsel)
        : startScanIdx{0u}, endScanIdx{0u}, threadCheckSSSPState{true}, ssspMorsel{ssspMorsel},
          localVisitedDstNodes{0u} {
        if (semiMask->isEnabled()) {
            for (auto offset = 0u; offset < maxOffset + 1; ++offset) {
                if (semiMask->isNodeMasked(offset)) {
                    targetDstNodeOffsets.push_back(offset);
                }
            }
        }
    }

    virtual ~BaseBFSMorsel() = default;

    void reset(uint64_t startScanIdx_, uint64_t endScanIdx_, SSSPMorsel* ssspMorsel_) {
        startScanIdx = startScanIdx_;
        endScanIdx = endScanIdx_;
        ssspMorsel = ssspMorsel_;
        threadCheckSSSPState = false;
        localVisitedDstNodes = 0u;
    }

    inline uint64_t getNumDstNodeOffsets() {
        if (targetDstNodeOffsets.empty()) {
            return ssspMorsel->maxOffset;
        } else {
            return targetDstNodeOffsets.size();
        }
    }

    // Get next node offset to extend from current level.
    common::offset_t getNextNodeOffset();

    void addToLocalNextBFSLevel(const std::shared_ptr<common::ValueVector>& tmpDstNodeIDVector);

public:
    std::vector<common::offset_t> targetDstNodeOffsets;
    bool threadCheckSSSPState;
    uint64_t startScanIdx;
    uint64_t endScanIdx;
    SSSPMorsel* ssspMorsel;
    uint64_t localVisitedDstNodes; // Only for the destinations visited, increment this.
};

struct ShortestPathBFSMorsel : public BaseBFSMorsel {

    ShortestPathBFSMorsel(
        common::offset_t maxOffset, NodeOffsetSemiMask* semiMask, SSSPMorsel* ssspMorsel)
        : BaseBFSMorsel{maxOffset, semiMask, ssspMorsel} {}
};

struct MorselDispatcher {
public:
    MorselDispatcher(uint64_t lowerBound, uint64_t upperBound, uint64_t maxOffset,
        uint64_t numThreadsForExecution)
        : threadIdxCounter{0u}, numActiveSSSP{0u},
          activeSSSPMorsel{std::vector<std::shared_ptr<SSSPMorsel>>(numThreadsForExecution)},
          globalState{IN_PROGRESS}, maxOffset{maxOffset}, lowerBound{lowerBound}, upperBound{
                                                                                      upperBound} {}

    uint32_t getThreadIdx();

    // Not thread safe, called only for initialization of BFSMorsel. ThreadIdx position is fixed.
    SSSPMorsel* getSSSPMorsel(uint32_t threadIdx);

    std::pair<GlobalSSSPState, SSSPLocalState> getBFSMorsel(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        const std::vector<common::ValueVector*> vectorsToScan,
        const std::vector<ft_col_idx_t> colIndicesToScan,
        const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
        std::unique_ptr<BaseBFSMorsel>& bfsMorsel, uint32_t threadIdx);

    int64_t getNextAvailableSSSPWork(uint32_t threadIdx);

    std::pair<GlobalSSSPState, SSSPLocalState> findAvailableSSSPMorsel(
        std::unique_ptr<BaseBFSMorsel>& bfsMorsel, SSSPLocalState& ssspLocalState,
        uint32_t threadIdx);

    int64_t writeDstNodeIDAndDistance(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        const std::shared_ptr<common::ValueVector>& dstNodeIDVector,
        const std::shared_ptr<common::ValueVector>& distanceVector, common::table_id_t tableID,
        uint32_t threadIdx);

private:
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    uint32_t threadIdxCounter;
    std::vector<std::shared_ptr<SSSPMorsel>> activeSSSPMorsel;
    uint32_t numActiveSSSP;
    GlobalSSSPState globalState;
    std::mutex mutex;
};

} // namespace processor
} // namespace kuzu
