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

enum SSSPComputationState {
    SSSP_MORSEL_INCOMPLETE,
    SSSP_MORSEL_COMPLETE,
    SSSP_MORSEL_WRITING_COMPLETE,
    SSSP_COMPUTATION_COMPLETE
};

struct Frontier {
    std::vector<common::offset_t> nodeOffsets;

    inline uint64_t size() { return nodeOffsets.size(); }

    Frontier() = default;
    virtual ~Frontier() = default;
    inline virtual void resetState() { nodeOffsets.clear(); }
    inline virtual uint64_t getMultiplicity(common::offset_t offset) { return 1; }
};

struct SSSPMorsel {
public:
    SSSPMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : currentLevel{0u}, nextScanStartIdx{0u}, curBFSLevel{std::make_unique<Frontier>()},
          nextBFSLevel{std::make_unique<Frontier>()}, numVisitedNodes{0u},
          visitedNodes{std::vector<uint8_t>(maxNodeOffset_ + 1, NOT_VISITED)},
          distance{std::vector<uint16_t>(maxNodeOffset_ + 1, 0u)}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsActiveOnMorsel{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u},
          threadsWritingDstDistances{std::unordered_set<std::thread::id>()},
          lvlStartTimeInMillis{0u}, startTimeInMillis{0u}, distWriteStartTimeInMillis{0u} {}

    void reset(std::vector<common::offset_t>& targetDstNodeOffsets);

    // If BFS has completed.
    bool isComplete(uint64_t numDstNodesToVisit);
    // Mark src as visited.
    void markSrc(const std::vector<common::offset_t>& targetDstNodeOffsets);

    void moveNextLevelAsCurrentLevel();

public:
    // Level state
    uint8_t currentLevel;
    uint64_t nextScanStartIdx;
    std::unique_ptr<Frontier> curBFSLevel;
    std::unique_ptr<Frontier> nextBFSLevel;
    // Visited state
    uint64_t numVisitedNodes;
    std::vector<uint8_t> visitedNodes;
    std::vector<uint16_t> distance;
    // Offset of src node.
    common::offset_t srcOffset;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    uint32_t numThreadsActiveOnMorsel;
    uint64_t nextDstScanStartIdx;
    uint64_t inputFTableTupleIdx;
    std::unordered_set<std::thread::id> threadsWritingDstDistances;
    uint64_t lvlStartTimeInMillis;
    uint64_t startTimeInMillis;
    uint64_t distWriteStartTimeInMillis;
};

struct BaseBFSMorsel {

public:
    explicit BaseBFSMorsel(
        common::offset_t maxOffset, NodeOffsetSemiMask* semiMask, SSSPMorsel* ssspMorsel)
        : startScanIdx{0u}, endScanIdx{0u}, threadCheckSSSPState{true}, ssspMorsel{ssspMorsel},
          localNextBFSLevel{std::make_unique<Frontier>()}, localNumVisitedNodes{0u} {
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
        localNextBFSLevel->resetState();
        localNumVisitedNodes = 0u;
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
    std::unique_ptr<Frontier> localNextBFSLevel;
    uint64_t localNumVisitedNodes;
};

struct ShortestPathBFSMorsel : public BaseBFSMorsel {

    ShortestPathBFSMorsel(
        common::offset_t maxOffset, NodeOffsetSemiMask* semiMask, SSSPMorsel* ssspMorsel)
        : BaseBFSMorsel{maxOffset, semiMask, ssspMorsel} {}
};

struct MorselDispatcher {
public:
    MorselDispatcher(uint64_t lowerBound, uint64_t upperBound, uint64_t maxNodeOffset)
        : state{SSSP_MORSEL_INCOMPLETE}, ssspMorsel{std::make_unique<SSSPMorsel>(
                                             upperBound, lowerBound, maxNodeOffset)} {}

    inline SSSPMorsel* getSSSPMorsel() { return ssspMorsel.get(); }

    bool finishBFSMorsel(std::unique_ptr<BaseBFSMorsel>& bfsMorsel);

    SSSPComputationState getBFSMorsel(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        const std::vector<common::ValueVector*> vectorsToScan,
        const std::vector<ft_col_idx_t> colIndicesToScan,
        const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
        std::unique_ptr<BaseBFSMorsel>& bfsMorsel);

    int64_t writeDstNodeIDAndDistance(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        const std::shared_ptr<common::ValueVector>& dstNodeIDVector,
        const std::shared_ptr<common::ValueVector>& distanceVector, common::table_id_t tableID);

private:
    inline void resetSSSPComputationState() { state = SSSP_MORSEL_INCOMPLETE; }

private:
    SSSPComputationState state;
    std::shared_mutex mutex;
    std::unique_ptr<SSSPMorsel> ssspMorsel;
};

} // namespace processor
} // namespace kuzu
