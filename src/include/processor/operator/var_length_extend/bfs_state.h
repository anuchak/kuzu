#pragma once

#include "processor/operator/result_collector.h"

namespace kuzu {
namespace processor {

enum VisitedState : uint8_t {
    NOT_VISITED = 0,
    VISITED = 1,
};

enum SSSPComputationState {
    SSSP_MORSEL_INCOMPLETE,
    SSSP_MORSEL_COMPLETE,
    SSSP_COMPUTATION_COMPLETE
};

struct BFSLevel {
public:
    BFSLevel() : bfsLevelNodes{std::vector<common::offset_t>()} {}

    inline void resetState() { bfsLevelNodes.clear(); }
    inline uint64_t size() const { return bfsLevelNodes.size(); }

public:
    std::vector<common::offset_t> bfsLevelNodes;
};

struct SSSPMorsel {
public:
    SSSPMorsel(common::offset_t maxOffset_, uint8_t upperBound_)
        : currentLevel{0u}, nextScanStartIdx{0u}, curBFSLevel{std::shared_ptr<BFSLevel>()},
          nextBFSLevel{std::shared_ptr<BFSLevel>()}, numVisitedNodes{0u},
          visitedNodes{std::vector<uint8_t>(maxOffset_ + 1, NOT_VISITED)},
          distance{std::unordered_map<common::offset_t, uint16_t>()}, srcOffset{0u},
          maxOffset{maxOffset_}, upperBound{upperBound_}, numThreadsActiveOnMorsel{0u} {}

    void reset();

    // If BFS has completed.
    bool isComplete();
    // Mark src as visited.
    void markSrc();
    // UnMark src as NOT visited to avoid outputting src which has length 0 path and thus should be
    // omitted.
    void unmarkSrc();
    // Mark node as visited.
    void markVisited(common::offset_t offset);
    void moveNextLevelAsCurrentLevel();

public:
    // Level state
    uint8_t currentLevel;
    uint64_t nextScanStartIdx;
    std::shared_ptr<BFSLevel> curBFSLevel;
    std::shared_ptr<BFSLevel> nextBFSLevel;
    // Visited state
    uint64_t numVisitedNodes;
    std::vector<uint8_t> visitedNodes;
    std::unordered_map<common::offset_t, uint16_t> distance;
    // Offset of src node.
    common::offset_t srcOffset;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint8_t upperBound;
    uint32_t numThreadsActiveOnMorsel;
};

struct BFSMorsel {
public:
    BFSMorsel(uint64_t startScanIdx, uint64_t endScanIdx)
        : startScanIdx{startScanIdx}, endScanIdx{endScanIdx},
          localBFSVisitedNodes{std::unordered_set<common::offset_t>()} {}

    common::offset_t getNextNodeOffset();

    void addToLocalNextBFSLevel(const std::shared_ptr<common::ValueVector>& tmpDstNodeIDVector);

public:
    uint64_t startScanIdx;
    uint64_t endScanIdx;
    std::unordered_set<common::offset_t> localBFSVisitedNodes;
    std::shared_ptr<BFSLevel> curBFSLevel;
};

struct MorselDispatcher {
public:
    MorselDispatcher(uint8_t upperBound, common::offset_t maxNodeOffset)
        : state{SSSP_MORSEL_INCOMPLETE}, ssspMorsel{std::make_unique<SSSPMorsel>(
                                             upperBound, maxNodeOffset)} {}

    inline void resetSSSPComputationState() {
        std::unique_lock lck{mutex};
        if (state != SSSP_MORSEL_INCOMPLETE) {
            state = SSSP_MORSEL_INCOMPLETE;
        }
    }

    bool finishBFSMorsel(BFSMorsel* bfsMorsel);

    std::pair<SSSPComputationState, BFSMorsel*> getBFSMorsel(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        const std::shared_ptr<common::ValueVector>& srcNodeIDVector, BFSMorsel* bfsMorsel);

private:
    SSSPComputationState state;
    std::shared_mutex mutex;
    std::unique_ptr<SSSPMorsel> ssspMorsel;
};

} // namespace processor
} // namespace kuzu
