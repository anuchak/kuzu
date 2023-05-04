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
    SSSPMorsel(common::offset_t maxOffset_, uint64_t upperBound_, uint64_t lowerBound_)
        : currentLevel{0u}, nextScanStartIdx{0u}, curBFSLevel{std::make_shared<BFSLevel>()},
          nextBFSLevel{std::make_shared<BFSLevel>()}, numVisitedNodes{0u},
          visitedNodes{std::make_shared<std::vector<uint8_t>>(maxOffset_ + 1, NOT_VISITED)},
          distance{std::unordered_map<common::offset_t, uint16_t>()}, srcOffset{0u},
          maxOffset{maxOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsActiveOnMorsel{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u},
          threadsWritingDstDistances{std::unordered_set<std::thread::id>()} {}

    void reset();

    // If BFS has completed.
    bool isComplete();
    // Mark src as visited.
    void markSrc();

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
    std::shared_ptr<std::vector<uint8_t>> visitedNodes;
    std::unordered_map<common::offset_t, uint16_t> distance;
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
};

struct BFSMorsel {
public:
    BFSMorsel(uint64_t startScanIdx, uint64_t endScanIdx,
        std::shared_ptr<std::vector<uint8_t>>& visitedNodes, std::shared_ptr<BFSLevel>& curBFSLevel)
        : startScanIdx{startScanIdx}, endScanIdx{endScanIdx},
          localBFSVisitedNodes{std::unordered_set<common::offset_t>()}, visitedNodes{visitedNodes},
          curBFSLevel{curBFSLevel} {}

    common::offset_t getNextNodeOffset();

    void addToLocalNextBFSLevel(const std::shared_ptr<common::ValueVector>& tmpDstNodeIDVector);

public:
    uint64_t startScanIdx;
    uint64_t endScanIdx;
    std::unordered_set<common::offset_t> localBFSVisitedNodes;
    std::shared_ptr<std::vector<uint8_t>> visitedNodes;
    std::shared_ptr<BFSLevel> curBFSLevel;
};

struct MorselDispatcher {
public:
    MorselDispatcher(uint64_t lowerBound, uint64_t upperBound, common::offset_t maxNodeOffset)
        : state{SSSP_MORSEL_INCOMPLETE}, morselSize{common::DEFAULT_VECTOR_CAPACITY},
          numThreads{0u}, ssspMorsel{std::make_unique<SSSPMorsel>(
                              maxNodeOffset, upperBound, lowerBound)} {}

    inline void setNumThreads(uint64_t numThreads_) { numThreads = numThreads_; }

    bool finishBFSMorsel(std::unique_ptr<BFSMorsel>& bfsMorsel);

    SSSPComputationState getBFSMorsel(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        const std::vector<common::ValueVector*> vectorsToScan,
        const std::vector<ft_col_idx_t> colIndicesToScan,
        const std::shared_ptr<common::ValueVector>& srcNodeIDVector,
        std::unique_ptr<BFSMorsel>& bfsMorsel);

    int64_t writeDstNodeIDAndDistance(
        const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        const std::shared_ptr<common::ValueVector>& dstNodeIDVector,
        const std::shared_ptr<common::ValueVector>& distanceVector, common::table_id_t tableID);

private:
    inline void resetSSSPComputationState() { state = SSSP_MORSEL_INCOMPLETE; }

private:
    SSSPComputationState state;
    uint64_t morselSize;
    uint64_t numThreads;
    std::shared_mutex mutex;
    std::unique_ptr<SSSPMorsel> ssspMorsel;
};

} // namespace processor
} // namespace kuzu
