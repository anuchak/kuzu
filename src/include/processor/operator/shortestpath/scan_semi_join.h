#pragma once

#include <map>
#include <utility>

#include "processor/operator/physical_operator.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/scan_node_id.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;

namespace kuzu {
namespace processor {

/***
 * Current Implementation:
 *
 * Only 1 thread is working on extending a `BFSLevel` to the next level.
 * And only 1 thread -> 1 src SP computation so this ensures 1 thread -> 1 `BFSLevel` at a time.
 *
 * In next iterations we will have multiple threads helping to extend the same `BFSLevel` for which
 * we will implement a `BFSLevelThreadLocalState` for each thread extending a level and a final
 * `mergeToLevel` function to merge all the nodes to the same global `BFSLevel`
 */
struct BFSLevel {
public:
    BFSLevel() : bfsLevelLock{std::mutex()},
          levelMinNodeOffset{UINT64_MAX},
          levelMaxNodeOffset{0},
          currLevelNodeOffsets{std::vector<common::offset_t>()},
    parentNodeOffsets{std::vector<common::offset_t>()} {}

public:
    // TODO: This bfs level lock is not being used now because only 1 thread extends 1 level.
    std::mutex bfsLevelLock;
    common::offset_t levelMinNodeOffset;
    common::offset_t levelMaxNodeOffset;
    std::vector<common::offset_t> currLevelNodeOffsets;
    std::vector<common::offset_t> parentNodeOffsets;
};

struct SingleSrcSPState {
public:
    explicit SingleSrcSPState(std::thread::id threadID, common::offset_t maxNodeOffset)
        : threadID{threadID}, maxNodeOffset{maxNodeOffset}, nodeMask{std::make_unique<Mask>(
                                                                maxNodeOffset + 1,
                                                                1 /* Masked Flag */)},
          bfsLevels{std::vector<std::unique_ptr<BFSLevel>>()},
          bfsVisitedNodes{std::unordered_set<common::offset_t>()} {};

    inline std::thread::id getThreadID() { return threadID; }

    inline void setMask(uint64_t nodeOffset, uint8_t maskerIdx) {
        nodeMask->setMask(nodeOffset, maskerIdx, maskerIdx);
    }

    inline bool isMasked(uint64_t pos) { return nodeMask->isMasked(pos); }

    inline void resetMask() { nodeMask->resetMask(maxNodeOffset + 1); }

    void setSrcDestSPMorsel(std::unique_ptr<FTableScanMorsel> morsel) {
        srcDestSPMorsel = std::move(morsel);
    }

    std::unique_ptr<FTableScanMorsel>& getSrcDestSPMorsel() { return srcDestSPMorsel; }

    std::vector<std::unique_ptr<BFSLevel>>& getBFSLevels() { return bfsLevels; }

    std::unordered_set<common::offset_t> getVisitedNodes() { return bfsVisitedNodes; }

private:
    std::thread::id threadID;
    common::offset_t maxNodeOffset;
    std::unique_ptr<Mask> nodeMask;
    std::unique_ptr<FTableScanMorsel> srcDestSPMorsel;
    std::vector<std::unique_ptr<BFSLevel>> bfsLevels;
    std::unordered_set<common::offset_t> bfsVisitedNodes;
};

struct SimpleRecursiveJoinGlobalState {
public:
    SimpleRecursiveJoinGlobalState()
        : mutex{std::shared_mutex()}, singleSrcSPTracker{
                                          std::vector<std::unique_ptr<SingleSrcSPState>>()} {};

    SingleSrcSPState* getSingleSrcSPState(std::thread::id threadID) {
        std::unique_lock<std::shared_mutex> lck{mutex};
        for (auto& singleSrcSP : singleSrcSPTracker) {
            if (singleSrcSP->getThreadID() == threadID) {
                return singleSrcSP.get();
            }
        }
        assert(false);
    }

    void setBFSFTableSharedState(std::shared_ptr<FTableSharedState> sharedState) {
        fTableOfSrcDest = std::move(sharedState);
    }

    std::shared_ptr<FTableSharedState> getFTableOfSrcDest() { return fTableOfSrcDest; }

    SingleSrcSPState* grabSrcDestMorsel(
        std::thread::id threadID, common::offset_t maxNodeOffset, uint64_t maxMorselSize);

private:
    std::shared_mutex mutex;
    std::vector<std::unique_ptr<SingleSrcSPState>> singleSrcSPTracker;
    std::shared_ptr<FTableSharedState> fTableOfSrcDest;
};

class RecursiveScanSemiJoin : public PhysicalOperator {

public:
    RecursiveScanSemiJoin(
        common::offset_t maxNodeOffset, uint32_t id, const std::string& paramsString)
        : PhysicalOperator(PhysicalOperatorType::RECURSIVE_SCAN_SEMI_JOIN, id, paramsString),
          maxNodeOffset{maxNodeOffset},
          simpleRecursiveJoinGlobalState(std::make_shared<SimpleRecursiveJoinGlobalState>()) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isSource() const override { return true; }

    bool getNextTuplesInternal() override;

    // TODO: gets set in logical to physical plan mapping
    inline void setSharedState(std::shared_ptr<FTableSharedState> state) {
        simpleRecursiveJoinGlobalState->setBFSFTableSharedState(std::move(state));
    }

    inline void setMaxMorselSize() {
        maxMorselSize = simpleRecursiveJoinGlobalState->getFTableOfSrcDest()->getMaxMorselSize();
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<RecursiveScanSemiJoin>(maxNodeOffset, id, paramsString);
    }

private:
    std::thread::id threadID;
    uint64_t maxMorselSize;
    common::offset_t maxNodeOffset;
    std::vector<DataPos> outVecPositions;
    std::vector<uint32_t> colIndicesToScan;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToScan;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};
} // namespace processor
} // namespace kuzu
