#pragma once

#include <map>
#include <utility>

#include "processor/operator/physical_operator.h"
#include "processor/operator/result_collector.h"
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
    BFSLevel() : nodeIDSelectedPos{std::vector<common::sel_t>()} {}

public:
    std::vector<common::sel_t> nodeIDSelectedPos;
};

struct SingleSrcSPState {
public:
    explicit SingleSrcSPState(common::offset_t maxNodeOffset)
        : nodeMask{std::vector<bool>(maxNodeOffset + 1, false)},
          bfsLevels{std::vector<std::unique_ptr<BFSLevel>>()},
          bfsVisitedNodes{std::unordered_set<common::offset_t>()} {}

    inline void setMask(uint64_t nodeOffset) { nodeMask[nodeOffset] = true; }

    inline bool isMasked(uint64_t pos) { return nodeMask[pos]; }

    inline void resetMask() { std::fill(nodeMask.begin(), nodeMask.end(), false); }

    void setSrcDestSPMorsel(std::unique_ptr<FTableScanMorsel> morsel) {
        srcDestSPMorsel = std::move(morsel);
    }

    std::unique_ptr<FTableScanMorsel>& getSrcDestSPMorsel() { return srcDestSPMorsel; }

    std::vector<std::unique_ptr<BFSLevel>>& getBFSLevels() { return bfsLevels; }

    std::unordered_set<common::offset_t> getVisitedNodes() { return bfsVisitedNodes; }

private:
    std::vector<bool> nodeMask;
    std::unique_ptr<FTableScanMorsel> srcDestSPMorsel;
    std::vector<std::unique_ptr<BFSLevel>> bfsLevels;
    std::unordered_set<common::offset_t> bfsVisitedNodes;
};

struct SimpleRecursiveJoinGlobalState {
public:
    SimpleRecursiveJoinGlobalState()
        : mutex{std::shared_mutex()},
          singleSrcSPTracker{
              std::unordered_map<std::thread::id, std::unique_ptr<SingleSrcSPState>>()} {};

    SingleSrcSPState* getSingleSrcSPState(std::thread::id threadID) {
        std::unique_lock<std::shared_mutex> lck{mutex};
        return singleSrcSPTracker[threadID].get();
    }

    void setBFSFTableSharedState(std::shared_ptr<FTableSharedState> sharedState) {
        fTableOfSrcDest = std::move(sharedState);
    }

    std::shared_ptr<FTableSharedState> getFTableOfSrcDest() { return fTableOfSrcDest; }

    SingleSrcSPState* grabSrcDestMorsel(
        std::thread::id threadID, common::offset_t maxNodeOffset, uint64_t maxMorselSize);

private:
    std::shared_mutex mutex;
    std::unordered_map<std::thread::id, std::unique_ptr<SingleSrcSPState>> singleSrcSPTracker;
    std::shared_ptr<FTableSharedState> fTableOfSrcDest;
};

class ScanBFSLevel : public PhysicalOperator {

public:
    ScanBFSLevel(common::offset_t maxNodeOffset, const DataPos& bfsInputVectorDataPos,
        const DataPos& bfsOutputVectorDataPos, uint32_t id, const std::string& paramsString)
        : PhysicalOperator(PhysicalOperatorType::SCAN_BFS_LEVEL, id, paramsString),
          maxNodeOffset{maxNodeOffset}, bfsInputVectorDataPos{bfsInputVectorDataPos},
          bfsOutputVectorDataPos{bfsOutputVectorDataPos},
          simpleRecursiveJoinGlobalState(std::make_shared<SimpleRecursiveJoinGlobalState>()) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isSource() const override { return true; }

    bool getNextTuplesInternal() override;

    // TODO: gets set in logical to physical plan mapping
    inline void setSharedState(std::shared_ptr<FTableSharedState> state) {
        simpleRecursiveJoinGlobalState->setBFSFTableSharedState(std::move(state));
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanBFSLevel>(
            maxNodeOffset, bfsInputVectorDataPos, bfsOutputVectorDataPos, id, paramsString);
    }

private:
    std::thread::id threadID;
    uint64_t maxMorselSize;
    common::offset_t maxNodeOffset;
    DataPos bfsInputVectorDataPos;
    DataPos bfsOutputVectorDataPos;
    std::shared_ptr<common::ValueVector> bfsInputValueVector;
    std::shared_ptr<common::ValueVector> bfsOutputValueVector;
    std::vector<DataPos> outVecPositions;
    std::vector<uint32_t> colIndicesToScan;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToScan;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};
} // namespace processor
} // namespace kuzu
