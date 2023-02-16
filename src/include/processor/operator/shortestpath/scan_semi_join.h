#pragma once

#include <map>
#include <utility>

#include "processor/operator/physical_operator.h"
#include "processor/operator/scan_node_id.h"
#include "processor/operator/shortestpath/src_dest_collector.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;

namespace kuzu {
namespace processor {

struct SemiJoinFilter {
public:
    SemiJoinFilter(common::offset_t maxNodeOffset, uint8_t maskedFlag)
        : nodeMask{std::make_unique<Mask>(maxNodeOffset + 1, maskedFlag)} {}

    inline void setMask(uint64_t nodeOffset, uint8_t maskerIdx) {
        nodeMask->setMask(nodeOffset, maskerIdx, maskerIdx + 1);
    }

    inline bool isMasked(uint64_t pos) { return nodeMask->isMasked(pos); }

    inline void resetMask() {
        // TODO: the Mask struct does not expose the val directly to reset
        // This is needed while extending from BFS {i} to BFS {i+1}
    }

private:
    std::unique_ptr<Mask> nodeMask;
};

struct BFSLevelNodeState {
    uint64_t parentNodeID;
    uint64_t relParentID;

    BFSLevelNodeState(uint64_t parentNodeID, uint64_t relParentID)
        : parentNodeID{parentNodeID}, relParentID{relParentID} {}
};

struct BFSLevel {
public:
    BFSLevel()
        : bfsLevelNodes{std::map<uint64_t, BFSLevelNodeState>()}, bfsLevelLock{std::mutex()} {}

    bool levelContainsNode(uint64_t nodeID) {
        return bfsLevelNodes.contains(nodeID);
    }

public:
    std::mutex bfsLevelLock;

private:
    std::map<uint64_t, BFSLevelNodeState> bfsLevelNodes;
};

struct LocalSharedState {
public:
    LocalSharedState(common::offset_t maxNodeOffset, uint8_t maskedFlag)
        : semiJoinFilter{std::make_shared<SemiJoinFilter>(maxNodeOffset, maskedFlag)},
          bfsLevels{std::vector<BFSLevel>()} {};

    std::shared_ptr<SemiJoinFilter> getSemiJoinFilter() { return semiJoinFilter; }

    void setBFSMorsel(std::unique_ptr<BFSScanMorsel> morsel) { bfsScanMorsel = std::move(morsel); }

    std::shared_ptr<BFSScanMorsel> getBFSScanMorsel() { return bfsScanMorsel; }

    std::vector<BFSLevel>& getBFSLevels() {
        return bfsLevels;
    }

private:
    std::shared_ptr<SemiJoinFilter> semiJoinFilter;
    std::shared_ptr<BFSScanMorsel> bfsScanMorsel;
    std::vector<BFSLevel> bfsLevels;
};

struct SimpleRecursiveJoinSharedState {
public:
    SimpleRecursiveJoinSharedState()
        : localStateTracker{std::map<std::thread::id, std::shared_ptr<LocalSharedState>>()} {};

    std::map<std::thread::id, std::shared_ptr<LocalSharedState>> getLocalStateTracker() {
        return localStateTracker;
    }

    void setBFSFTableSharedState(std::shared_ptr<BFSFTableSharedState> sharedState) {
        bfsfTableSharedState = std::move(sharedState);
    }

    std::shared_ptr<BFSFTableSharedState> getBFSFTableSharedState() { return bfsfTableSharedState; }

    std::shared_ptr<BFSScanMorsel> grabMorsel(
        std::thread::id threadID, common::offset_t maxNodeOffset, uint64_t maxMorselSize);

private:
    std::map<std::thread::id, std::shared_ptr<LocalSharedState>> localStateTracker;
    std::shared_ptr<BFSFTableSharedState> bfsfTableSharedState;
};

class RecursiveScanSemiJoin : public PhysicalOperator {

public:
    RecursiveScanSemiJoin(
        common::offset_t maxNodeOffset, uint32_t id, const std::string& paramsString)
        : PhysicalOperator(PhysicalOperatorType::RECURSIVE_SCAN_SEMI_JOIN, id, paramsString),
          maxNodeOffset{maxNodeOffset},
          simpleRecursiveJoinSharedState(std::make_shared<SimpleRecursiveJoinSharedState>()) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isSource() const override { return true; }

    bool getNextTuplesInternal() override;

    // TODO: gets set in logical to physical plan mapping
    inline void setSharedState(std::shared_ptr<BFSFTableSharedState> state) {
        simpleRecursiveJoinSharedState->setBFSFTableSharedState(std::move(state));
    }

    inline void setMaxMorselSize() {
        maxMorselSize =
            simpleRecursiveJoinSharedState->getBFSFTableSharedState()->getMaxMorselSize();
    }

    inline std::shared_ptr<BFSScanMorsel> grabMorsel() {
        return simpleRecursiveJoinSharedState->grabMorsel(
            std::this_thread::get_id(), maxNodeOffset, maxMorselSize);
    }

    inline std::shared_ptr<SimpleRecursiveJoinSharedState> getSimpleRecursiveJoinSharedState() {
        return simpleRecursiveJoinSharedState;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<RecursiveScanSemiJoin>(maxNodeOffset, id, paramsString);
    }

private:
    uint64_t maxMorselSize;
    common::offset_t maxNodeOffset;
    std::vector<DataPos> outVecPositions;
    std::vector<uint32_t> colIndicesToScan;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToScan;
    std::shared_ptr<SimpleRecursiveJoinSharedState> simpleRecursiveJoinSharedState;
};
} // namespace processor
} // namespace kuzu
