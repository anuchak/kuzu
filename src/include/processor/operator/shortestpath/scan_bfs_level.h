#pragma once

#include <bitset>
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
 * And only 1 thread -> 1 src SP computation so this ensures 1 thread -> 1 `BFSLevel` at a time.
 *
 * In next iterations we will have multiple threads helping to extend the same `BFSLevel` for which
 * we will implement a `BFSLevelThreadLocalState` for each thread extending a level and a final
 * `mergeBFSLevelLocalStates` function to merge all the nodes to the same global `BFSLevel`
 */
struct BFSLevel {
public:
    BFSLevel()
        : bfsLevelNodes{std::vector<common::nodeID_t>()}, bfsLevelScanStartIdx{0u}, bfsLevelHeight{
                                                                                        0u} {}

    common::nodeID_t getBFSLevelNodeID(common::offset_t nodeOffset) {
        for (auto& nodeID : bfsLevelNodes) {
            if (nodeID.offset == nodeOffset) {
                return nodeID;
            }
        }
    }

public:
    std::vector<common::nodeID_t> bfsLevelNodes;
    uint32_t bfsLevelScanStartIdx;
    uint32_t bfsLevelHeight;
};

struct SingleSrcSPState {
public:
    explicit SingleSrcSPState(common::offset_t maxNodeOffset)
        : nodeMask{std::vector<bool>(maxNodeOffset + 1, false)},
          currBFSLevel{std::make_unique<BFSLevel>()}, nextBFSLevel{std::make_unique<BFSLevel>()} {}

public:
    std::vector<bool> nodeMask;
    std::unique_ptr<FTableScanMorsel> srcDstSPMorsel;
    std::unique_ptr<BFSLevel> currBFSLevel;
    std::unique_ptr<BFSLevel> nextBFSLevel;
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

    SingleSrcSPState* grabSrcDstMorsel(std::thread::id threadID, common::offset_t maxNodeOffset);

public:
    std::shared_mutex mutex;
    std::unordered_map<std::thread::id, std::unique_ptr<SingleSrcSPState>> singleSrcSPTracker;
    std::shared_ptr<FTableSharedState> fTableOfSrcDst;
};

class ScanBFSLevel : public PhysicalOperator {

public:
    ScanBFSLevel(common::offset_t maxNodeOffset, const DataPos& bfsInputVectorDataPos, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator(PhysicalOperatorType::SCAN_BFS_LEVEL, id, paramsString),
          maxNodeOffset{maxNodeOffset}, bfsInputVectorDataPos{bfsInputVectorDataPos},
          simpleRecursiveJoinGlobalState(std::make_shared<SimpleRecursiveJoinGlobalState>()) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isSource() const override { return true; }

    uint32_t copyNodeIDsToVector(BFSLevel& currBFSLevel);

    bool getNextTuplesInternal() override;

    void rearrangeCurrBFSLevelNodes(SingleSrcSPState* singleSrcSPState) const;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanBFSLevel>(
            maxNodeOffset, bfsInputVectorDataPos, id, paramsString);
    }

private:
    std::thread::id threadID;
    common::offset_t maxNodeOffset;
    DataPos bfsInputVectorDataPos;
    std::shared_ptr<common::ValueVector> nodesToExtendValueVector;
    std::vector<DataPos> outVecPositions;
    std::vector<uint32_t> colIndicesToScan;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToScan;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};
} // namespace processor
} // namespace kuzu
