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

enum VisitedState { NOT_VISITED, NOT_VISITED_DST, VISITED, VISITED_DST };

/***
 * Current Implementation:
 *
 * Only 1 thread -> 1 src SP computation so this ensures 1 thread -> 1 BFSLevel at a time.
 *
 * In next iterations we will have multiple threads helping to extend the same BFSLevel for which
 * we will implement a BFSLevelThreadLocalState for each thread extending a level and a final
 * mergeBFSLevelLocalStates function to merge all the nodes to the same global BFSLevel
 */
struct BFSLevel {
public:
    BFSLevel()
        : bfsLevelNodes{std::vector<common::nodeID_t>()}, bfsLevelScanStartIdx{0u}, bfsLevelNumber{
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
    uint32_t bfsLevelNumber;
};

struct BFSLevelMorsel {
public:
    BFSLevelMorsel(uint32_t bfsLevelScanStartIdx, uint32_t bfsLevelMorselSize)
        : bfsLevelScanStartIdx{bfsLevelScanStartIdx}, bfsLevelMorselSize{bfsLevelMorselSize} {}

public:
    uint32_t bfsLevelScanStartIdx;
    uint32_t bfsLevelMorselSize;
};

struct SingleSrcSPMorsel {
public:
    explicit SingleSrcSPMorsel(common::offset_t maxNodeOffset)
        : mutex{std::shared_mutex()}, nodeMask{std::vector<bool>(maxNodeOffset + 1, false)},
          dstNodeDistances{std::make_unique<std::vector<uint32_t>>(maxNodeOffset + 1, 0u)},
          currBFSLevel{std::make_unique<BFSLevel>()}, nextBFSLevel{std::make_unique<BFSLevel>()},
          bfsVisitedNodes{
              std::make_unique<std::vector<VisitedState>>(maxNodeOffset + 1, NOT_VISITED)} {}

    BFSLevelMorsel grabBFSLevelMorsel();

    void setDstNodeOffsets(std::shared_ptr<common::ValueVector>& valueVector) const;

public:
    std::shared_mutex mutex;
    std::vector<bool> nodeMask;
    std::unique_ptr<std::vector<uint32_t>> dstNodeDistances;
    std::unique_ptr<FTableScanMorsel> srcDstFTableMorsel;
    std::unique_ptr<BFSLevel> currBFSLevel;
    std::unique_ptr<BFSLevel> nextBFSLevel;
    std::unique_ptr<std::vector<VisitedState>> bfsVisitedNodes;
};

struct SimpleRecursiveJoinGlobalState {
public:
    SimpleRecursiveJoinGlobalState()
        : mutex{std::shared_mutex()},
          ssSPMorselTracker{
              std::unordered_map<std::thread::id, std::unique_ptr<SingleSrcSPMorsel>>()} {};

    SingleSrcSPMorsel* getSingleSrcSPMorsel(std::thread::id threadID) {
        std::unique_lock<std::shared_mutex> lck{mutex};
        if (ssSPMorselTracker.contains(threadID)) {
            return ssSPMorselTracker[threadID].get();
        }
        return nullptr;
    }

    SingleSrcSPMorsel* grabSrcDstMorsel(std::thread::id threadID, common::offset_t maxNodeOffset);

public:
    std::shared_mutex mutex;
    std::unordered_map<std::thread::id, std::unique_ptr<SingleSrcSPMorsel>> ssSPMorselTracker;
    std::shared_ptr<FTableSharedState> fTableOfSrcDst;
};

class ScanBFSLevel : public PhysicalOperator {

public:
    ScanBFSLevel(common::offset_t maxNodeOffset, const DataPos& nodesToExtendDataPos,
        std::vector<DataPos> inputValVectorPos, const DataPos& outputValVectorPos,
        std::vector<uint32_t> colIndicesToScan, uint32_t id, const std::string& paramsString)
        : PhysicalOperator(PhysicalOperatorType::SCAN_BFS_LEVEL, id, paramsString),
          maxNodeOffset{maxNodeOffset}, nodesToExtendDataPos{nodesToExtendDataPos},
          inputValVectorPos{std::move(inputValVectorPos)}, outputValVectorPos{outputValVectorPos},
          colIndicesToScan{std::move(colIndicesToScan)},
          simpleRecursiveJoinGlobalState(std::make_shared<SimpleRecursiveJoinGlobalState>()) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isSource() const override { return true; }

    bool getNextTuplesInternal() override;

    void writeDistToOutputVector();

    void initializeNewSSSPMorsel();

    void initializeNextBFSLevel(BFSLevelMorsel& bfsLevelMorsel);

    void rearrangeCurrBFSLevelNodes() const;

    void copyNodeIDsToVector(BFSLevel& currBFSLevel, BFSLevelMorsel& bfsLevelMorsel);

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanBFSLevel>(maxNodeOffset, nodesToExtendDataPos,
            inputValVectorPos, outputValVectorPos, colIndicesToScan, id, paramsString);
    }

private:
    std::thread::id threadID;
    common::offset_t maxNodeOffset;

    // The value vector into which scan bfs level will write the nodes to be extended.
    DataPos nodesToExtendDataPos;
    std::shared_ptr<common::ValueVector> nodesToExtendValueVector;

    // The value vectors into which scan bfs level will write the src, dst nodeIDs.
    std::vector<DataPos> inputValVectorPos;
    std::vector<std::shared_ptr<common::ValueVector>> vectorsToScan;

    // The value vector into which scan bfs level will write the dst bfs level number.
    DataPos outputValVectorPos;
    std::shared_ptr<common::ValueVector> outputValueVector;

    std::vector<uint32_t> colIndicesToScan;
    SingleSrcSPMorsel* singleSrcSPMorsel;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};
} // namespace processor
} // namespace kuzu
