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

/*
 * Current Implementation:
 * Only 1 thread is assigned to each SSSP computation.
 * This ensures that at any point in time 1 thread works on 1 BFSLevel.
 * In later iterations, we will  have multiple threads working together to extend the same BFSLevel.
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

struct SSSPMorsel {
public:
    explicit SSSPMorsel(common::offset_t maxNodeOffset)
        : numDstNodesNotReached{0u}, curLevelNodeMask{std::vector<bool>(maxNodeOffset + 1, false)},
          dstNodeDistances{std::make_unique<std::unordered_map<common::offset_t, uint32_t>>()},
          curBFSLevel{std::make_unique<BFSLevel>()}, nextBFSLevel{std::make_unique<BFSLevel>()},
          bfsVisitedNodes{
              std::make_unique<std::vector<VisitedState>>(maxNodeOffset + 1, NOT_VISITED)} {}

    BFSLevelMorsel grabBFSLevelMorsel();

    void setDstNodeOffsets(std::shared_ptr<common::ValueVector>& valueVector);

public:
    uint32_t numDstNodesNotReached;
    std::shared_mutex mutex;
    std::vector<bool> curLevelNodeMask;
    std::unique_ptr<std::unordered_map<common::offset_t, uint32_t>> dstNodeDistances;
    std::unique_ptr<FTableScanMorsel> srcDstFTableMorsel;
    std::unique_ptr<BFSLevel> curBFSLevel;
    std::unique_ptr<BFSLevel> nextBFSLevel;
    std::unique_ptr<std::vector<VisitedState>> bfsVisitedNodes;
};

struct SimpleRecursiveJoinGlobalState {
public:
    SimpleRecursiveJoinGlobalState()
        : ssspMorselTracker{std::unordered_map<std::thread::id, std::unique_ptr<SSSPMorsel>>()} {};

    SSSPMorsel* getSSSPMorsel(std::thread::id threadID) {
        std::unique_lock<std::shared_mutex> lck{mutex};
        if (ssspMorselTracker.contains(threadID)) {
            return ssspMorselTracker[threadID].get();
        }
        return nullptr;
    }

    SSSPMorsel* grabSrcDstMorsel(std::thread::id threadID, common::offset_t maxNodeOffset);

public:
    std::shared_mutex mutex;
    std::unordered_map<std::thread::id, std::unique_ptr<SSSPMorsel>> ssspMorselTracker;
    std::shared_ptr<FTableSharedState> fTableOfSrcDst;
};

class ScanBFSLevel : public PhysicalOperator {

public:
    ScanBFSLevel(common::offset_t maxNodeOffset, const DataPos& nodesToExtendDataPos,
        std::vector<DataPos> srcDstNodeIDVectorsDataPos, const DataPos& dstBFSLevelVectorDataPos,
        std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeIDs, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator(PhysicalOperatorType::SCAN_BFS_LEVEL, id, paramsString),
          maxNodeOffset{maxNodeOffset}, nodesToExtendDataPos{nodesToExtendDataPos},
          srcDstNodeIDVectorsDataPos{std::move(srcDstNodeIDVectorsDataPos)},
          dstBFSLevelVectorDataPos{dstBFSLevelVectorDataPos},
          ftColIndicesOfSrcAndDstNodeIDs{std::move(ftColIndicesOfSrcAndDstNodeIDs)},
          ssspMorsel{nullptr},
          simpleRecursiveJoinGlobalState(std::make_shared<SimpleRecursiveJoinGlobalState>()) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isSource() const override { return true; }

    bool getNextTuplesInternal() override;

    void writeDistToOutputVector();

    void initializeNewSSSPMorsel();

    void initializeNextBFSLevel(BFSLevelMorsel& bfsLevelMorsel);

    void rearrangeCurBFSLevelNodes() const;

    void copyNodeIDsToVector(BFSLevel& curBFSLevel, BFSLevelMorsel& bfsLevelMorsel);

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanBFSLevel>(maxNodeOffset, nodesToExtendDataPos,
            srcDstNodeIDVectorsDataPos, dstBFSLevelVectorDataPos, ftColIndicesOfSrcAndDstNodeIDs,
            id, paramsString);
    }

private:
    std::thread::id threadID;
    common::offset_t maxNodeOffset;

    // The ValueVector into which ScanBFSLevel will write the nodes to be extended.
    DataPos nodesToExtendDataPos;
    std::shared_ptr<common::ValueVector> nodesToExtend;

    // The ValueVectors into which the src, dst nodeIDs will be written.
    std::vector<DataPos> srcDstNodeIDVectorsDataPos;
    std::vector<std::shared_ptr<common::ValueVector>> srcDstNodeIDVectors;

    // The ValueVector into which ScanBFSLevel will write the dst bfsLevelNumber.
    DataPos dstBFSLevelVectorDataPos;
    std::shared_ptr<common::ValueVector> dstBFSLevel;

    std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeIDs;
    SSSPMorsel* ssspMorsel;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};
} // namespace processor
} // namespace kuzu
