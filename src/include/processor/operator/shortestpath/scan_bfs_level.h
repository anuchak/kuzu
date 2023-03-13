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
 * Only 1 thread is assigned to each SSSP computation. This ensures that at any point in time 1
 * thread works on 1 BFSLevel.
 */
struct BFSLevel {
public:
    BFSLevel() : bfsLevelNodes{std::vector<common::nodeID_t>()}, levelNumber{0u} {}

    inline bool isEmpty() const { return bfsLevelNodes.empty(); }

public:
    std::vector<common::nodeID_t> bfsLevelNodes;
    uint32_t levelNumber;
};

struct BFSLevelMorsel {
public:
    BFSLevelMorsel(uint32_t startIdx, uint32_t size)
        : startIdx{startIdx}, size{size} {}

    inline bool isEmpty() const { return size == 0u; }

public:
    uint32_t startIdx;
    uint32_t size;
};

struct SSSPMorsel {
public:
    explicit SSSPMorsel(common::offset_t maxNodeOffset)
        : isSSSPMorselComplete{false}, numDstNodesNotReached{0u}, bfsMorselNextStartIdx{0u},
          dstTableID{0u}, nextLevelNodeMask{std::vector<bool>(maxNodeOffset + 1, false)},
          dstNodeDistances{std::make_unique<std::unordered_map<common::offset_t, uint32_t>>()},
          curBFSLevel{std::make_unique<BFSLevel>()}, nextBFSLevel{std::make_unique<BFSLevel>()},
          bfsVisitedNodes{std::make_unique<std::vector<uint8_t>>(maxNodeOffset + 1, NOT_VISITED)} {}

    BFSLevelMorsel grabBFSLevelMorsel();

    void markDstNodeOffsets(
        common::offset_t srcNodeOffset, std::shared_ptr<common::ValueVector>& dstNodeIDValueVector);

public:
    std::shared_mutex mutex;
    bool isSSSPMorselComplete;
    uint32_t numDstNodesNotReached;
    uint32_t bfsMorselNextStartIdx;
    common::table_id_t dstTableID;
    std::vector<bool> nextLevelNodeMask;
    std::unique_ptr<std::unordered_map<common::offset_t, uint32_t>> dstNodeDistances;
    std::unique_ptr<FTableScanMorsel> srcDstFTableMorsel;
    std::unique_ptr<BFSLevel> curBFSLevel;
    std::unique_ptr<BFSLevel> nextBFSLevel;
    // Each element is of size 1 byte (unsigned char) and we store members of VisitedState enum
    std::unique_ptr<std::vector<uint8_t>> bfsVisitedNodes;
};

struct SimpleRecursiveJoinGlobalState {
public:
    explicit SimpleRecursiveJoinGlobalState(std::shared_ptr<FTableSharedState> fTableOfSrcDst)
        : ssspMorselTracker{std::unordered_map<std::thread::id, std::unique_ptr<SSSPMorsel>>()},
          fTableOfSrcDst{std::move(fTableOfSrcDst)} {};

    SSSPMorsel* getAssignedSSSPMorsel(std::thread::id threadID);

    SSSPMorsel* grabAndInitializeSSSPMorsel(std::thread::id threadID,
        common::offset_t maxNodeOffset,
        std::vector<std::shared_ptr<common::ValueVector>> srcDstNodeIDVectors,
        std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeIDs,
        std::vector<std::shared_ptr<common::ValueVector>> srcDstNodePropertiesVectors,
        std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeProperties);

    SSSPMorsel* getSSSPMorsel(std::thread::id threadID, common::offset_t maxNodeOffset);

public:
    std::shared_mutex mutex;
    std::unordered_map<std::thread::id, std::unique_ptr<SSSPMorsel>> ssspMorselTracker;
    std::shared_ptr<FTableSharedState> fTableOfSrcDst;
};

class ScanBFSLevel : public PhysicalOperator {

public:
    ScanBFSLevel(common::offset_t maxNodeOffset, const DataPos& nodesToExtendDataPos,
        std::vector<DataPos> srcDstNodeIDVectorsDataPos,
        std::vector<DataPos> srcDstNodePropertiesVectorsDataPos,
        const DataPos& dstDistanceVectorDataPos,
        std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeIDs,
        std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeProperties,
        std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator(
              PhysicalOperatorType::SCAN_BFS_LEVEL, std::move(child), id, paramsString),
          maxNodeOffset{maxNodeOffset}, nodesToExtendDataPos{nodesToExtendDataPos},
          srcDstNodeIDVectorsDataPos{std::move(srcDstNodeIDVectorsDataPos)},
          srcDstNodePropertiesVectorsDataPos{std::move(srcDstNodePropertiesVectorsDataPos)},
          dstDistanceVectorDataPos{dstDistanceVectorDataPos},
          ftColIndicesOfSrcAndDstNodeIDs{std::move(ftColIndicesOfSrcAndDstNodeIDs)},
          ftColIndicesOfSrcAndDstNodeProperties{std::move(ftColIndicesOfSrcAndDstNodeProperties)},
          ssspMorsel{nullptr}, simpleRecursiveJoinGlobalState{
                                   std::move(simpleRecursiveJoinGlobalState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isSource() const override { return true; }

    bool getNextTuplesInternal() override;

    void writeDistToOutputVector();

    void rearrangeCurBFSLevelNodes() const;

    void copyNodeIDsToVector(BFSLevel& curBFSLevel, BFSLevelMorsel& bfsLevelMorsel);

    std::shared_ptr<SimpleRecursiveJoinGlobalState>& getSimpleRecursiveJoinGlobalState() {
        return simpleRecursiveJoinGlobalState;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanBFSLevel>(maxNodeOffset, nodesToExtendDataPos,
            srcDstNodeIDVectorsDataPos, srcDstNodePropertiesVectorsDataPos,
            dstDistanceVectorDataPos, ftColIndicesOfSrcAndDstNodeIDs,
            ftColIndicesOfSrcAndDstNodeProperties, simpleRecursiveJoinGlobalState,
            children[0]->clone(), id, paramsString);
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
    // The ValueVectors into which the src, dst node properties will be written.
    std::vector<DataPos> srcDstNodePropertiesVectorsDataPos;
    std::vector<std::shared_ptr<common::ValueVector>> srcDstNodePropertiesVectors;
    // The ValueVector into which ScanBFSLevel will write the dst bfsLevelNumber.
    DataPos dstDistanceVectorDataPos;
    std::shared_ptr<common::ValueVector> dstDistances;
    // The FTable column indices for the src, dest nodeIDs and node properties to scan.
    std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeIDs;
    std::vector<uint32_t> ftColIndicesOfSrcAndDstNodeProperties;
    SSSPMorsel* ssspMorsel;
    std::shared_ptr<SimpleRecursiveJoinGlobalState> simpleRecursiveJoinGlobalState;
};
} // namespace processor
} // namespace kuzu
