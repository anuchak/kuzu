#pragma once

#include <algorithm>
#include <mutex>
#include <thread>

#include "frontier.h"
#include <common/enums/query_rel_type.h>
#include <common/vector/value_vector.h>
#include <planner/operator/extend/recursive_join_type.h>
#include <processor/operator/table_scan/ftable_scan_function.h>

namespace kuzu {
namespace processor {

struct RecursiveJoinVectors {
    common::ValueVector* srcNodeIDVector = nullptr;
    common::ValueVector* dstNodeIDVector = nullptr;
    common::ValueVector* pathLengthVector = nullptr;
    common::ValueVector* pathVector = nullptr;               // STRUCT(LIST(NODE), LIST(REL))
    common::ValueVector* pathNodesVector = nullptr;          // LIST(NODE)
    common::ValueVector* pathNodesIDDataVector = nullptr;    // INTERNAL_ID
    common::ValueVector* pathNodesLabelDataVector = nullptr; // STRING
    common::ValueVector* pathRelsVector = nullptr;           // LIST(REL)
    common::ValueVector* pathRelsSrcIDDataVector = nullptr;  // INTERNAL_ID
    common::ValueVector* pathRelsDstIDDataVector = nullptr;  // INTERNAL_ID
    common::ValueVector* pathRelsIDDataVector = nullptr;     // INTERNAL_ID
    common::ValueVector* pathRelsLabelDataVector = nullptr;  // STRING

    common::ValueVector* recursiveEdgeIDVector = nullptr;
    common::ValueVector* recursiveDstNodeIDVector = nullptr;
    common::ValueVector* recursiveNodePredicateExecFlagVector = nullptr;
};

/**
 * States used for nTkS scheduler to mark different status of node offsets.
 * VISITED_NEW and VISITED_DST_NEW are the states when a node is initially visited and
 * when we scan to prepare the bfsLevelNodes vector for next level extension,
 * VISITED_NEW -> VISITED | VISITED_DST_NEW -> VISITED_DST
 */
enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
};

/**
 * The Lifecycle of an BFSSharedState in nTkS scheduler are the 3 following states. Depending on
 * which state an BFSSharedState is in, the thread will held that state :-
 *
 * 1) EXTEND_IN_PROGRESS: thread helps in level extension, try to get a morsel if available
 *
 * 2) PATH_LENGTH_WRITE_IN_PROGRESS: thread helps in writing path length to vector
 *
 * 3) MORSEL_COMPLETE: morsel is complete, try to launch another BFSSharedState (if available),
 *                     else find an existing one
 */
enum SSSPLocalState {
    EXTEND_IN_PROGRESS,
    PATH_LENGTH_WRITE_IN_PROGRESS,
    MORSEL_COMPLETE,

    // NOTE: This is an intermediate state returned ONLY when no work could be provided to a thread.
    // It will not be assigned to the local SSSPLocalState inside a BFSSharedState.
    NO_WORK_TO_SHARE
};

/**
 * Global states of MorselDispatcher used primarily for nTkS scheduler :-
 *
 * 1) IN_PROGRESS: Globally (across all threads active), there is still work available.
 * BFSSharedStates are available for threads to launch.
 *
 * 2) IN_PROGRESS_ALL_SRC_SCANNED: All BFSSharedState available to be launched have been launched.
 * It indicates that the inputFTable has no more tuples to be scanned. The threads have to search
 * for an active BFSSharedState (either in EXTEND_IN_PROGRESS / PATH_LENGTH_WRITE_IN_PROGRESS) to
 * execute.
 *
 * 3) COMPLETE: All BFSSharedStates have been completed, there is no more work either in BFS
 * extension or path length writing to vector. Threads can exit completely (return false and return
 * to parent operator).
 */
enum GlobalSSSPState { IN_PROGRESS, IN_PROGRESS_ALL_SRC_SCANNED, COMPLETE };

// Target dst nodes are populated from semi mask and is expected to have small size.
// TargetDstNodeOffsets is empty if no semi mask available. Note that at the end of BFS, we may
// not visit all target dst nodes because they may simply not connect to src.
class TargetDstNodes {
public:
    TargetDstNodes(uint64_t numNodes, common::node_id_set_t nodeIDs)
        : numNodes{numNodes}, nodeIDs{std::move(nodeIDs)} {}

    inline void setTableIDFilter(std::unordered_set<common::table_id_t> filter) {
        tableIDFilter = std::move(filter);
    }

    inline bool contains(common::nodeID_t nodeID) {
        if (nodeIDs.empty()) {           // no semi mask available
            if (tableIDFilter.empty()) { // no dst table ID filter available
                return true;
            }
            return tableIDFilter.contains(nodeID.tableID);
        }
        return nodeIDs.contains(nodeID);
    }

    inline uint64_t getNumNodes() const { return numNodes; }

    inline common::node_id_set_t getNodeIDs() const { return nodeIDs; }

private:
    uint64_t numNodes;
    common::node_id_set_t nodeIDs;
    std::unordered_set<common::table_id_t> tableIDFilter;
};

struct BaseBFSState;

struct edgeListAndLevel;

/**
 * To track each edge offset and the source / bound node from where the neighbour node was
 * encountered. We do a CAS operation at the end of the *top* pointer once each thread has finished
 * writing to its edgeListSegment.
 * The edgeListSegment holds a pointer to a block of struct edgeList type.
 */
struct edgeList {
    common::offset_t edgeOffset;
    edgeListAndLevel* src;
    edgeList* next;

    edgeList(common::offset_t edgeOffset_, edgeListAndLevel* src_, edgeList* next_) {
        edgeOffset = edgeOffset_;
        src = src_;
        next = next_;
    }

    virtual ~edgeList() = default;
};

/**
 * 1) A pointer to this struct is held by every element of nodeEdgeListAndLevel vector.
 * Tracks level by level information of {edge offset, src node} every node is discovered from.
 *
 * 2) When writing the path back to the ValueVectors we traverse from every entry using the *next
 * pointer to go to the next level where this node was encountered and the *top pointer
 */
struct edgeListAndLevel {
    uint8_t bfsLevel;
    common::offset_t nodeOffset;
    edgeListAndLevel* next;
    edgeList* top;

    edgeListAndLevel(uint8_t bfsLevel_, common::offset_t nodeOffset_, edgeListAndLevel* next_) {
        bfsLevel = bfsLevel_;
        nodeOffset = nodeOffset_;
        next = next_;
        top = nullptr;
    }

    virtual ~edgeListAndLevel() = default;
};

/**
 * 1) This is the basic unit of memory that will be allocated when BFS extension is being done and
 * "edgeListSize" no. of neighbours are encountered.
 *
 * 2) We are guaranteed that each of the edge Offset and src Node pointer needs to be maintained
 * for a neighbour node that is encountered.
 *
 * 3) However we cannot allot the same amount of memory for edgeListAndLevel pointers. We push into
 * the vector edgeListAndLevelBlock those blocks of memory that are actually being used.
 */
struct edgeListSegment {
    edgeList* edgeListBlockPtr;
    std::vector<edgeListAndLevel*> edgeListAndLevelBlock;

    explicit edgeListSegment(uint32_t edgeListSize) {
        edgeListBlockPtr = (edgeList*)malloc(edgeListSize * sizeof(edgeList));
        edgeListAndLevelBlock = std::vector<edgeListAndLevel*>();
    }

    ~edgeListSegment() {
        free(edgeListBlockPtr);
        for (auto& edgeListAndLevel : edgeListAndLevelBlock) {
            delete edgeListAndLevel;
        }
    }
};

/// Each element of nodeIDMultiplicityToLevel vector for Variable Length BFSSharedState
struct multiplicityAndLevel {
    std::atomic<uint64_t> multiplicity;
    uint8_t bfsLevel;
    multiplicityAndLevel* next;

    multiplicityAndLevel(uint64_t multiplicity_, uint8_t bfsLevel_, multiplicityAndLevel* next_) {
        multiplicity.store(multiplicity_, std::memory_order_relaxed);
        bfsLevel = bfsLevel_;
        next = next_;
    }

    virtual ~multiplicityAndLevel() = default;
};

/**
 * A BFSSharedState is a unit of work for the nTkS scheduling policy. It is *ONLY* used for the
 * particular case of SINGLE_LABEL | NO_PATH_TRACKING. A BFSSharedState is *NOT*
 * exclusive to a thread and any thread can pick up BFS extension or Writing Path Length.
 * A shared_ptr is maintained by the BaseBFSMorsel and a morsel of work is fetched using this ptr.
 */
struct BFSSharedState {
public:
    BFSSharedState(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : mutex{std::mutex()}, ssspLocalState{EXTEND_IN_PROGRESS}, sparseFactor{1u},
          currentLevel{0u}, nextScanStartIdx{0u}, numVisitedNodes{0u},
          visitedNodes{std::vector<uint8_t>(maxNodeOffset_ + 1, NOT_VISITED)},
          pathLength{std::vector<uint8_t>(maxNodeOffset_ + 1, 0u)}, currentFrontierSize{0u},
          nextFrontierSize{0u}, denseFrontier{nullptr}, nextFrontier{nullptr}, srcOffset{0u},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_},
          numThreadsBFSRegistered{0u}, numThreadsBFSFinished{0u}, numThreadsOutputRegistered{0u},
          numThreadsOutputFinished{0u}, nextDstScanStartIdx{0u}, inputFTableTupleIdx{0u} {}

    ~BFSSharedState() {
        if (nextFrontier) {
            delete[] nextFrontier;
        }
        if (denseFrontier) {
            delete[] denseFrontier;
        }
    }

    bool registerThreadForBFS(BaseBFSState* bfsMorsel, common::QueryRelType queryRelType);

    bool registerThreadForPathOutput();

    bool deregisterThreadFromPathOutput();

    inline bool isComplete() const { return ssspLocalState == MORSEL_COMPLETE; }

    inline void freeIntermediatePathData() {
        std::unique_lock lck{mutex};
        if (!allEdgeListSegments.empty()) {
            for (auto& allEdgeListSegment : allEdgeListSegments) {
                delete allEdgeListSegment;
            }
        }
    }

    void reset(TargetDstNodes* targetDstNodes, common::QueryRelType queryRelType,
        planner::RecursiveJoinType joinType);

    SSSPLocalState getBFSMorsel(BaseBFSState* bfsMorsel, common::QueryRelType queryRelType);

    SSSPLocalState getBFSMorselAdaptive(BaseBFSState* bfsMorsel, common::QueryRelType queryRelType);

    void finishBFSMorsel(BaseBFSState* bfsMorsel, common::QueryRelType queryRelType);

    // If BFS has completed.
    bool isBFSComplete(uint64_t numDstNodesToVisit, common::QueryRelType queryRelType) const;
    // Mark src as visited.
    void markSrc(bool isSrcDestination, common::QueryRelType queryRelType);

    void moveNextLevelAsCurrentLevel();

    std::pair<uint64_t, int64_t> getDstPathLengthMorsel();

public:
    std::mutex mutex;
    SSSPLocalState ssspLocalState;
    uint64_t startTimeInMillis1;
    uint64_t startTimeInMillis2;
    uint64_t sparseFactor;
    uint8_t currentLevel;
    char padding0[64];
    std::atomic<uint64_t> nextScanStartIdx;
    char padding1[64];

    // Visited state
    std::atomic<uint64_t> numVisitedNodes;
    char padding2[64];
    std::vector<uint8_t> visitedNodes;
    std::vector<uint8_t> pathLength;

    uint64_t currentFrontierSize;
    char padding3[64];
    std::atomic<uint64_t> nextFrontierSize;
    char padding4[64];
    bool isSparseFrontier;
    // sparse frontier
    std::vector<common::offset_t> sparseFrontier;
    // dense frontier
    uint8_t* denseFrontier;
    // next frontier
    uint8_t* nextFrontier;

    // Offset of src node.
    common::offset_t srcOffset;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    uint32_t numThreadsBFSRegistered;
    uint32_t numThreadsBFSFinished;
    uint32_t numThreadsOutputRegistered;
    uint32_t numThreadsOutputFinished;
    char padding5[64];
    std::atomic<uint64_t> nextDstScanStartIdx;
    char padding6[64];
    uint64_t inputFTableTupleIdx;

    // FOR ALL_SHORTEST_PATH only
    uint8_t minDistance;
    std::vector<uint64_t> nodeIDToMultiplicity;

    // For VARIABLE_LENGTH only
    std::vector<multiplicityAndLevel*> nodeIDMultiplicityToLevel;

    // FOR RETURNING SHORTEST_PATH + TRACK_PATH ONLY
    std::vector<std::pair<uint64_t, uint64_t>> srcNodeOffsetAndEdgeOffset;
    // TEMP - to keep the edge table ID saved somewhere
    common::table_id_t edgeTableID;

    // FOR RETURNING ALL_SHORTEST_PATH / VARIABLE_LENGTH + TRACK_PATH ONLY
    std::vector<edgeListAndLevel*> nodeIDEdgeListAndLevel;
    std::vector<edgeListSegment*> allEdgeListSegments;
};

class BaseBFSState {
public:
    explicit BaseBFSState(uint8_t upperBound, uint8_t lowerBound, TargetDstNodes* targetDstNodes,
        uint64_t bfsMorselSize,
        const std::unordered_map<common::table_id_t, std::string>& tableIDToName)
        : upperBound{upperBound}, lowerBound{lowerBound}, currentLevel{0}, nextNodeIdxToExtend{0},
          targetDstNodes{targetDstNodes}, tableIDToName{tableIDToName}, bfsSharedState{nullptr},
          bfsMorselSize{bfsMorselSize} {}

    virtual ~BaseBFSState() = default;

    virtual bool getRecursiveJoinType() = 0;

    bool hasBFSSharedState() const { return bfsSharedState != nullptr; }

    // Get next node offset to extend from current level.
    common::nodeID_t getNextNodeID() {
        if (nextNodeIdxToExtend == currentFrontier->nodeIDs.size()) {
            return common::nodeID_t{common::INVALID_OFFSET, common::INVALID_TABLE_ID};
        }
        return currentFrontier->nodeIDs[nextNodeIdxToExtend++];
    }

    virtual void resetState() {
        currentLevel = 0;
        nextNodeIdxToExtend = 0;
        frontiers.clear();
        initStartFrontier();
        addNextFrontier();
    }
    virtual bool isComplete() = 0;

    virtual void markSrc(common::nodeID_t nodeID) = 0;
    virtual void markVisited(common::nodeID_t boundNodeID, common::nodeID_t nbrNodeID,
        common::relID_t relID, uint64_t multiplicity) = 0;
    inline uint64_t getMultiplicity(common::nodeID_t nodeID) const {
        return currentFrontier->getMultiplicity(nodeID);
    }

    inline void finalizeCurrentLevel() { moveNextLevelAsCurrentLevel(); }
    inline size_t getNumFrontiers() const { return frontiers.size(); }
    inline Frontier* getFrontier(common::idx_t idx) const { return frontiers[idx].get(); }

    inline SSSPLocalState getBFSMorsel(common::QueryRelType queryRelType) {
        return bfsSharedState->getBFSMorsel(this, queryRelType);
    }

    inline SSSPLocalState getBFSMorselAdaptive(common::QueryRelType queryRelType) {
        return bfsSharedState->getBFSMorselAdaptive(this, queryRelType);
    }

    inline void finishBFSMorsel(common::QueryRelType queryRelType) {
        bfsSharedState->finishBFSMorsel(this, queryRelType);
    }

    /// This is used for nTkSCAS scheduler case (no tracking of path + single label case)
    virtual void reset(uint64_t startScanIdx, uint64_t endScanIdx,
        BFSSharedState* bfsSharedState) = 0;

    virtual uint64_t getBoundNodeMultiplicity(common::offset_t nodeOffset) = 0;

    virtual void addToLocalNextBFSLevel(RecursiveJoinVectors* vectors,
        uint64_t boundNodeMultiplicity, unsigned long boundNodeOffset) = 0;

    virtual common::offset_t getNextNodeOffset() = 0;

    virtual bool hasMoreToWrite() = 0;

    virtual std::pair<uint64_t, int64_t> getPrevDistStartScanIdxAndSize() = 0;

    virtual int64_t writeToVector(
        const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
        RecursiveJoinVectors* vectors) = 0;

protected:
    inline bool isCurrentFrontierEmpty() const { return currentFrontier->nodeIDs.empty(); }
    inline bool isUpperBoundReached() const { return currentLevel == upperBound; }
    inline void initStartFrontier() {
        KU_ASSERT(frontiers.empty());
        frontiers.push_back(std::make_unique<Frontier>());
        currentFrontier = frontiers[frontiers.size() - 1].get();
    }
    inline void addNextFrontier() {
        frontiers.push_back(std::make_unique<Frontier>());
        nextFrontier = frontiers[frontiers.size() - 1].get();
    }
    void moveNextLevelAsCurrentLevel() {
        currentFrontier = nextFrontier;
        currentLevel++;
        nextNodeIdxToExtend = 0;
        if (currentLevel < upperBound) { // No need to sort if we are not extending further.
            addNextFrontier();
            std::sort(currentFrontier->nodeIDs.begin(), currentFrontier->nodeIDs.end());
        }
    }

protected:
    // Static information
    uint8_t upperBound;
    uint8_t lowerBound;
    // Level state
    uint8_t currentLevel;
    uint64_t nextNodeIdxToExtend; // next node to extend from current frontier.
    Frontier* currentFrontier;
    Frontier* nextFrontier;
    std::vector<std::unique_ptr<Frontier>> frontiers;

public:
    // Target information.
    TargetDstNodes* targetDstNodes;
    std::unordered_map<common::table_id_t, std::string> tableIDToName;
    BFSSharedState* bfsSharedState;
    uint64_t bfsMorselSize;
};

} // namespace processor
} // namespace kuzu
