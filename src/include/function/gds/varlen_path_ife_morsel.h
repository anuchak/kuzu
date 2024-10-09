#pragma once

#include "ife_morsel.h"

namespace kuzu {
namespace function {

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

struct VarlenPathIFEMorsel : public IFEMorsel {
public:
    VarlenPathIFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_,
        common::offset_t srcOffset)
        : IFEMorsel(upperBound_, lowerBound_, maxNodeOffset_, srcOffset), nextDstScanStartIdx{0u} {}

    void init() override;

    uint64_t getWork() override;

    function::CallFuncMorsel getDstWriteMorsel(uint64_t morselSize) override;

    bool isBFSCompleteNoLock() override;

    bool isIFEMorselCompleteNoLock() override;

    void mergeResults(std::vector<edgeListSegment*>& localEdgeListSegment) {
        std::unique_lock lck{mutex};
        allEdgeListSegments.insert(allEdgeListSegments.end(), localEdgeListSegment.begin(),
            localEdgeListSegment.end());
        localEdgeListSegment.clear();
    }

    void clearAllIntermediateResults() {
        for (auto edgeListSegment : allEdgeListSegments) {
            free(edgeListSegment);
        }
        allEdgeListSegments.clear();
    }

    void initializeNextFrontierNoLock() override;

public:
    // Track visited / not visited
    std::vector<uint8_t> visitedNodes;

    // Returning Variable length + track path
    std::vector<edgeListAndLevel*> nodeIDEdgeListAndLevel;
    std::vector<edgeListSegment*> allEdgeListSegments;

    std::atomic<uint64_t> nextDstScanStartIdx;
};

} // namespace function
} // namespace kuzu
