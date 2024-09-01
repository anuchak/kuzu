#pragma once

#include "ife_morsel.h"

namespace kuzu {
namespace function {

struct SPIFEMorsel : public IFEMorsel {
public:
    SPIFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_,
        common::offset_t srcOffset)
    : IFEMorsel(upperBound_, lowerBound_, maxNodeOffset_, srcOffset) {}



public:
    std::mutex mutex;
    bool initializedIFEMorsel;
    uint64_t startTime;
    bool isSparseFrontier;
    uint8_t currentLevel;
    std::atomic<uint64_t> nextScanStartIdx;
    uint64_t currentFrontierSize;
    std::atomic<uint64_t> nextFrontierSize;
    common::offset_t srcOffset;

    // Visited state
    std::atomic<uint64_t> numVisitedDstNodes;
    uint64_t numDstNodesToVisit;
    std::vector<uint8_t> visitedNodes;
    // If the frontier is dense, then we use these 2 arrays as frontier and next frontier
    // Based on if the frontier size > (total nodes / 8)
    uint8_t* currentFrontier;
    uint8_t* nextFrontier;
    // If the frontier is sparse, then use this vector as frontier to extend
    // Based on if frontier size < (total nodes / 8)
    std::vector<common::offset_t> bfsFrontier;

    std::vector<uint8_t> pathLength;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    std::atomic<uint64_t> nextDstScanStartIdx;
};

}
}
