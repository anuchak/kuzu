#pragma once

#include "ife_morsel.h"

namespace kuzu {
namespace function {

struct SPIFEMorsel : public IFEMorsel {
public:
    SPIFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_,
        common::offset_t srcOffset)
    : IFEMorsel(upperBound_, lowerBound_, maxNodeOffset_, srcOffset), numVisitedDstNodes{0u},
          numDstNodesToVisit{maxNodeOffset_ + 1}, nextDstScanStartIdx{0u} {}

    void init() override;

    uint64_t getWork() override;

    function::CallFuncMorsel getDstWriteMorsel(uint64_t morselSize) override;

    bool isBFSCompleteNoLock() override;

    bool isIFEMorselCompleteNoLock() override;

    void mergeResults(uint64_t numDstVisitedLocal, uint64_t numNonDstVisitedLocal);

public:

    // Visited state
    std::atomic<uint64_t> numVisitedDstNodes;
    uint64_t numDstNodesToVisit;
    std::vector<uint8_t> visitedNodes;

    std::vector<uint8_t> pathLength;
    std::atomic<uint64_t> nextDstScanStartIdx;
};

}
}
