#pragma once

#include "ife_morsel.h"

namespace kuzu {
namespace function {

template<typename T>
struct MSBFSIFEMorsel : public IFEMorsel {
public:
    MSBFSIFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_,
        std::vector<common::offset_t> srcOffsets)
        : IFEMorsel(upperBound_, lowerBound_, maxNodeOffset_, srcOffsets[0]),
          numVisitedDstNodes{0u}, numDstNodesToVisit{maxNodeOffset_ + 1},
          nextDstScanStartIdx{0u}, srcOffsets{std::move(srcOffsets)} {}

    void init() override;

    uint64_t getWork() override {
        if (isBFSCompleteNoLock()) {
            return maxOffset -
                   nextDstScanStartIdx.load(std::memory_order_acquire);
        }
        return maxOffset - nextScanStartIdx.load(std::memory_order_acquire);
    }

    function::CallFuncMorsel getDstWriteMorsel(uint64_t morselSize) override {
        auto morselStartIdx = nextDstScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
        if (morselStartIdx >= maxOffset) {
            return function::CallFuncMorsel::createInvalidMorsel();
        }
        auto morselEndIdx = std::min(morselStartIdx + morselSize, maxOffset + 1);
        return {morselStartIdx, morselEndIdx};
    }

    bool isBFSCompleteNoLock() override;

    bool isIFEMorselCompleteNoLock() override {
        return isBFSCompleteNoLock() &&
               (nextDstScanStartIdx.load(std::memory_order_acq_rel) >= maxOffset);
    }

    void mergeResults(uint64_t numDstVisitedLocal, uint64_t numNonDstVisitedLocal);

public:
    std::vector<common::offset_t> srcOffsets;
    // Visited state
    std::atomic<uint64_t> numVisitedDstNodes;
    uint64_t numDstNodesToVisit;
    T *seen;

    T *current;
    T *next;
    uint8_t *pathLength;
    std::atomic<uint64_t> nextDstScanStartIdx;
};

}
}
