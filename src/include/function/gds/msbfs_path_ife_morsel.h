#pragma once

#include "ife_morsel.h"

namespace kuzu {
namespace function {

template<typename T>
struct MSBFSPathIFEMorsel : public IFEMorsel {
public:
    MSBFSPathIFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : IFEMorsel(upperBound_, lowerBound_, maxNodeOffset_, common::INVALID_OFFSET
              /* passing this as a placeholder, no use of the srcOffset variable in IFEMorsel */),
          srcOffsets{std::vector<common::offset_t>()}, isBFSActive{true}, seen{nullptr},
          current{nullptr}, next{nullptr}, pathLength{nullptr}, nextDstScanStartIdx{0u} {}

    ~MSBFSPathIFEMorsel() override {
        if (seen) {
            delete[] seen;
            delete[] current;
            delete[] next;
            delete[] pathLength;
        }
    }

    void resetNoLock(common::offset_t srcOffset_) override {
        IFEMorsel::resetNoLock(srcOffset_);
        isBFSActive = true;
    }

    void init() override;

    uint64_t getWork() override {
        if (isBFSCompleteNoLock()) {
            return maxOffset - nextDstScanStartIdx.load(std::memory_order_acquire);
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

    bool isBFSCompleteNoLock() override {
        if (currentLevel == upperBound) {
            return true;
        }
        if (!isBFSActive) {
            return true;
        }
        return false;
    }

    bool isIFEMorselCompleteNoLock() override {
        return isBFSCompleteNoLock() &&
               (nextDstScanStartIdx.load(std::memory_order_acq_rel) >= maxOffset);
    }

    void mergeResults(uint64_t /*numDstVisitedLocal*/, uint64_t /*numNonDstVisitedLocal*/) {}

    void initializeNextFrontierNoLock() override;

public:
    std::vector<common::offset_t> srcOffsets;
    // Visited state
    bool isBFSActive;
    T* seen;

    T* current;
    T* next;
    uint8_t* pathLength;
    uint64_t *parentOffset;
    uint64_t *edgeOffset;

    std::atomic<uint64_t> nextDstScanStartIdx;
};

} // namespace function
} // namespace kuzu
