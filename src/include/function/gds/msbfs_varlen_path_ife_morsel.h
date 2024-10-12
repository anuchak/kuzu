#pragma once

#include "function/gds/varlen_path_ife_morsel.h"

namespace kuzu {
namespace function {

template<typename T>
struct MSBFSVarlenPathIFEMorsel : public IFEMorsel {
public:
    MSBFSVarlenPathIFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : IFEMorsel(upperBound_, lowerBound_, maxNodeOffset_, common::INVALID_OFFSET
              /* this source offset will NOT be used */),
          isBFSActive{true}, nextDstScanStartIdx{0u} {}

    ~MSBFSVarlenPathIFEMorsel() override {
        if (!allEdgeListSegments.empty()) {
            for (auto edgeListSegment : allEdgeListSegments) {
                delete edgeListSegment;
            }
            allEdgeListSegments.clear();
        }
        if (seen) {
            delete[] seen;
            delete[] current;
            delete[] next;
        }
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
        return isBFSActive;
    }

    bool isIFEMorselCompleteNoLock() override {
        return isBFSCompleteNoLock() &&
               (nextDstScanStartIdx.load(std::memory_order_acq_rel) >= maxOffset);
    }

    void mergeResults(std::vector<edgeListSegment*>& localEdgeListSegment) {
        std::unique_lock lck{mutex};
        allEdgeListSegments.insert(allEdgeListSegments.end(), localEdgeListSegment.begin(),
            localEdgeListSegment.end());
        localEdgeListSegment.clear();
    }

    void clearAllIntermediateResults() {
        for (auto edgeListSegment : allEdgeListSegments) {
            delete edgeListSegment;
        }
        allEdgeListSegments.clear();
    }

    void initializeNextFrontierNoLock() override;

public:
    std::vector<common::offset_t> srcOffsets;
    bool isBFSActive;
    T* seen;
    T* current;
    T* next;

    // Returning Variable length + track path
    std::vector<edgeListAndLevel*> nodeIDEdgeListAndLevel;
    std::vector<edgeListSegment*> allEdgeListSegments;

    std::atomic<uint64_t> nextDstScanStartIdx;
};

} // namespace function
} // namespace kuzu
