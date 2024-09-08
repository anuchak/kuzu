#include <cmath>
#include "function/gds/msbfs_ife_morsel.h"

namespace kuzu {
namespace function {

template<> void MSBFSIFEMorsel<uint8_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    if (!seen) {
        seen = new uint8_t [maxOffset + 1];
        current = new uint8_t [maxOffset + 1];
        next = new uint8_t [maxOffset + 1];
        // lane width 8, 1 byte path length value
        pathLength = new uint8_t [8 * (maxOffset + 1)];
    } else {
        memset(seen, 0u, maxOffset + 1);
        memset(current, 0u, maxOffset + 1);
        memset(next, 0u, maxOffset + 1);
        memset(pathLength, 0u, 8 * (maxOffset + 1));
    }
    auto seenVal = 0x1;
    for (auto srcOffset : srcOffsets) {
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
}

template<> void MSBFSIFEMorsel<uint16_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    if (!seen) {
        seen = new uint16_t [maxOffset + 1];
        current = new uint16_t [maxOffset + 1];
        next = new uint16_t [maxOffset + 1];
        // lane width 16, 1 byte path length value
        pathLength = new uint8_t [16 * (maxOffset + 1)];
    } else {
        memset(seen, 0u, maxOffset + 1);
        memset(current, 0u, maxOffset + 1);
        memset(next, 0u, maxOffset + 1);
        memset(pathLength, 0u, 16 * (maxOffset + 1));
    }
    auto seenVal = 0x1;
    for (auto srcOffset : srcOffsets) {
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
}

template<> void MSBFSIFEMorsel<uint32_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    if (!seen) {
        seen = new uint32_t [maxOffset + 1];
        current = new uint32_t [maxOffset + 1];
        next = new uint32_t [maxOffset + 1];
        // lane width 32, 1 byte path length value
        pathLength = new uint8_t [32 * (maxOffset + 1)];
    } else {
        memset(seen, 0u, maxOffset + 1);
        memset(current, 0u, maxOffset + 1);
        memset(next, 0u, maxOffset + 1);
        memset(pathLength, 0u, 32 * (maxOffset + 1));
    }
    auto seenVal = 0x1;
    for (auto srcOffset : srcOffsets) {
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
}

template<> void MSBFSIFEMorsel<uint64_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    if (!seen) {
        seen = new uint64_t [maxOffset + 1];
        current = new uint64_t [maxOffset + 1];
        next = new uint64_t [maxOffset + 1];
        // lane width 32, 1 byte path length value
        pathLength = new uint8_t [32 * (maxOffset + 1)];
    } else {
        memset(seen, 0u, maxOffset + 1);
        memset(current, 0u, maxOffset + 1);
        memset(next, 0u, maxOffset + 1);
        memset(pathLength, 0u, 32 * (maxOffset + 1));
    }
    auto seenVal = 0x1;
    for (auto srcOffset : srcOffsets) {
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
}

bool MSBFSIFEMorsel::isBFSCompleteNoLock() {
    if (currentLevel == upperBound) {
        return true;
    }
    if (currentFrontierSize == 0u) {
        return true;
    }
    if (numVisitedDstNodes.load(std::memory_order_acq_rel) == numDstNodesToVisit) {
        return true;
    }
    return false;
}

void MSBFSIFEMorsel::mergeResults(uint64_t numDstVisitedLocal, uint64_t numNonDstVisitedLocal) {
    numVisitedDstNodes.fetch_add(numDstVisitedLocal);
    nextFrontierSize.fetch_add(numDstVisitedLocal + numNonDstVisitedLocal);
}

}
}
