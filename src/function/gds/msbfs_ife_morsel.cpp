#include <cmath>
#include "function/gds/msbfs_ife_morsel.h"

namespace kuzu {
namespace function {

template<> void MSBFSIFEMorsel<uint8_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    currentDstLane = 0;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    isBFSActive = true;
    isSparseFrontier = false;
    if (!seen) {
        seen = new uint8_t [maxOffset + 1]{0};
        current = new uint8_t [maxOffset + 1]{0};
        next = new uint8_t [maxOffset + 1]{0};
        // lane width 8, 1 byte path length value
        pathLength = new uint8_t [8 * (maxOffset + 1)]{0};
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
    initializedIFEMorsel = true;
}

template<> void MSBFSIFEMorsel<uint16_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentDstLane = 0;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    isBFSActive = true;
    isSparseFrontier = false;
    if (!seen) {
        seen = new uint16_t [maxOffset + 1]{0};
        current = new uint16_t [maxOffset + 1]{0};
        next = new uint16_t [maxOffset + 1]{0};
        // lane width 16, 1 byte path length value
        pathLength = new uint8_t [16 * (maxOffset + 1)]{0};
    } else {
        memset(seen, 0u, 2 * (maxOffset + 1));
        memset(current, 0u, 2 * (maxOffset + 1));
        memset(next, 0u, 2 * (maxOffset + 1));
        memset(pathLength, 0u, 16 * (maxOffset + 1));
    }
    auto seenVal = 0x1;
    for (auto srcOffset : srcOffsets) {
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
}

template<> void MSBFSIFEMorsel<uint32_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentDstLane = 0;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    isBFSActive = true;
    isSparseFrontier = false;
    if (!seen) {
        seen = new uint32_t [maxOffset + 1]{0};
        current = new uint32_t [maxOffset + 1]{0};
        next = new uint32_t [maxOffset + 1]{0};
        // lane width 32, 1 byte path length value
        pathLength = new uint8_t [32 * (maxOffset + 1)]{0};
    } else {
        memset(seen, 0u, 4 * (maxOffset + 1));
        memset(current, 0u, 4 * (maxOffset + 1));
        memset(next, 0u, 4 * (maxOffset + 1));
        memset(pathLength, 0u, 32 * (maxOffset + 1));
    }
    auto seenVal = 0x1;
    for (auto srcOffset : srcOffsets) {
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
}

template<> void MSBFSIFEMorsel<uint64_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentDstLane = 0;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    isBFSActive = true;
    isSparseFrontier = false;
    if (!seen) {
        seen = new uint64_t [maxOffset + 1]{0};
        current = new uint64_t [maxOffset + 1]{0};
        next = new uint64_t [maxOffset + 1]{0};
        // lane width 64, 1 byte path length value
        pathLength = new uint8_t [64 * (maxOffset + 1)]{0};
    } else {
        memset(seen, 0u, 8 * (maxOffset + 1));
        memset(current, 0u, 8 * (maxOffset + 1));
        memset(next, 0u, 8 * (maxOffset + 1));
        memset(pathLength, 0u, 64 * (maxOffset + 1));
    }
    auto seenVal = 0x1;
    for (auto srcOffset : srcOffsets) {
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
}

template<> void MSBFSIFEMorsel<uint8_t>::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (next[offset]) {
                isBFSActive = true;
            }
            seen[offset] |= next[offset];
        }
        auto temp = current;
        current = next;
        next = temp;
        memset(next, 0u, maxOffset + 1);
    } else {
        isBFSActive = false;
    }
}

template<> void MSBFSIFEMorsel<uint16_t>::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (next[offset]) {
                isBFSActive = true;
            }
            seen[offset] |= next[offset];
        }
        auto temp = current;
        current = next;
        next = temp;
        memset(next, 0u, 2 * (maxOffset + 1));
    } else {
        isBFSActive = false;
    }
}

template<> void MSBFSIFEMorsel<uint32_t>::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (next[offset]) {
                isBFSActive = true;
            }
            seen[offset] |= next[offset];
        }
        auto temp = current;
        current = next;
        next = temp;
        memset(next, 0u, 4 * (maxOffset + 1));
    }  else {
        isBFSActive = false;
    }
}

template<> void MSBFSIFEMorsel<uint64_t>::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (next[offset]) {
                isBFSActive = true;
            }
            seen[offset] |= next[offset];
        }
        auto temp = current;
        current = next;
        next = temp;
        memset(next, 0u, 8 * (maxOffset + 1));
    } else {
        isBFSActive = false;
    }
}

}
}
