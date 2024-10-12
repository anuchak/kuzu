#include "function/gds/msbfs_varlen_path_ife_morsel.h"

namespace kuzu {
namespace function {

template<>
void MSBFSVarlenPathIFEMorsel<uint8_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    isBFSActive = true;
    isSparseFrontier = false;
    clearAllIntermediateResults();
    if (!seen) {
        seen = new uint8_t[maxOffset + 1]{0};
        current = new uint8_t[maxOffset + 1]{0};
        next = new uint8_t[maxOffset + 1]{0};
        // lane width 8, 8 bytes for each pointer
        nodeIDEdgeListAndLevel = std::vector<edgeListAndLevel*>(8 * (maxOffset + 1), nullptr);
    } else {
        memset(seen, 0u, maxOffset + 1);
        memset(current, 0u, maxOffset + 1);
        memset(next, 0u, maxOffset + 1);
        std::fill(nodeIDEdgeListAndLevel.begin(), nodeIDEdgeListAndLevel.end(), nullptr);
    }
    auto seenVal = 0x1;
    auto srcEdgeListSegment = new edgeListSegment(0u);
    for (auto idx = 0u; idx < srcOffsets.size(); idx++) {
        auto exactPos = 8 * srcOffsets[idx] + (7 - idx);
        srcEdgeListSegment->edgeListAndLevelBlock.push_back(
            new edgeListAndLevel(0u, srcOffsets[idx], nullptr));
        nodeIDEdgeListAndLevel[exactPos] = srcEdgeListSegment->edgeListAndLevelBlock[idx];
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    initializedIFEMorsel = true;
}

template<>
void MSBFSVarlenPathIFEMorsel<uint16_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    isBFSActive = true;
    isSparseFrontier = false;
    clearAllIntermediateResults();
    if (!seen) {
        seen = new uint16_t[maxOffset + 1]{0};
        current = new uint16_t[maxOffset + 1]{0};
        next = new uint16_t[maxOffset + 1]{0};
        // lane width 16, 8 bytes for each pointer
        nodeIDEdgeListAndLevel = std::vector<edgeListAndLevel*>(16 * (maxOffset + 1), nullptr);
    } else {
        memset(seen, 0u, 2 * (maxOffset + 1));
        memset(current, 0u, 2 * (maxOffset + 1));
        memset(next, 0u, 2 * (maxOffset + 1));
        std::fill(nodeIDEdgeListAndLevel.begin(), nodeIDEdgeListAndLevel.end(), nullptr);
    }
    auto seenVal = 0x1;
    auto srcEdgeListSegment = new edgeListSegment(0u);
    for (auto idx = 0u; idx < srcOffsets.size(); idx++) {
        auto exactPos = 16 * srcOffsets[idx] + (15 - idx);
        srcEdgeListSegment->edgeListAndLevelBlock.push_back(
            new edgeListAndLevel(0u, srcOffsets[idx], nullptr));
        nodeIDEdgeListAndLevel[exactPos] = srcEdgeListSegment->edgeListAndLevelBlock[idx];
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
}

template<>
void MSBFSVarlenPathIFEMorsel<uint32_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    isBFSActive = true;
    isSparseFrontier = false;
    clearAllIntermediateResults();
    if (!seen) {
        seen = new uint32_t[maxOffset + 1]{0};
        current = new uint32_t[maxOffset + 1]{0};
        next = new uint32_t[maxOffset + 1]{0};
        // lane width 32, 8 bytes for each pointer
        nodeIDEdgeListAndLevel = std::vector<edgeListAndLevel*>(32 * (maxOffset + 1), nullptr);
    } else {
        memset(seen, 0u, 4 * (maxOffset + 1));
        memset(current, 0u, 4 * (maxOffset + 1));
        memset(next, 0u, 4 * (maxOffset + 1));
        std::fill(nodeIDEdgeListAndLevel.begin(), nodeIDEdgeListAndLevel.end(), nullptr);
    }
    auto seenVal = 0x1;
    auto srcEdgeListSegment = new edgeListSegment(0u);
    for (auto idx = 0u; idx < srcOffsets.size(); idx++) {
        auto exactPos = 32 * srcOffsets[idx] + (31 - idx);
        srcEdgeListSegment->edgeListAndLevelBlock.push_back(
            new edgeListAndLevel(0u, srcOffsets[idx], nullptr));
        nodeIDEdgeListAndLevel[exactPos] = srcEdgeListSegment->edgeListAndLevelBlock[idx];
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
}

template<>
void MSBFSVarlenPathIFEMorsel<uint64_t>::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    isBFSActive = true;
    isSparseFrontier = false;
    clearAllIntermediateResults();
    if (!seen) {
        seen = new uint64_t[maxOffset + 1]{0};
        current = new uint64_t[maxOffset + 1]{0};
        next = new uint64_t[maxOffset + 1]{0};
        // lane width 64, 8 bytes for each pointer
        nodeIDEdgeListAndLevel = std::vector<edgeListAndLevel*>(64 * (maxOffset + 1), nullptr);
    } else {
        memset(seen, 0u, 8 * (maxOffset + 1));
        memset(current, 0u, 8 * (maxOffset + 1));
        memset(next, 0u, 8 * (maxOffset + 1));
        std::fill(nodeIDEdgeListAndLevel.begin(), nodeIDEdgeListAndLevel.end(), nullptr);
    }
    auto seenVal = 1llu;
    auto srcEdgeListSegment = new edgeListSegment(0u);
    for (auto idx = 0u; idx < srcOffsets.size(); idx++) {
        auto exactPos = 64 * srcOffsets[idx] + (63 - idx);
        srcEdgeListSegment->edgeListAndLevelBlock.push_back(
            new edgeListAndLevel(0u, srcOffsets[idx], nullptr));
        nodeIDEdgeListAndLevel[exactPos] = srcEdgeListSegment->edgeListAndLevelBlock[idx];
        seen[srcOffset] = seenVal;
        current[srcOffset] = seenVal;
        seenVal = seenVal << 1;
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
}

template<>
void MSBFSVarlenPathIFEMorsel<uint8_t>::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (!isBFSActive && next[offset]) {
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

template<>
void MSBFSVarlenPathIFEMorsel<uint16_t>::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (!isBFSActive && next[offset]) {
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

template<>
void MSBFSVarlenPathIFEMorsel<uint32_t>::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (!isBFSActive && next[offset]) {
                isBFSActive = true;
            }
            seen[offset] |= next[offset];
        }
        auto temp = current;
        current = next;
        next = temp;
        memset(next, 0u, 4 * (maxOffset + 1));
    } else {
        isBFSActive = false;
    }
}

template<>
void MSBFSVarlenPathIFEMorsel<uint64_t>::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (!isBFSActive && next[offset]) {
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

} // namespace function
} // namespace kuzu
