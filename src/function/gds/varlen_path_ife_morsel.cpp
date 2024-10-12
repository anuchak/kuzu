#include "function/gds/varlen_path_ife_morsel.h"

namespace kuzu {
namespace function {

void VarlenPathIFEMorsel::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    IFEMorsel::init();
    clearAllIntermediateResults();
    if (visitedNodes.empty()) {
        visitedNodes = std::vector<uint8_t>(maxOffset + 1, NOT_VISITED_DST);
        nodeIDEdgeListAndLevel = std::vector<edgeListAndLevel*>(maxOffset + 1, nullptr);
    } else {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
        std::fill(nodeIDEdgeListAndLevel.begin(), nodeIDEdgeListAndLevel.end(), nullptr);
    }
    isSparseFrontier = false;
    currentFrontier[srcOffset] = 1u;
    bfsFrontier.clear();
    auto srcEdgeListSegment = new edgeListSegment(0u);
    srcEdgeListSegment->edgeListAndLevelBlock.push_back(
        new edgeListAndLevel(0u, srcOffset, nullptr));
    nodeIDEdgeListAndLevel[srcOffset] = srcEdgeListSegment->edgeListAndLevelBlock[0];
    allEdgeListSegments.push_back(srcEdgeListSegment);
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    visitedNodes[srcOffset] = VISITED_DST;
}

uint64_t VarlenPathIFEMorsel::getWork() {
    if (isBFSCompleteNoLock()) {
        return maxOffset - nextDstScanStartIdx.load(std::memory_order_acquire);
    }
    return maxOffset - nextScanStartIdx.load(std::memory_order_acquire);
}

function::CallFuncMorsel VarlenPathIFEMorsel::getDstWriteMorsel(uint64_t morselSize) {
    auto morselStartIdx = nextDstScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
    if (morselStartIdx >= maxOffset) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto morselEndIdx = std::min(morselStartIdx + morselSize, maxOffset + 1);
    return {morselStartIdx, morselEndIdx};
}

bool VarlenPathIFEMorsel::isBFSCompleteNoLock() {
    if (currentLevel == upperBound) {
        return true;
    }
    return isBFSActive;
}

bool VarlenPathIFEMorsel::isIFEMorselCompleteNoLock() {
    return isBFSCompleteNoLock() &&
           (nextDstScanStartIdx.load(std::memory_order_acq_rel) >= maxOffset);
}

void VarlenPathIFEMorsel::initializeNextFrontierNoLock() {
    currentLevel++;
    nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
    if (currentLevel < upperBound) {
        auto temp = currentFrontier;
        currentFrontier = nextFrontier;
        nextFrontier = temp;
        std::fill(nextFrontier, nextFrontier + maxOffset + 1, 0u);
        isBFSActive = false;
        for (auto offset = 0u; offset <= maxOffset; offset++) {
            if (currentFrontier[offset]) {
                isBFSActive = true;
                break;
            }
        }
    } else {
        isBFSActive = false;
    }
}

} // namespace function
} // namespace kuzu
