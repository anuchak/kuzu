#include "function/gds/sp_path_ife_morsel.h"

namespace kuzu {
namespace function {

void SPPathIFEMorsel::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    IFEMorsel::init();
    if (visitedNodes.empty()) {
        visitedNodes = std::vector<uint8_t>(maxOffset + 1, NOT_VISITED_DST);
        pathLength = std::vector<uint8_t>(maxOffset + 1, 0u);
        parentOffset = std::vector<common::offset_t>(maxOffset + 1, common::INVALID_OFFSET);
        edgeOffset = std::vector<common::offset_t>(maxOffset + 1, common::INVALID_OFFSET);
    } else {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
        std::fill(pathLength.begin(), pathLength.end(), 0u);
        std::fill(parentOffset.begin(), parentOffset.end(), common::INVALID_OFFSET);
        std::fill(edgeOffset.begin(), edgeOffset.end(), common::INVALID_OFFSET);
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    visitedNodes[srcOffset] = VISITED_DST;
    currentFrontierSize = 1u;
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
}

uint64_t SPPathIFEMorsel::getWork() {
    if (isBFSCompleteNoLock()) {
        return maxOffset - nextDstScanStartIdx.load(std::memory_order_acquire);
    }
    /*
     * This is an approximation of the remaining frontier, it can be either:
     * (1) if frontier is sparse, we subtract the next scan idx from current frontier size
     *     and return the value
     * (2) if frontier is dense, subtract next scan idx from maxOffset (since in this case the
     *     frontier is technically the whole currFrontier array)
     */
    if (isSparseFrontier) {
        return currentFrontierSize - nextScanStartIdx.load(std::memory_order_acquire);
    }
    return maxOffset - nextScanStartIdx.load(std::memory_order_acquire);
}

function::CallFuncMorsel SPPathIFEMorsel::getDstWriteMorsel(uint64_t morselSize) {
    auto morselStartIdx = nextDstScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
    if (morselStartIdx >= maxOffset) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto morselEndIdx = std::min(morselStartIdx + morselSize, maxOffset + 1);
    return {morselStartIdx, morselEndIdx};
}

bool SPPathIFEMorsel::isBFSCompleteNoLock() {
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

bool SPPathIFEMorsel::isIFEMorselCompleteNoLock() {
    return isBFSCompleteNoLock() &&
           (nextDstScanStartIdx.load(std::memory_order_acq_rel) >= maxOffset);
}

void SPPathIFEMorsel::mergeResults(uint64_t numDstVisitedLocal, uint64_t numNonDstVisitedLocal) {
    numVisitedDstNodes.fetch_add(numDstVisitedLocal);
    nextFrontierSize.fetch_add(numDstVisitedLocal + numNonDstVisitedLocal);
}

} // namespace function
} // namespace kuzu
