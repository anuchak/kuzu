#include <cmath>
#include "function/gds/sp_ife_morsel.h"

namespace kuzu {
namespace function {

void SPIFEMorsel::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    IFEMorsel::init();
    if (visitedNodes.empty()) {
        visitedNodes = std::vector<uint8_t>(maxOffset + 1, NOT_VISITED_DST);
        pathLength = std::vector<uint8_t>(maxOffset + 1, 0u);
    } else {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
        std::fill(pathLength.begin(), pathLength.end(), 0u);
    }
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    visitedNodes[srcOffset] = VISITED_DST;
    currentFrontierSize = 1u;
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
}

uint64_t SPIFEMorsel::getWork() {
    if (isBFSCompleteNoLock()) {
        return maxOffset -
               nextDstScanStartIdx.load(std::memory_order_acquire);
    }
    /*
     * This is an approximation of the remaining frontier, it can be either:
     * (1) if frontier is sparse, we subtract the next scan idx from current frontier size
     *     and return the value
     * (2) if frontier is dense, subtract next scan idx from maxOffset (since in this case the
     *     frontier is technically the whole currFrontier array)
     */
    if (isSparseFrontier) {
        return currentFrontierSize -
               nextScanStartIdx.load(std::memory_order_acquire);
    }
    return maxOffset - nextScanStartIdx.load(std::memory_order_acquire);
}

function::CallFuncMorsel SPIFEMorsel::getDstWriteMorsel(uint64_t morselSize) {
    auto morselStartIdx = nextDstScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
    if (morselStartIdx >= maxOffset) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto morselEndIdx = std::min(morselStartIdx + morselSize, maxOffset + 1);
    return {morselStartIdx, morselEndIdx};
}

bool SPIFEMorsel::isBFSCompleteNoLock() {
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

bool SPIFEMorsel::isIFEMorselCompleteNoLock() {
    return isBFSCompleteNoLock() &&
           (nextDstScanStartIdx.load(std::memory_order_acq_rel) >= maxOffset);
}

void SPIFEMorsel::mergeResults(uint64_t numDstVisitedLocal, uint64_t numNonDstVisitedLocal) {
    numVisitedDstNodes.fetch_add(numDstVisitedLocal);
    nextFrontierSize.fetch_add(numDstVisitedLocal + numNonDstVisitedLocal);
}

}
}
