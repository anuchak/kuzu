#include "function/gds/sp_ife_mega_morsel.h"

#include <cmath>

namespace kuzu {
namespace function {

void SPIFEMegaMorsel::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    for (auto lane = 0; lane < totalActiveLanes; lane++) {
        auto &spIFEMorsel = spIFEMorsels[lane];
        if (!spIFEMorsel->currentFrontier) {
            spIFEMorsel->currentFrontier = new uint8_t[maxOffset + 1]{0u};
            spIFEMorsel->nextFrontier = new uint8_t[maxOffset + 1]{0u};
            spIFEMorsel->visitedNodes = std::vector<uint8_t>(maxOffset + 1, NOT_VISITED_DST);
            spIFEMorsel->pathLength = std::vector<uint8_t>(maxOffset + 1, 0u);
        } else {
            std::fill(spIFEMorsel->currentFrontier, spIFEMorsel->currentFrontier + maxOffset + 1,
                0u);
            std::fill(spIFEMorsel->nextFrontier, spIFEMorsel->nextFrontier + maxOffset + 1, 0u);
            std::fill(spIFEMorsel->visitedNodes.begin(), spIFEMorsel->visitedNodes.end(),
                NOT_VISITED_DST);
            std::fill(spIFEMorsel->pathLength.begin(), spIFEMorsel->pathLength.end(), 0u);
        }
        spIFEMorsel->visitedNodes[spIFEMorsel->srcOffset] = VISITED_DST;
        spIFEMorsel->currentFrontier[spIFEMorsel->srcOffset] = 1u;
        spIFEMorsel->currentLevel = 0u;
    }
    initializedIFEMorsel = true;
    isBFSActive = true;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
}

uint64_t SPIFEMegaMorsel::getWork() {
    if (isBFSCompleteNoLock()) {
        return maxOffset - nextDstScanStartIdx.load(std::memory_order_acquire);
    }
    return maxOffset - nextScanStartIdx.load(std::memory_order_acquire);
}

function::CallFuncMorsel SPIFEMegaMorsel::getDstWriteMorsel(uint64_t morselSize) {
    auto morselStartIdx = nextDstScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
    if (morselStartIdx >= maxOffset) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto morselEndIdx = std::min(morselStartIdx + morselSize, maxOffset + 1);
    return {morselStartIdx, morselEndIdx};
}

bool SPIFEMegaMorsel::isBFSCompleteNoLock() {
    if (currentLevel == upperBound) {
        return true;
    }
    if (!isBFSActive) {
        return true;
    }
    return false;
}

bool SPIFEMegaMorsel::isIFEMorselCompleteNoLock() {
    return isBFSCompleteNoLock() &&
           (nextDstScanStartIdx.load(std::memory_order_acq_rel) >= maxOffset);
}

void SPIFEMegaMorsel::initializeNextFrontierNoLock() {
    currentLevel++;
    if (currentLevel < upperBound) {
        nextScanStartIdx.store(0u, std::memory_order_acq_rel);
        isBFSActive = false;
        for (auto lane = 0; lane < totalActiveLanes; lane++) {
            auto &spIFEMorsel = spIFEMorsels[lane];
            auto temp = spIFEMorsel->currentFrontier;
            spIFEMorsel->currentFrontier = spIFEMorsel->nextFrontier;
            spIFEMorsel->nextFrontier = temp;
            std::fill(spIFEMorsel->nextFrontier, spIFEMorsel->nextFrontier + maxOffset + 1, 0u);
            if (!isBFSActive)
                for (auto offset = 0u; offset <= maxOffset; offset++)
                    if (spIFEMorsel->currentFrontier[offset]) {
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
