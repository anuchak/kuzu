#include "function/gds/ife_morsel.h"
#include <immintrin.h>

#include <cmath>

#include "function/table/call_functions.h"

namespace kuzu {
namespace function {

IFEMorsel::~IFEMorsel() {
    if (currentFrontier) {
        delete[] currentFrontier;
        delete[] nextFrontier;
    }
}

void IFEMorsel::init() {
    if (!currentFrontier) {
        currentFrontier = new uint8_t[maxOffset + 1]{0u};
        nextFrontier = new uint8_t[maxOffset + 1]{0u};
        bfsFrontier.reserve(std::ceil(maxOffset / 8));
    } else {
        std::fill(currentFrontier, currentFrontier + maxOffset + 1, 0u);
        std::fill(nextFrontier, nextFrontier + maxOffset + 1, 0u);
        bfsFrontier.clear();
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    isSparseFrontier = true;
    bfsFrontier.push_back(srcOffset);
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    currentFrontierSize = 1u;
}

void IFEMorsel::resetNoLock(common::offset_t srcOffset_) {
    initializedIFEMorsel = false;
    srcOffset = srcOffset_;
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    startTime = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

function::CallFuncMorsel IFEMorsel::getMorsel(uint64_t morselSize) {
    auto morselStartIdx = nextScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
    if (isSparseFrontier) {
        if (morselStartIdx >= currentFrontierSize) {
            return {common::INVALID_OFFSET, common::INVALID_OFFSET};
        }
        auto morselEndIdx = std::min(morselStartIdx + morselSize, currentFrontierSize);
        return {morselStartIdx, morselEndIdx};
    } else {
        if (morselStartIdx > maxOffset) {
            return {common::INVALID_OFFSET, common::INVALID_OFFSET};
        }
        auto morselEndIdx = std::min(morselStartIdx + morselSize, maxOffset + 1);
        return {morselStartIdx, morselEndIdx};
    }
}

void IFEMorsel::initializeNextFrontierNoLock() {
    currentLevel++;
    nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
    currentFrontierSize = nextFrontierSize.load(std::memory_order_acq_rel);
    nextFrontierSize.store(0u, std::memory_order_acq_rel);
    // if true, next frontier is sparse, push elements in bfsFrontier vector
    // else, next frontier is dense, switch pointers with current frontier array & next frontier
    if (currentFrontierSize < (uint64_t) std::ceil((maxOffset / 8))) {
        isSparseFrontier = true;
        bfsFrontier.clear();
        auto simdWidth = 16u, i = 0u, pos = 0u;
        // SSE2 vector with all elements set to 1
        __m128i ones = _mm_set1_epi8(1);
        for (; i + simdWidth < maxOffset + 1; i += simdWidth) {
            __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&nextFrontier[i]));
            __m128i cmp = _mm_cmpeq_epi8(vec, ones);
            int mask = _mm_movemask_epi8(cmp);
            while (mask != 0) {
                int index = __builtin_ctz(mask);
                bfsFrontier[pos++] = (i + index);
                mask &= ~(1 << index);
            }
        }

        // Process any remaining elements
        for (; i < maxOffset + 1; ++i) {
            if (nextFrontier[i]) {
                bfsFrontier[pos++] = i;
            }
        }
    } else {
        isSparseFrontier = false;
        auto temp = currentFrontier;
        currentFrontier = nextFrontier;
        nextFrontier = temp;
    }
    std::fill(nextFrontier, nextFrontier + maxOffset + 1, 0u);
}

} // namespace function
} // namespace kuzu
