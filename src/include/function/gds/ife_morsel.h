#pragma once

#include <atomic>
#include <mutex>
#include <vector>

#include "common/types/internal_id_t.h"
#include "function/table/call_functions.h"

namespace kuzu {
namespace function {

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
};

struct IFEMorsel {
public:
    IFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_,
        common::offset_t srcOffset)
        : mutex{std::mutex()}, initializedIFEMorsel{false}, currentLevel{0u}, nextScanStartIdx{0u},
          currentFrontierSize{0u}, srcOffset{srcOffset}, currentFrontier{nullptr},
          nextFrontier{nullptr}, bfsFrontier{std::vector<common::offset_t>()},
          maxOffset{maxNodeOffset_}, upperBound{upperBound_}, lowerBound{lowerBound_} {}

    ~IFEMorsel();

    virtual void init();

    void resetNoLock(common::offset_t srcOffset);

    function::CallFuncMorsel getMorsel(uint64_t morselSize);

    virtual uint64_t getWork() = 0;

    virtual function::CallFuncMorsel getDstWriteMorsel(uint64_t morselSize) = 0;

    virtual bool isBFSCompleteNoLock() = 0;

    virtual bool isIFEMorselCompleteNoLock() = 0;

    void initializeNextFrontierNoLock();

public:
    std::mutex mutex;
    bool initializedIFEMorsel;
    uint64_t startTime;
    bool isSparseFrontier;
    uint8_t currentLevel;
    char padding0[64]{0};
    std::atomic<uint64_t> nextScanStartIdx;
    char padding1[64]{0};
    uint64_t currentFrontierSize;
    char padding2[64]{0};
    std::atomic<uint64_t> nextFrontierSize;
    char padding3[64]{0};
    common::offset_t srcOffset;

    // If the frontier is dense, then we use these 2 arrays as frontier and next frontier
    // Based on if the frontier size > (total nodes / 8)
    uint8_t* currentFrontier;
    uint8_t* nextFrontier;
    // If the frontier is sparse, then use this vector as frontier to extend
    // Based on if frontier size < (total nodes / 8)
    std::vector<common::offset_t> bfsFrontier;

    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
};

} // namespace function
} // namespace kuzu
