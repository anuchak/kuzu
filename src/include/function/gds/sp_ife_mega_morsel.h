#pragma once

#include "sp_ife_morsel.h"

namespace kuzu {
namespace function {

struct SPIFEMegaMorsel : public IFEMorsel {
public:
    SPIFEMegaMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_,
        int batchSize)
        : IFEMorsel(upperBound_, lowerBound_, maxNodeOffset_,
              common::INVALID_OFFSET /* not using this */), isBFSActive{true},
          totalActiveLanes{0} {
        spIFEMorsels = std::vector<std::unique_ptr<SPIFEMorsel>>();
        isSparseFrontier = false;
        while (batchSize--) {
            spIFEMorsels.push_back(
                std::make_unique<SPIFEMorsel>(upperBound_, lowerBound_, maxNodeOffset_, srcOffset));
        }
    }

    void init() override;

    uint64_t getWork() override;

    function::CallFuncMorsel getDstWriteMorsel(uint64_t morselSize) override;

    bool isBFSCompleteNoLock() override;

    bool isIFEMorselCompleteNoLock() override;

    void initializeNextFrontierNoLock() override;

public:
    bool isBFSActive;
    int totalActiveLanes;
    std::vector<std::unique_ptr<SPIFEMorsel>> spIFEMorsels;
    std::atomic<uint64_t> nextDstScanStartIdx;
};

} // namespace function
} // namespace kuzu
