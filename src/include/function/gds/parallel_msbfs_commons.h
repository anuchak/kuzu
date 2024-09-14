#pragma once

#include "common/task_system/task_scheduler.h"
#include "gds.h"
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

typedef std::vector<std::pair<std::unique_ptr<IFEMorsel>, std::shared_ptr<ScheduledTask>>>
    scheduledTaskMap;

struct ParallelMSBFSPathBindData final : public GDSBindData {
    uint8_t upperBound;
    std::string bfsPolicy;
    int laneWidth;

    ParallelMSBFSPathBindData(std::shared_ptr<Expression> nodeInput, uint8_t upperBound,
        std::string bfsPolicy, int laneWidth)
        : GDSBindData{std::move(nodeInput)}, upperBound{upperBound},
          bfsPolicy{std::move(bfsPolicy)}, laneWidth{laneWidth} {}
    ParallelMSBFSPathBindData(const ParallelMSBFSPathBindData& other) = default;

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ParallelMSBFSPathBindData>(*this);
    }
};

struct ParallelMSBFSLocalState : public GDSLocalState {
public:
    explicit ParallelMSBFSLocalState() : ifeMorsel{nullptr}, currentDstLane{UINT8_MAX},
          dstScanMorsel{CallFuncMorsel::createInvalidMorsel()} {}

    void init(main::ClientContext* clientContext) override {
        auto mm = clientContext->getMemoryManager();
        srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        lengthVector = std::make_unique<ValueVector>(LogicalType::INT64(), mm);
        srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        dstNodeIDVector->state = std::make_shared<DataChunkState>();
        lengthVector->state = dstNodeIDVector->state;
        outputVectors.push_back(srcNodeIDVector.get());
        outputVectors.push_back(dstNodeIDVector.get());
        outputVectors.push_back(lengthVector.get());
        nbrScanState = std::make_unique<graph::NbrScanState>(mm);
    }

    /*
     * Return the amount of work currently being processed on for this IFEMorsel.
     * Based on the frontier on which BFS extension is being done, or if output is being written.
     */
    inline uint64_t getWork() override {
        if (!ifeMorsel || !ifeMorsel->initializedIFEMorsel) {
            return 0u;
        }
        return ifeMorsel->getWork();
    }

    std::unique_ptr<GDSLocalState> copy() override {
        auto localState = std::make_unique<ParallelMSBFSLocalState>();
        localState->ifeMorsel = ifeMorsel;
        return localState;
    }

public:
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
    IFEMorsel* ifeMorsel;
    // Destination writing information
    // Destination lane & scan index should be set BEFORE launching task
    uint8_t currentDstLane;
    CallFuncMorsel dstScanMorsel;
};

} // namespace function
} // namespace kuzu
