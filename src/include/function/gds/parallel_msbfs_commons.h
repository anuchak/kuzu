#pragma once

#include "common/task_system/task_scheduler.h"
#include "function/gds/msbfs_ife_morsel.h"
#include "gds.h"
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

typedef std::vector<
    std::pair<std::unique_ptr<MSBFSIFEMorsel<uint8_t>>, std::shared_ptr<ScheduledTask>>>
    scheduledTaskMapLane8;

typedef std::vector<
    std::pair<std::unique_ptr<MSBFSIFEMorsel<uint16_t>>, std::shared_ptr<ScheduledTask>>>
    scheduledTaskMapLane16;

typedef std::vector<
    std::pair<std::unique_ptr<MSBFSIFEMorsel<uint32_t>>, std::shared_ptr<ScheduledTask>>>
    scheduledTaskMapLane32;

typedef std::vector<
    std::pair<std::unique_ptr<MSBFSIFEMorsel<uint64_t>>, std::shared_ptr<ScheduledTask>>>
    scheduledTaskMapLane64;

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
    explicit ParallelMSBFSLocalState(bool isReturningPath = false)
        : GDSLocalState(), isReturningPath{isReturningPath}, ifeMorsel{nullptr},
          currentDstLane{UINT8_MAX}, dstScanMorsel{CallFuncMorsel::createInvalidMorsel()} {}

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
        if (isReturningPath) {
            pathVector =
                std::make_unique<ValueVector>(LogicalType::LIST(LogicalType::INTERNAL_ID()), mm);
            pathVector->state = dstNodeIDVector->state;
            ListVector::getDataVector(pathVector.get())->state = std::make_shared<DataChunkState>();
            outputVectors.push_back(pathVector.get());
        }
        nbrScanState = std::make_unique<graph::NbrScanState>(mm, isReturningPath);
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
        auto localState = std::make_unique<ParallelMSBFSLocalState>(isReturningPath);
        localState->ifeMorsel = ifeMorsel;
        return localState;
    }

public:
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
    bool isReturningPath;
    std::unique_ptr<ValueVector> pathVector;
    IFEMorsel* ifeMorsel;
    // Destination writing information
    // Destination lane & scan index should be set BEFORE launching task
    uint8_t currentDstLane;
    CallFuncMorsel dstScanMorsel;
};

} // namespace function
} // namespace kuzu
