#pragma once

#include "common/task_system/task_scheduler.h"
#include "gds.h"
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

typedef std::vector<std::pair<std::unique_ptr<IFEMorsel>, std::shared_ptr<ScheduledTask>>>
    scheduledTaskMap;

struct ParallelShortestPathBindData final : public GDSBindData {
    uint8_t upperBound;
    std::string bfsPolicy;

    ParallelShortestPathBindData(std::shared_ptr<Expression> nodeInput, uint8_t upperBound,
        std::string bfsPolicy)
        : GDSBindData{std::move(nodeInput)}, upperBound{upperBound},
          bfsPolicy{std::move(bfsPolicy)} {}
    ParallelShortestPathBindData(const ParallelShortestPathBindData& other) = default;

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ParallelShortestPathBindData>(*this);
    }
};

class ParallelShortestPathLocalState : public GDSLocalState {
public:
    explicit ParallelShortestPathLocalState(bool isReturningPath = false) : GDSLocalState(),
          isReturningPath{isReturningPath}, ifeMorsel{nullptr} {}

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
            pathVector = std::make_unique<ValueVector>(LogicalType::LIST(LogicalType::INTERNAL_ID()), mm);
            pathVector->state = dstNodeIDVector->state;
            ListVector::getDataVector(pathVector.get())->state =
                std::make_shared<DataChunkState>();
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
        auto localState = std::make_unique<ParallelShortestPathLocalState>(isReturningPath);
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
};

} // namespace function
} // namespace kuzu
