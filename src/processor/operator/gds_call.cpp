#include "processor/operator/gds_call.h"

#include "processor/result/factorized_table.h"

using namespace kuzu::binder;
using namespace kuzu::graph;

namespace kuzu {
namespace processor {

void GDSCallSharedState::merge(kuzu::processor::FactorizedTable& localFTable) {
    std::unique_lock lck{mtx};
    fTable->merge(localFTable);
}

std::vector<NodeSemiMask*> GDSCall::getSemiMasks() const {
    std::vector<NodeSemiMask*> masks;
    for (auto& [_, mask] : sharedState->inputNodeOffsetMasks) {
        masks.push_back(mask.get());
    }
    return masks;
}

void GDSCall::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    info.gds->init(sharedState.get(), context->clientContext);
}

void GDSCall::executeInternal(ExecutionContext* executionContext) {
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    info.gds->exec(executionContext);
    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
    printf("Total time taken: %lu\n", millis1 - millis);
}

} // namespace processor
} // namespace kuzu
