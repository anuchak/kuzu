#include "processor/operator/shortestpath/scan_semi_join.h"

namespace kuzu {
namespace processor {

std::shared_ptr<BFSScanMorsel> SimpleRecursiveJoinSharedState::grabMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset, uint64_t maxMorselSize) {
    if (!localStateTracker.contains(threadID)) {
        auto localSharedState =
            std::make_unique<LocalSharedState>(maxNodeOffset, 0 /* masked flag */);
        localSharedState->setBFSMorsel(bfsfTableSharedState->getMorsel(maxMorselSize));
        localStateTracker[threadID] = std::move(localSharedState);
        return localStateTracker.at(threadID)->getBFSScanMorsel();
    } else {
        return localStateTracker.at(threadID)->getBFSScanMorsel();
    }
}

void RecursiveScanSemiJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    for (auto& dataPos : outVecPositions) {
        auto valueVector = resultSet->getValueVector(dataPos);
        vectorsToScan.push_back(valueVector);
    }
    setMaxMorselSize();
}

bool RecursiveScanSemiJoin::getNextTuplesInternal() {
    auto morsel = grabMorsel();
    if (morsel->getNumTuples() == 0) {
        return false;
    }
    morsel->getFTable()->scan(
        vectorsToScan, morsel->getStartIdx(), morsel->getNumTuples(), colIndicesToScan);
    metrics->numOutputTuple.increase(morsel->getNumTuples());
    return true;
}

} // namespace processor
} // namespace kuzu
