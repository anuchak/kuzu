#include "processor/operator/shortestpath/scan_semi_join.h"

#include "processor/operator/result_collector.h"

namespace kuzu {
namespace processor {

SingleSrcSPState* SimpleRecursiveJoinGlobalState::grabSrcDestMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset, uint64_t maxMorselSize) {
    std::unique_lock<std::shared_mutex> lck(mutex);
    for (auto& singleSrcSPState : singleSrcSPTracker) {
        if (singleSrcSPState->getThreadID() == threadID) {
            return singleSrcSPState.get();
        }
    }
    auto singleSrcSPState = std::make_unique<SingleSrcSPState>(threadID, maxNodeOffset);

    /// This will scan the factorized table to fetch 1 src and a set of dest nodes
    singleSrcSPState->setBFSMorsel(fTableOfSrcDest->getMorsel(maxMorselSize));

    singleSrcSPTracker.push_back(std::move(singleSrcSPState));
    return singleSrcSPTracker[singleSrcSPTracker.size() - 1].get();
}

void RecursiveScanSemiJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : outVecPositions) {
        auto valueVector = resultSet->getValueVector(dataPos);
        vectorsToScan.push_back(valueVector);
    }
    setMaxMorselSize();
}

bool RecursiveScanSemiJoin::getNextTuplesInternal() {
    auto singleSrcSPState =
        simpleRecursiveJoinGlobalState->grabSrcDestMorsel(threadID, maxNodeOffset, maxMorselSize);
    auto srcDestSPMorsel = singleSrcSPState->getSrcDestSPMorsel();
    if (srcDestSPMorsel->numTuples == 0) {
        return false;
    }
    if (singleSrcSPState->getBFSLevels().empty()) {
        srcDestSPMorsel->table->scan(vectorsToScan, srcDestSPMorsel->startTupleIdx,
            srcDestSPMorsel->numTuples, colIndicesToScan);
        metrics->numOutputTuple.increase(srcDestSPMorsel->numTuples);
        auto srcIdx = vectorsToScan[0]->state->selVector->selectedPositions[0];
        auto srcOffset = vectorsToScan[0]->readNodeOffset(srcIdx);
        auto bfsLevel = std::make_unique<BFSLevel>();
        bfsLevel->levelMinNodeOffset = srcOffset;
        bfsLevel->levelMaxNodeOffset = srcOffset;
        bfsLevel->currLevelNodeOffsets.push_back(srcOffset);
        bfsLevel->parentNodeOffsets.push_back(UINT64_MAX);
        singleSrcSPState->getBFSLevels().push_back(std::move(bfsLevel));
        singleSrcSPState->setMask(srcOffset, 1 /* Masker Index */);
    } else {
        singleSrcSPState->resetMask();
        auto& bfsLevels = singleSrcSPState->getBFSLevels();
        auto& lastBFSLevel = bfsLevels[bfsLevels.size() - 1];
        std::unique_lock<std::mutex> lck{lastBFSLevel->bfsLevelLock};
        common::offset_t maxOffset = 0, minOffset = UINT64_MAX;
        for (auto& nodeOffset : lastBFSLevel->currLevelNodeOffsets) {
            singleSrcSPState->setMask(nodeOffset, 1 /* Masker Index */);
            maxOffset = std::max(nodeOffset, maxOffset);
            minOffset = std::min(nodeOffset, minOffset);
        }
        lastBFSLevel->levelMinNodeOffset = minOffset;
        lastBFSLevel->levelMaxNodeOffset = maxOffset;
    }
    return true;
}

} // namespace processor
} // namespace kuzu
