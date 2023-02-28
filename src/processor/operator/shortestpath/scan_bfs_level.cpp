#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SingleSrcSPState* SimpleRecursiveJoinGlobalState::grabSrcDestMorsel(
    std::thread::id threadID, common::offset_t maxNodeOffset, uint64_t maxMorselSize) {
    /// We are iterating over the single source SP global tracker to check if a threadID
    /// has been allotted a SP computation or not. If yes, we return the singleSrcSPState.
    /// Currently we have [1 thread -> 1 singleSrcSPState] enforced here.
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (singleSrcSPTracker.contains(threadID)) {
        return singleSrcSPTracker[threadID].get();
    }
    /// In case 1 thread is not allotted a singleSrcSPState yet, we use the fTable sharedState
    /// reference to allot a SrcDestSPMorsel to the thread. After this gets done, that thread is
    /// bound to that single SrcDestSPMorsel and will extend until it finishes.
    auto singleSrcSPState = std::make_unique<SingleSrcSPState>(maxNodeOffset);
    singleSrcSPState->setSrcDestSPMorsel(fTableOfSrcDest->getMorsel(maxMorselSize));
    return (singleSrcSPTracker[threadID] = std::move(singleSrcSPState)).get();
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : outVecPositions) {
        auto valueVector = resultSet->getValueVector(dataPos);
        vectorsToScan.push_back(valueVector);
    }
    maxMorselSize = simpleRecursiveJoinGlobalState->getFTableOfSrcDest()->getMaxMorselSize();
    bfsInputValueVector = resultSet->getValueVector(bfsInputVectorDataPos);
    bfsOutputValueVector = resultSet->getValueVector(bfsOutputVectorDataPos);
}

bool ScanBFSLevel::getNextTuplesInternal() {
    auto singleSrcSPState =
        simpleRecursiveJoinGlobalState->grabSrcDestMorsel(threadID, maxNodeOffset, maxMorselSize);
    auto& srcDestSPMorsel = singleSrcSPState->getSrcDestSPMorsel();
    if (srcDestSPMorsel->numTuples == 0) {
        return false;
    }
    if (singleSrcSPState->getBFSLevels().empty()) {
        srcDestSPMorsel->table->scan(vectorsToScan, srcDestSPMorsel->startTupleIdx,
            srcDestSPMorsel->numTuples, colIndicesToScan);
        auto selPos = vectorsToScan[0]->state->selVector->selectedPositions[0];
        auto nodeInternalID = ((internalID_t*)(vectorsToScan[0]->getData()))[selPos];
        bfsInputValueVector->setValue<internalID_t>(0 /* position */, nodeInternalID);
        bfsInputValueVector->state->initOriginalAndSelectedSize(1 /* size */);
        auto bfsLevel = std::make_unique<BFSLevel>();
        bfsLevel->nodeIDSelectedPos.push_back(selPos);
        singleSrcSPState->getBFSLevels().push_back(std::move(bfsLevel));
    } else {
        auto& bfsLevels = singleSrcSPState->getBFSLevels();
        auto& lastBFSLevelNodeIDPos = bfsLevels[bfsLevels.size() - 1]->nodeIDSelectedPos;
        int indexPos;
        for (indexPos = 0; indexPos < lastBFSLevelNodeIDPos.size(); indexPos++) {
            auto selPos = lastBFSLevelNodeIDPos[indexPos];
            auto nodeInternalID =
                ((common::internalID_t*)(bfsOutputValueVector->getData()))[selPos];
            bfsInputValueVector->setValue<common::internalID_t>(indexPos, nodeInternalID);
        }
        bfsInputValueVector->state->selVector->resetSelectorToUnselectedWithSize(indexPos);
        auto newBFSLevel = std::make_unique<BFSLevel>();
        bfsLevels.push_back(std::move(newBFSLevel));
    }
    return true;
}

} // namespace processor
} // namespace kuzu
