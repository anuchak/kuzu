#include "processor/operator/var_length_extend/bfs_state.h"

namespace kuzu {
namespace processor {

void SSSPMorsel::reset() {
    currentLevel = 0u;
    nextScanStartIdx = 0u;
    curBFSLevel.reset();
    nextBFSLevel.reset();
    numVisitedNodes = 0u;
    visitedNodes = std::vector<uint8_t>(maxOffset + 1, NOT_VISITED);
    distance = std::unordered_map<common::offset_t, uint16_t>();
    srcOffset = 0u;
    numThreadsActiveOnMorsel = 0u;
}

bool SSSPMorsel::isComplete() {
    if (curBFSLevel->size() == 0) { // no more to extend.
        return true;
    }
    if (currentLevel == upperBound) { // upper limit reached.
        return true;
    }
    if (numVisitedNodes == maxOffset) { // all destinations have been reached.
        return true;
    }
    return false;
}

void SSSPMorsel::markSrc() {
    visitedNodes[srcOffset] = VISITED;
    distance[srcOffset] = 0;
    numVisitedNodes++;
    curBFSLevel->bfsLevelNodes.push_back(srcOffset);
}

void SSSPMorsel::unmarkSrc() {
    visitedNodes[srcOffset] = NOT_VISITED;
    numVisitedNodes--;
}

void SSSPMorsel::markVisited(common::offset_t offset) {
    assert(visitedNodes[offset] == NOT_VISITED);
    visitedNodes[offset] = VISITED;
    distance[offset] = currentLevel + 1;
    numVisitedNodes++;
    nextBFSLevel->bfsLevelNodes.push_back(offset);
}

void SSSPMorsel::moveNextLevelAsCurrentLevel() {
    curBFSLevel = std::move(nextBFSLevel);
    currentLevel++;
    nextBFSLevel = std::make_unique<BFSLevel>();
    if (currentLevel == upperBound) { // No need to sort if we are not extending further.
        std::sort(curBFSLevel->bfsLevelNodes.begin(), curBFSLevel->bfsLevelNodes.end());
    }
}

common::offset_t BFSMorsel::getNextNodeOffset() {
    if (startScanIdx == endScanIdx) {
        return common::INVALID_NODE_OFFSET;
    }
    return curBFSLevel->bfsLevelNodes[startScanIdx++];
}

void BFSMorsel::addToLocalNextBFSLevel(
    const std::shared_ptr<common::ValueVector>& tmpDstNodeIDVector) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        if (!localBFSVisitedNodes.contains(nodeID.offset)) {
            localBFSVisitedNodes.insert(nodeID.offset);
        }
    }
}

bool MorselDispatcher::finishBFSMorsel(BFSMorsel* bfsMorsel) {
    std::unique_lock lck{mutex};
    if (ssspMorsel->isComplete()) {
        return true;
    }
    for (auto offset : bfsMorsel->localBFSVisitedNodes) {
        if (ssspMorsel->visitedNodes[offset] == NOT_VISITED) {
            ssspMorsel->markVisited(offset);
        }
    }
    ssspMorsel->numThreadsActiveOnMorsel--;
    if (ssspMorsel->numThreadsActiveOnMorsel == 0 &&
        ssspMorsel->nextScanStartIdx == ssspMorsel->curBFSLevel->size()) {
        ssspMorsel->moveNextLevelAsCurrentLevel();
        if (ssspMorsel->isComplete()) {
            state = SSSP_MORSEL_COMPLETE;
            return true;
        }
    }
    return false;
}

std::pair<SSSPComputationState, BFSMorsel*> MorselDispatcher::getBFSMorsel(
    const std::shared_ptr<FTableSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    const std::shared_ptr<common::ValueVector>& srcNodeIDVector, BFSMorsel* bfsMorsel) {
    std::unique_lock lck{mutex};
    if (state == SSSP_COMPUTATION_COMPLETE || state == SSSP_MORSEL_COMPLETE) {
        return {state, nullptr};
    }
    if (ssspMorsel->isComplete()) {
        auto inputFTableMorsel = inputFTableSharedState->getMorsel(1);
        // Marks end of SSSP Computation, no more source tuples to trigger SSSP.
        if (inputFTableMorsel->numTuples == 0) {
            state = SSSP_COMPUTATION_COMPLETE;
            return {state, nullptr};
        }
        inputFTableSharedState->getTable()->scan(vectorsToScan, inputFTableMorsel->startTupleIdx,
            inputFTableMorsel->numTuples, colIndicesToScan);
        ssspMorsel->reset();
        auto nodeID = srcNodeIDVector->getValue<common::nodeID_t>(
            srcNodeIDVector->state->selVector->selectedPositions[0]);
        ssspMorsel->srcOffset = nodeID.offset;
        ssspMorsel->markSrc();
    }
    // We don't swap here, because we want the last thread to complete its BFSMorsel and only then
    // swap the curBFSLevel and nextBFSLevel (happens in Line 93 in finishBFSMorsel function).
    if (ssspMorsel->nextScanStartIdx == ssspMorsel->curBFSLevel->size()) {
        return {state, nullptr};
    }
    ssspMorsel->numThreadsActiveOnMorsel++;
    auto bfsMorselSize = std::min(common::DEFAULT_VECTOR_CAPACITY,
        ssspMorsel->curBFSLevel->size() - ssspMorsel->nextScanStartIdx);
    auto morselScanEndIdx = ssspMorsel->nextScanStartIdx + bfsMorselSize;
    bfsMorsel = std::make_unique<BFSMorsel>(ssspMorsel->nextScanStartIdx, morselScanEndIdx).get();
    ssspMorsel->nextScanStartIdx += bfsMorselSize;
    return {state, bfsMorsel};
}

} // namespace processor
} // namespace kuzu
