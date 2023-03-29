#include "processor/operator/shortestpath/scan_bfs_level.h"

#include "common/vector/value_vector_utils.h"
#include "processor/operator/result_collector.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

BFSLevelMorsel SSSPMorsel::getBFSLevelMorsel() {
    std::unique_lock<std::shared_mutex> lck(mutex);
    if (bfsMorselNextStartIdx == curBFSLevel->bfsLevelNodes.size()) {
        return BFSLevelMorsel(bfsMorselNextStartIdx, 0 /* bfsLevelMorsel size */);
    }
    auto bfsLevelMorselSize = std::min(
        DEFAULT_VECTOR_CAPACITY, curBFSLevel->bfsLevelNodes.size() - bfsMorselNextStartIdx);
    bfsMorselNextStartIdx += bfsLevelMorselSize;
    return BFSLevelMorsel(bfsMorselNextStartIdx - bfsLevelMorselSize, bfsLevelMorselSize);
}

// This function is required to track which node offsets are destination nodes, they are tracked
// by a different state NOT_VISITED_DST and set as VISITED_DST if they are visited later.
void SSSPMorsel::markDstNodeOffsets(
    common::offset_t srcNodeOffset, common::ValueVector* dstNodeIDValueVector) {
    for (int i = 0; i < dstNodeIDValueVector->state->selVector->selectedSize; i++) {
        auto destIdx = dstNodeIDValueVector->state->selVector->selectedPositions[i];
        if (!dstNodeIDValueVector->isNull(destIdx)) {
            auto destNodeOffset = dstNodeIDValueVector->readNodeOffset(destIdx);
            // This is an edge case we need to consider, if the source is a part of the destination
            // nodes, then we should not count it separately as a destination. Set it as visited
            // right here and place distance as 0;
            if (destNodeOffset == srcNodeOffset) {
                bfsVisitedNodes[srcNodeOffset] = VISITED_DST;
                dstNodeDistances[srcNodeOffset] = 0;
            } else {
                bfsVisitedNodes[destNodeOffset] = NOT_VISITED_DST;
                numDstNodesNotReached++;
            }
        }
    }
}

SSSPMorsel* SimpleRecursiveJoinGlobalState::getAssignedSSSPMorsel(std::thread::id threadID) {
    std::unique_lock<std::shared_mutex> lck{mutex};
    if (ssspMorselPerThread.contains(threadID)) {
        return ssspMorselPerThread[threadID].get();
    }
    return nullptr;
}

void SimpleRecursiveJoinGlobalState::removePrevAssignedSSSPMorsel(std::thread::id threadID) {
    std::unique_lock<std::shared_mutex> lck{mutex};
    if (ssspMorselPerThread.contains(threadID)) {
        ssspMorselPerThread.erase(threadID);
    }
}

/*
 * This function initialises a new SSSPMorsel, it completes the following operations:
 *
 * 1) Scanning the FTable to load the source and destination nodeIDs into the ValueVectors.
 * 2) Setting the destination node offset positions.
 * 3) Initialises bfsLevelNodes of curBFSLevel, adds the source nodeID.
 */
SSSPMorsel* SimpleRecursiveJoinGlobalState::getSSSPMorsel(std::thread::id threadID,
    common::offset_t maxNodeOffset, std::vector<common::ValueVector*> srcDstValueVectors,
    std::vector<uint32_t>& ftColIndicesToScan) {
    auto ssspMorsel = std::make_unique<SSSPMorsel>(maxNodeOffset);
    // If there are no morsels left, numTuples will be 0 for the srcDstFTableMorsel.
    std::unique_ptr<FTableScanMorsel> inputFTableMorsel =
        std::move(inputFTable->getMorsel(1 /* morsel size */));
    if (inputFTableMorsel->numTuples == 0) {
        removePrevAssignedSSSPMorsel(threadID);
        return nullptr;
    }
    // Reset the unflat destination value vector size and selected positions to default (2048).
    srcDstValueVectors[1]->state->selVector->resetSelectorToUnselected();
    inputFTableMorsel->table->scan(srcDstValueVectors, inputFTableMorsel->startTupleIdx,
        inputFTableMorsel->numTuples, ftColIndicesToScan);
    auto srcNodeID = ((nodeID_t*)(srcDstValueVectors[0]->getData()))[0];
    ssspMorsel->markDstNodeOffsets(srcNodeID.offset, srcDstValueVectors[1]);
    ssspMorsel->dstTableID = ((nodeID_t*)(srcDstValueVectors[1]->getData()))[0].tableID;
    // curBFSLevel's levelNumber is already set as 0 in constructor of BFSLevel.
    ssspMorsel->curBFSLevel->bfsLevelNodes.push_back(srcNodeID);
    ssspMorsel->nextBFSLevel->levelNumber = ssspMorsel->curBFSLevel->levelNumber + 1;
    std::unique_lock<std::shared_mutex> lck{mutex};
    return (ssspMorselPerThread[threadID] = std::move(ssspMorsel)).get();
}

void ScanBFSLevel::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    for (auto& dataPos : srcDstVectorsDataPos) {
        srcDstValueVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
    dstDistances = resultSet->getValueVector(dstDistanceVectorDataPos);
    nodesToExtend = resultSet->getValueVector(nodesToExtendDataPos);
}

bool ScanBFSLevel::getNextTuplesInternal() {
    if (!ssspMorsel || ssspMorsel->isSSSPMorselComplete) {
        ssspMorsel = simpleRecursiveJoinGlobalState->getSSSPMorsel(
            threadID, maxNodeOffset, srcDstValueVectors, ftColIndicesToScan);
        if (!ssspMorsel) {
            return false;
        }
    }
    // If numDstNodesNotReached is 0, it indicates we have visited ALL our destination nodes.
    if (ssspMorsel->numDstNodesNotReached == 0) {
        writeDistToOutputVector();
        ssspMorsel->isSSSPMorselComplete = true;
        return false;
    }
    auto bfsLevelMorsel = ssspMorsel->getBFSLevelMorsel();
    // If there are no more nodes to extend in curBFSLevel, there are two cases:
    // - the nextBFSLevel is empty, meaning all have been visited, termination condition.
    // - the curBFSLevel level is (upperBound-1), meaning we have reached upper limit of BFS.
    // If BOTH these conditions are false, just swap curBFSLevel with nextBFSLevel to extend next
    // level of BFS nodeID's.
    if (bfsLevelMorsel.isEmpty()) {
        if (ssspMorsel->nextBFSLevel->isEmpty() ||
            ssspMorsel->curBFSLevel->levelNumber == bfsUpperBound - 1) {
            writeDistToOutputVector();
            ssspMorsel->isSSSPMorselComplete = true;
            return false;
        } else {
            ssspMorsel->bfsMorselNextStartIdx = 0u;
            ssspMorsel->curBFSLevel = std::move(ssspMorsel->nextBFSLevel);
            // Sort node offsets of BFS level in ascending order for sequential scan of adjList.
            std::sort(ssspMorsel->curBFSLevel->bfsLevelNodes.begin(),
                ssspMorsel->curBFSLevel->bfsLevelNodes.end(), NodeIDComparatorFunction());
            ssspMorsel->nextBFSLevel = std::make_unique<BFSLevel>();
            ssspMorsel->nextBFSLevel->levelNumber = ssspMorsel->curBFSLevel->levelNumber + 1;
            bfsLevelMorsel = ssspMorsel->getBFSLevelMorsel();
        }
    }
    copyNodeIDsToVector(*ssspMorsel->curBFSLevel, bfsLevelMorsel);
    return true;
}

// Write (only) reached destination distances to output value vector.
void ScanBFSLevel::writeDistToOutputVector() {
    auto dstValVector = srcDstValueVectors[1];
    auto srcValVector = srcDstValueVectors[0];
    std::vector<uint16_t> newSelPositions = std::vector<uint16_t>();
    for (int i = 0; i < dstValVector->state->selVector->selectedSize; i++) {
        auto destIdx = dstValVector->state->selVector->selectedPositions[i];
        if (!dstValVector->isNull(destIdx)) {
            auto nodeOffset = dstValVector->readNodeOffset(destIdx);
            auto visitedState = ssspMorsel->bfsVisitedNodes[nodeOffset];
            auto distValue = ssspMorsel->dstNodeDistances[nodeOffset];
            if (visitedState == VISITED_DST && distValue >= bfsLowerBound) {
                newSelPositions.push_back(destIdx);
                dstDistances->setValue<int64_t>(destIdx, distValue);
            }
        }
    }
    // In case NO destination was reached in an SSSPMorsel, then the source should also not be
    // printed in the final output. This is the same semantics as Neo4j, where an empty result is
    // returned when no dest is reached. That's why we set both srcValVector and the dstValVector
    // sharedState's selectedSize to be 0.
    if (newSelPositions.empty()) {
        dstValVector->state->selVector->selectedSize = 0u;
        srcValVector->state->selVector->selectedSize = 0u;
        return;
    }
    dstValVector->state->selVector->resetSelectorToValuePosBufferWithSize(newSelPositions.size());
    for (int i = 0; i < newSelPositions.size(); i++) {
        dstValVector->state->selVector->selectedPositions[i] = newSelPositions[i];
    }
}

void ScanBFSLevel::copyNodeIDsToVector(BFSLevel& curBFSLevel, BFSLevelMorsel& bfsLevelMorsel) {
    auto finalScanIdx = bfsLevelMorsel.startIdx + bfsLevelMorsel.size;
    for (auto idx = bfsLevelMorsel.startIdx; idx < finalScanIdx; idx++) {
        auto nodeID = curBFSLevel.bfsLevelNodes[idx];
        nodesToExtend->setValue<nodeID_t>(idx - bfsLevelMorsel.startIdx, nodeID);
    }
    nodesToExtend->state->initOriginalAndSelectedSize(bfsLevelMorsel.size);
}

} // namespace processor
} // namespace kuzu
