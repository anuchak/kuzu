#include "processor/operator/shortestpath/simple_recursive_join.h"

namespace kuzu {
namespace processor {

void SimpleRecursiveJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    destValVector = resultSet->dataChunks[destNodeDataPos.dataChunkPos]
                        ->valueVectors[destNodeDataPos.valueVectorPos];
    adjNodeIDVector =
        std::make_shared<common::ValueVector>(common::INTERNAL_ID, context->memoryManager);
    adjNodeIDVector->state = std::make_shared<common::DataChunkState>();
    relIDVector = std::make_shared<common::ValueVector>(common::INT64, context->memoryManager);
    relIDVector->state = adjNodeIDVector->state;
    listHandles = std::make_shared<storage::ListHandle>(*listSyncState);
}

bool SimpleRecursiveJoin::getNextTuplesInternal() {
    while (true) {
        if (!children[0]->getNextTuple()) {
            return false;
        }
        // fetch all the destination node offsets into a vector at once
        if (destNodeOffsets.empty()) {
            for (int i = 0; i < destValVector->state->selVector->selectedSize; i++) {
                auto destIdx = destValVector->state->selVector->selectedPositions[i];
                if (!destValVector->isNull(destIdx)) {
                    destNodeOffsets.insert(destValVector->readNodeOffset(destIdx));
                }
            }
        }
        /// This operator will be extending from the ith Level to the {i+1}th Level here for ALL
        /// nodes present in the last BFSLevel. This will be changed in subsequent implementations
        /// where we will have a BFSLevelMorsel allotted to a specific thread that it will extend.
        auto singleSrcSPState = simpleRecursiveJoinGlobalState->getSingleSrcSPState(threadID);
        auto& bfsLevels = singleSrcSPState->getBFSLevels();
        auto& lastBFSLevel = bfsLevels[bfsLevels.size() - 1];
        std::unique_lock<std::mutex> lck{lastBFSLevel->bfsLevelLock};
        auto newBFSLevel = std::make_unique<BFSLevel>();
        for (int i = 0; i <= (lastBFSLevel->levelMaxNodeOffset - lastBFSLevel->levelMinNodeOffset);
             i++) {
            if (singleSrcSPState->isMasked(i + lastBFSLevel->levelMinNodeOffset)) {
                extendNode(i + lastBFSLevel->levelMinNodeOffset, newBFSLevel, &singleSrcSPState);
            }
        }
        singleSrcSPState->getBFSLevels().push_back(std::move(newBFSLevel));
    }
}

void SimpleRecursiveJoin::extendNode(common::offset_t parentNodeOffset,
    std::unique_ptr<BFSLevel>& bfsLevel, SingleSrcSPState** singleSrcSPState) {
    adjLists->initListReadingState(parentNodeOffset, *listHandle, transaction->getType());
    adjLists->readValues(transaction, adjNodeIDVector, *listHandle);
    relPropertyLists->readValues(transaction, relIDVector, *listHandles);
    addToNextFrontier(parentNodeOffset, bfsLevel, singleSrcSPState);
    while (getNextBatchOfChildNodes()) {
        relPropertyLists->readValues(transaction, relIDVector, *listHandles);
        addToNextFrontier(parentNodeOffset, bfsLevel, singleSrcSPState);
    }
}

void SimpleRecursiveJoin::addToNextFrontier(common::offset_t parentNodeOffset,
    std::unique_ptr<BFSLevel>& bfsLevel, SingleSrcSPState** singleSrcSPState) {
    auto visitedNodes = (*singleSrcSPState)->getVisitedNodes();
    for (auto pos = 0u; pos < adjNodeIDVector->state->selVector->selectedSize; pos++) {
        auto offsetIdx = adjNodeIDVector->state->selVector->selectedPositions[pos];
        auto nodeOffset = adjNodeIDVector->readNodeOffset(offsetIdx);
        if (visitedNodes.contains(nodeOffset)) {
            continue;
        }
        bfsLevel->currLevelNodeOffsets.push_back(nodeOffset);
        bfsLevel->parentNodeOffsets.push_back(parentNodeOffset);
        visitedNodes.insert(nodeOffset);

        /// This means we have hit a destination node while extending, so we hit output mode here
        if (destNodeOffsets.contains(nodeOffset)) {
            // TODO: Copy the path length to the output vector
        }
    }
}

bool SimpleRecursiveJoin::getNextBatchOfChildNodes() {
    if (listHandle->hasMoreAndSwitchSourceIfNecessary()) {
        adjLists->readValues(transaction, adjNodeIDVector, *listHandle);
        return true;
    }
    return false;
}

} // namespace processor
} // namespace kuzu
