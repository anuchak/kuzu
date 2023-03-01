#include "processor/operator/shortestpath/simple_recursive_join.h"

#include "common/types/internal_id_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void SimpleRecursiveJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    destValVector = resultSet->getValueVector(destNodeDataPos);
    bfsOutputValueVector = resultSet->getValueVector(bfsOutputVectorDataPos);
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
        auto singleSrcSPState = simpleRecursiveJoinGlobalState->getSingleSrcSPState(threadID);
        singleSrcSPState->resetMask();
        auto& bfsLevels = singleSrcSPState->getBFSLevels();
        auto& lastBFSLevel = bfsLevels[bfsLevels.size() - 1];
        auto visitedNodesMap = singleSrcSPState->getVisitedNodes();
        std::unordered_map<offset_t, nodeID_t> nodeOffsetIDMap = std::unordered_map<offset_t, nodeID_t>();
        common::offset_t levelBFSMinOffset = UINT64_MAX, levelBFSMaxOffset = 0;
        for (int i = 0; i < bfsOutputValueVector->state->selVector->selectedSize; i++) {
            auto selectedPos = bfsOutputValueVector->state->selVector->selectedPositions[i];
            auto nodeID = ((nodeID_t*)(bfsOutputValueVector->getData()))[selectedPos];
            if (visitedNodesMap.contains(nodeID.offset)) {
                continue;
            }
            if (destNodeOffsets.contains(nodeID.offset)) {
                // TODO: A destination node has been reached, write to output vector
            }
            visitedNodesMap.insert(nodeID.offset);
            singleSrcSPState->setMask(nodeID.offset);
            nodeOffsetIDMap[nodeID.offset] = nodeID;
            levelBFSMinOffset = std::min(levelBFSMinOffset, nodeID.offset);
            levelBFSMaxOffset = std::max(levelBFSMaxOffset, nodeID.offset);
        }
        for (auto nodeOffset = levelBFSMinOffset; nodeOffset <= levelBFSMaxOffset; nodeOffset++) {
            if (singleSrcSPState->isMasked(nodeOffset)) {
                lastBFSLevel->bfsLevelNodes.push_back(nodeOffsetIDMap[nodeOffset]);
            }
        }
    }
}

} // namespace processor
} // namespace kuzu
