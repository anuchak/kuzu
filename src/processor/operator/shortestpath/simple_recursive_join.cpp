#include "processor/operator/shortestpath/simple_recursive_join.h"

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
        /// This is the last bfs level into which we will add the newly read nodes from scan_rel_table
        auto& lastBFSLevel = bfsLevels[bfsLevels.size() - 1];
        auto visitedNodesMap = singleSrcSPState->getVisitedNodes();
        std::unordered_map<common::offset_t, common::sel_t> nextLevelOffsets =
            std::unordered_map<common::offset_t, common::sel_t>();
        common::offset_t levelBFSMinOffset = UINT64_MAX, levelBFSMaxOffset = 0;
        for (int i = 0; i < bfsOutputValueVector->state->selVector->selectedSize; i++) {
            auto selPos = bfsOutputValueVector->state->selVector->selectedPositions[i];
            auto nodeOffset = bfsOutputValueVector->readNodeOffset(selPos);
            if (visitedNodesMap.contains(nodeOffset)) {
                continue;
            }
            if (destNodeOffsets.contains(nodeOffset)) {
                // TODO: A destination node has been reached, write to output vector
            }
            visitedNodesMap.insert(nodeOffset);
            singleSrcSPState->setMask(nodeOffset);
            nextLevelOffsets[nodeOffset] = selPos;
            levelBFSMinOffset = std::min(levelBFSMinOffset, nodeOffset);
            levelBFSMaxOffset = std::max(levelBFSMaxOffset, nodeOffset);
        }
        for (auto i = levelBFSMinOffset; i <= levelBFSMaxOffset; i++) {
            if (singleSrcSPState->isMasked(i)) {
                lastBFSLevel->nodeIDSelectedPos.push_back(nextLevelOffsets[i]);
            }
        }
    }
}

} // namespace processor
} // namespace kuzu
