#include "processor/operator/shortestpath/simple_recursive_join.h"

#include "common/types/internal_id_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void SimpleRecursiveJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    dstValVector = resultSet->getValueVector(dstNodeDataPos);
    inputNodeIDVector = resultSet->getValueVector(inputNodeIDDataPos);
}

bool SimpleRecursiveJoin::getNextTuplesInternal() {
    while (true) {
        if (!children[0]->getNextTuple()) {
            /* TODO: The termination condition for SimpleRecursiveJoin needs a bit of discussion.
             * Specifically these are the main points :-
             *
             * 1) We can't return false as soon as the child operator returns false
             *    (ScanRelTableList). It returns false when it will finish extending a batch of
             *    nodes which is not a termination condition. Our condition is visiting all dest.
             *    nodes.
             *
             * 2) If there are 1-2 million dest. nodes then we can't keep collecting them in the
             *    outvaluevector and send them to the result collector only after we visited all.
             *    So we have to collect them 2048 batch size. There should be a break in the loop
             *    for that.
             */
            return false;
        }
        // fetch all the destination node offsets into a vector at once
        if (dstNodeOffsets.empty()) {
            for (int i = 0; i < dstValVector->state->selVector->selectedSize; i++) {
                auto destIdx = dstValVector->state->selVector->selectedPositions[i];
                if (!dstValVector->isNull(destIdx)) {
                    dstNodeOffsets.insert(dstValVector->readNodeOffset(destIdx));
                }
            }
        }
        auto singleSrcSPState = simpleRecursiveJoinGlobalState->getSingleSrcSPState(threadID);
        auto& nextBFSLevel = singleSrcSPState->nextBFSLevel;
        auto visitedNodesMap = singleSrcSPState->bfsVisitedNodes;
        auto nodeMask = singleSrcSPState->nodeMask;
        for (int i = 0; i < inputNodeIDVector->state->selVector->selectedSize; i++) {
            auto selectedPos = inputNodeIDVector->state->selVector->selectedPositions[i];
            auto nodeID = ((nodeID_t*)(inputNodeIDVector->getData()))[selectedPos];
            if (visitedNodesMap.contains(nodeID.offset)) {
                continue;
            }
            if (dstNodeOffsets.contains(nodeID.offset)) {
                // TODO: A destination node has been reached, write to output vector
            }
            visitedNodesMap.insert(nodeID.offset);
            // set position in mask to true to indicate node offset exists in bfsLevel.
            nodeMask[nodeID.offset] = true;
            nextBFSLevel->bfsLevelNodes[nodeID.offset] = nodeID;
        }
    }
}

} // namespace processor
} // namespace kuzu
