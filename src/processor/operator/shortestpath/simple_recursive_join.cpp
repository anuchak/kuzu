#include "processor/operator/shortestpath/simple_recursive_join.h"

#include "common/types/internal_id_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void SimpleRecursiveJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    threadID = std::this_thread::get_id();
    inputNodeIDVector = resultSet->getValueVector(inputNodeIDDataPos);
}

bool SimpleRecursiveJoin::getNextTuplesInternal() {
    while (true) {
        if (!children[0]->getNextTuple()) {
            return false;
        }
        auto singleSrcSPMorsel = simpleRecursiveJoinGlobalState->getSingleSrcSPMorsel(threadID);
        auto& nextBFSLevel = singleSrcSPMorsel->nextBFSLevel;
        auto& visitedNodes = *singleSrcSPMorsel->bfsVisitedNodes;
        auto nodeMask = singleSrcSPMorsel->nodeMask;
        for (int i = 0; i < inputNodeIDVector->state->selVector->selectedSize; i++) {
            auto selectedPos = inputNodeIDVector->state->selVector->selectedPositions[i];
            auto nodeID = ((nodeID_t*)(inputNodeIDVector->getData()))[selectedPos];
            if (visitedNodes[nodeID.offset] == VISITED ||
                visitedNodes[nodeID.offset] == VISITED_DST) {
                continue;
            }
            if (visitedNodes[nodeID.offset] == NOT_VISITED_DST) {
                visitedNodes[nodeID.offset] = VISITED_DST;
                singleSrcSPMorsel->dstNodeDistances->operator[](nodeID.offset) =
                    nextBFSLevel->bfsLevelNumber;
            } else {
                visitedNodes[nodeID.offset] = VISITED;
            }
            // set position in mask to true to indicate node offset exists in bfsLevel.
            nodeMask[nodeID.offset] = true;
            nextBFSLevel->bfsLevelNodes[nodeID.offset] = nodeID;
        }
    }
}

} // namespace processor
} // namespace kuzu
