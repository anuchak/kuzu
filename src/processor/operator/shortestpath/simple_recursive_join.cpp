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
            auto ssspMorsel = simpleRecursiveJoinGlobalState->getAssignedSSSPMorsel(threadID);
            if (!ssspMorsel) {
                return false;
            } else {
                return true;
            }
        }
        auto ssspMorsel = simpleRecursiveJoinGlobalState->getAssignedSSSPMorsel(threadID);
        auto& visitedNodes = ssspMorsel->bfsVisitedNodes;
        for (int i = 0; i < inputNodeIDVector->state->selVector->selectedSize; i++) {
            auto selectedPos = inputNodeIDVector->state->selVector->selectedPositions[i];
            auto nodeID = ((nodeID_t*)(inputNodeIDVector->getData()))[selectedPos];
            if (visitedNodes[nodeID.offset] == VISITED ||
                visitedNodes[nodeID.offset] == VISITED_DST) {
                continue;
            }
            if (visitedNodes[nodeID.offset] == NOT_VISITED_DST) {
                visitedNodes[nodeID.offset] = VISITED_DST;
                ssspMorsel->numDstNodesNotReached--;
                ssspMorsel->dstNodeDistances[nodeID.offset] = ssspMorsel->nextBFSLevel->levelNumber;
            } else {
                visitedNodes[nodeID.offset] = VISITED;
            }
            ssspMorsel->nextBFSLevel->bfsLevelNodes.emplace_back(nodeID);
        }
    }
}

} // namespace processor
} // namespace kuzu
