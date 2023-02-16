#include "processor/operator/shortestpath/simple_recursive_join.h"

namespace kuzu {
namespace processor {

void SimpleRecursiveJoin::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    srcValVector = resultSet->dataChunks[srcNodesDataPos.dataChunkPos]
                       ->valueVectors[srcNodesDataPos.valueVectorPos];
    destValVector = resultSet->dataChunks[destNodeDataPos.dataChunkPos]
                        ->valueVectors[destNodeDataPos.valueVectorPos];
}

bool SimpleRecursiveJoin::getNextTuplesInternal() {
    auto threadID = std::this_thread::get_id();
    while (true) {
        if (!children[0]->getNextTuple()) {
            // TODO: go to output mode
        }
        // TODO: construct BFSLevel {i+1} here
        // If control reaches here, it means current thread allotted a morsel
        auto localState = simpleRecursiveJoinSharedState->getLocalStateTracker().at(threadID);
        auto bfsMorsel = localState->getBFSScanMorsel();
        auto& bfsLevels = localState->getBFSLevels();
        if(bfsLevels.empty()) {
            // TODO: first level to be scanned from factorized table
        } else {
            // reset mask to scan rel table
            localState->getSemiJoinFilter()->resetMask();

            auto &lastBFSLevel = bfsLevels[bfsLevels.size() - 1];
            std::unique_lock<std::mutex> lck{lastBFSLevel.bfsLevelLock};
        }
    }
}

} // namespace processor
} // namespace kuzu
