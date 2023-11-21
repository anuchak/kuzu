#include "processor/operator/recursive_extend/recursive_join.h"

#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/ms_bfs_morsel.h"
#include "processor/operator/recursive_extend/scan_frontier.h"
#include "processor/operator/recursive_extend/variable_length_state.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

// User's setting of scheduler policy is given preference. If user changes policy to 1T1S then
// before operator begins execution we change the policy globally here.
void RecursiveJoin::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    if ((context->clientContext->getBFSSchedulerType() == SchedulerType::OneThreadOneMorsel) &&
        (sharedState->morselDispatcher->getSchedulerType() != SchedulerType::Reachability)) {
        sharedState->morselDispatcher->setSchedulerType(SchedulerType::OneThreadOneMorsel);
        return;
    }
    if (sharedState->morselDispatcher->getSchedulerType() == SchedulerType::nThreadkMorsel) {
        printf("Setting max active bfs at any given point to: %lu\n",
            context->clientContext->getMaxActiveBFSSharedState());
        sharedState->morselDispatcher->initActiveBFSSharedState(
            context->clientContext->getMaxActiveBFSSharedState());
    }
}

void RecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    for (auto& dataPos : vectorsToScanPos) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos).get());
    }
    populateTargetDstNodes();
    vectors = std::make_unique<RecursiveJoinVectors>();
    vectors->srcNodeIDVector = resultSet->getValueVector(dataInfo->srcNodePos).get();
    vectors->dstNodeIDVector = resultSet->getValueVector(dataInfo->dstNodePos).get();
    vectors->pathLengthVector = resultSet->getValueVector(dataInfo->pathLengthPos).get();
    std::vector<std::unique_ptr<BaseFrontierScanner>> scanners;
    switch (queryRelType) {
    case QueryRelType::VARIABLE_LENGTH: {
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo->pathPos).get();
            bfsMorsel = std::make_unique<VariableLengthMorsel<true /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsMorsel = std::make_unique<VariableLengthMorsel<false /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            throw NotImplementedException("BaseRecursiveJoin::initLocalStateInternal");
        }
    } break;
    case QueryRelType::SHORTEST: {
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo->pathPos).get();
            bfsMorsel = std::make_unique<ShortestPathMorsel<true /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsMorsel = std::make_unique<ShortestPathMorsel<false /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<DstNodeScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            throw NotImplementedException("BaseRecursiveJoin::initLocalStateInternal");
        }
    } break;
    case QueryRelType::ALL_SHORTEST: {
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo->pathPos).get();
            bfsMorsel = std::make_unique<AllShortestPathMorsel<true /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsMorsel = std::make_unique<AllShortestPathMorsel<false /* TRACK_PATH */>>(
                upperBound, lowerBound, targetDstNodes.get());
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            throw NotImplementedException("BaseRecursiveJoin::initLocalStateInternal");
        }
    } break;
    case QueryRelType::REACHABILITY: {
        // Need to confirm this here since REACHABILITY should be triggered only if no path being
        // tracked.
        assert(joinType != planner::RecursiveJoinType::TRACK_PATH);
        bfsMorsel = std::make_unique<MSBFSMorsel<false /* TRACK_PATH */>>(
            upperBound, lowerBound, sharedState->getMaxOffset(), targetDstNodes.get());
    } break;
    default:
        throw NotImplementedException("BaseRecursiveJoin::initLocalStateInternal");
    }
    if (vectors->pathVector != nullptr) {
        auto pathNodesFieldIdx =
            StructType::getFieldIdx(&vectors->pathVector->dataType, InternalKeyword::NODES);
        vectors->pathNodesVector =
            StructVector::getFieldVector(vectors->pathVector, pathNodesFieldIdx).get();
        auto pathNodesDataVector = ListVector::getDataVector(vectors->pathNodesVector);
        auto pathNodesIDFieldIdx =
            StructType::getFieldIdx(&pathNodesDataVector->dataType, InternalKeyword::ID);
        vectors->pathNodesIDDataVector =
            StructVector::getFieldVector(pathNodesDataVector, pathNodesIDFieldIdx).get();
        auto pathRelsFieldIdx =
            StructType::getFieldIdx(&vectors->pathVector->dataType, InternalKeyword::RELS);
        vectors->pathRelsVector =
            StructVector::getFieldVector(vectors->pathVector, pathRelsFieldIdx).get();
        auto pathRelsDataVector = ListVector::getDataVector(vectors->pathRelsVector);
        auto pathRelsSrcIDFieldIdx =
            StructType::getFieldIdx(&pathRelsDataVector->dataType, InternalKeyword::SRC);
        vectors->pathRelsSrcIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsSrcIDFieldIdx).get();
        auto pathRelsDstIDFieldIdx =
            StructType::getFieldIdx(&pathRelsDataVector->dataType, InternalKeyword::DST);
        vectors->pathRelsDstIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsDstIDFieldIdx).get();
        auto pathRelsIDFieldIdx =
            StructType::getFieldIdx(&pathRelsDataVector->dataType, InternalKeyword::ID);
        vectors->pathRelsIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsIDFieldIdx).get();
    }
    frontiersScanner = std::make_unique<FrontiersScanner>(std::move(scanners));
    initLocalRecursivePlan(context);
}

bool RecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
    if (targetDstNodes->getNumNodes() == 0) {
        return false;
    }
    // There are two high level steps.
    //
    // (1) BFS Computation phase: Grab a new source to do a BFS and compute an entire BFS starting
    // from a single source;
    //
    // (2) Outputting phase: Once a BFS from a single source finishes, we output the results in
    // pieces of vectors to the parent operator.
    //
    // These 2 steps are repeated iteratively until all sources to do a BFS are exhausted. The first
    // if statement checks if we are in the outputting phase and if so, scans a vector to output and
    // returns true. Otherwise, we compute a new BFS.
    while (true) {
        if (scanOutput()) { // Phase 2
            return true;
        }
        if (!computeBFS(context)) {
            return false;
        }
        frontiersScanner->resetState(*bfsMorsel);
    }
}

/**
 * This function differs based on scheduler type. For OneThreadOneMorsel the final results writing
 * is not shared with other threads. In the other case, other threads can help in writing final
 * distance values. Return value policy for both is same, true if some value was written to the
 * vector and false if no values were written.
 * @return - true if some values were written to the ValueVector, else false
 */
bool RecursiveJoin::scanOutput() {
    if (sharedState->getSchedulerType() == SchedulerType::OneThreadOneMorsel) {
        common::sel_t offsetVectorSize = 0u;
        common::sel_t nodeIDDataVectorSize = 0u;
        common::sel_t relIDDataVectorSize = 0u;
        if (vectors->pathVector != nullptr) {
            vectors->pathVector->resetAuxiliaryBuffer();
        }
        frontiersScanner->scan(
            vectors.get(), offsetVectorSize, nodeIDDataVectorSize, relIDDataVectorSize);
        if (offsetVectorSize == 0) {
            return false;
        }
        vectors->dstNodeIDVector->state->initOriginalAndSelectedSize(offsetVectorSize);
        return true;
    } else if (sharedState->getSchedulerType() == SchedulerType::Reachability) {
        auto msBFSMorsel = (reinterpret_cast<MSBFSMorsel<false>*>(bfsMorsel.get()));
        auto tableID = *begin(dataInfo->dstNodeTableIDs);
        int64_t numValuesWritten;
        while ((numValuesWritten = msBFSMorsel->writeToVector(tableID, vectors.get())) == 0) {
            // keep pulling until no. of values written is not 0 or -1
        }
        if (numValuesWritten > 0) {
            return true;
        }
        return false;
    } else {
        while (true) {
            if (!bfsMorsel->hasBFSSharedState()) {
                return false;
            }
            auto tableID = *begin(dataInfo->dstNodeTableIDs);
            auto ret = sharedState->writeDstNodeIDAndPathLength(
                vectorsToScan, colIndicesToScan, tableID, bfsMorsel, vectors.get());
            /**
             * ret > 0: non-zero path lengths were written to vector, return to parent op
             * ret < 0: path length writing is complete, proceed to computeBFS for another morsel
             * ret = 0: the distance morsel received was empty, go back to get another morsel
             */
            if (ret > 0) {
                return true;
            }
            if (ret < 0) {
                return false;
            }
        }
    }
}

/**
 * The main computeBFS to be called for both 1T1S and nTkS scheduling policies. Initially for both
 * cases, the BFSSharedState ptr will be null.
 * - For nTkS policy, this will be filled with a pointer to the main BFSSharedState from which work
 *   will be fetched. This BFSSharedState will not be bound to a specific thread and can change.
 * - For 1T1S policy, there is no BFSSharedState involved and a thread works on its own morsel, not
 *   shared with any other thread.
 */
bool RecursiveJoin::computeBFS(kuzu::processor::ExecutionContext* context) {
    if (sharedState->getSchedulerType() == SchedulerType::OneThreadOneMorsel) {
        auto state = sharedState->getBFSMorsel(vectorsToScan, colIndicesToScan,
            vectors->srcNodeIDVector, bfsMorsel.get(), queryRelType, joinType);
        if (state.first == COMPLETE) {
            return false;
        }
        computeBFSOneThreadOneMorsel(context);
        return true;
    } else if (sharedState->getSchedulerType() == SchedulerType::Reachability) {
        auto state = sharedState->getBFSMorsel(vectorsToScan, colIndicesToScan,
            vectors->srcNodeIDVector, bfsMorsel.get(), queryRelType, joinType);
        if (state.first == COMPLETE) {
            return false;
        }
        computeMSBFSMorsel(context);
        return true;
    } else {
        return doBFSnThreadkMorsel(context);
    }
}

bool RecursiveJoin::doBFSnThreadkMorsel(kuzu::processor::ExecutionContext* context) {
    while (true) {
        if (bfsMorsel->hasBFSSharedState()) {
            auto state = bfsMorsel->getBFSMorsel();
            if (state == EXTEND_IN_PROGRESS) {
                computeBFSnThreadkMorsel(context);
                if (bfsMorsel->finishBFSMorsel(queryRelType)) {
                    return true;
                }
                continue;
            }
            if (state == PATH_LENGTH_WRITE_IN_PROGRESS) {
                return true;
            }
        }
        auto state = sharedState->getBFSMorsel(vectorsToScan, colIndicesToScan,
            vectors->srcNodeIDVector, bfsMorsel.get(), queryRelType, joinType);
        if (state.first == COMPLETE) {
            return false;
        }
        if (state.second == PATH_LENGTH_WRITE_IN_PROGRESS) {
            return true;
        }
        if (state.second == EXTEND_IN_PROGRESS) {
            computeBFSnThreadkMorsel(context);
            if (bfsMorsel->finishBFSMorsel(queryRelType)) {
                return true;
            }
        } else {
            std::this_thread::sleep_for(
                std::chrono::microseconds(common::THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        }
    }
}

// Used for nTkS scheduling policy, extension is done morsel at a time of size 2048. Each thread
// is operating on its morsel. Lock-free CAS operation occurs here, visited nodes are marked with
// the function call addToLocalNextBFSLevel on the visited vector. The path lengths are written to
// the pathLength vector.
void RecursiveJoin::computeBFSnThreadkMorsel(ExecutionContext* context) {
    // Cast the BaseBFSMorsel to ShortestPathMorsel, the TRACK_NONE RecursiveJoin is the case it is
    // applicable for. If true, indicates TRACK_PATH is true else TRACK_PATH is false.
    common::offset_t nodeOffset = bfsMorsel->getNextNodeOffset();
    uint64_t boundNodeMultiplicity;
    while (nodeOffset != common::INVALID_OFFSET) {
        boundNodeMultiplicity = bfsMorsel->getBoundNodeMultiplicity(nodeOffset);
        scanFrontier->setNodeID(common::nodeID_t{nodeOffset, *begin(dataInfo->dstNodeTableIDs)});
        while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
            bfsMorsel->addToLocalNextBFSLevel(vectors.get(), boundNodeMultiplicity, nodeOffset);
        }
        nodeOffset = bfsMorsel->getNextNodeOffset();
    }
}

// Used for 1T1S scheduling policy, an offset at a time BFS extension is done, and then we check
// if the BFS is complete or not. No lock-free CAS operation occurring on the visited vector here.
// Work not shared between any other threads, the data structures used are unordered_set and
// unordered_map.
void RecursiveJoin::computeBFSOneThreadOneMorsel(ExecutionContext* context) {
    while (!bfsMorsel->isComplete()) {
        auto boundNodeID = bfsMorsel->getNextNodeID();
        if (boundNodeID.offset != common::INVALID_OFFSET) {
            // Found a starting node from current frontier.
            scanFrontier->setNodeID(boundNodeID);
            while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
                updateVisitedNodes(boundNodeID);
            }
        } else {
            // Otherwise move to the next frontier.
            bfsMorsel->finalizeCurrentLevel();
        }
    }
}

void RecursiveJoin::computeMSBFSMorsel(kuzu::processor::ExecutionContext* context) {
    auto msBFSMorsel = (reinterpret_cast<MSBFSMorsel<false>*>(bfsMorsel.get()));
    uint64_t *temp, *x = msBFSMorsel->visit, *next_ = msBFSMorsel->next;
    while (doMSBFS(msBFSMorsel->seen, x, next_, msBFSMorsel->maxOffset, context)) {
        msBFSMorsel->updateBFSLevel();
        temp = x;
        x = next_;
        next_ = temp;
    }
}

bool RecursiveJoin::doMSBFS(uint64_t* seen, uint64_t* curFrontier, uint64_t* nextFrontier,
    uint64_t maxOffset, kuzu::processor::ExecutionContext* context) {
    for (auto offset = 0u; offset < (maxOffset + 1); offset++) {
        seen[offset] |= curFrontier[offset];
        nextFrontier[offset] = 0llu;
    }
    if (bfsMorsel->isComplete()) {
        return false;
    }
    bool active = false;
    for (auto offset = 0u; offset < (maxOffset + 1); offset++) {
        if (curFrontier[offset]) {
            callMSBFSRecursivePlan(seen, curFrontier, nextFrontier, offset, active, context);
            if (bfsMorsel->isComplete())
                return false;
        }
    }
    return active;
}

void RecursiveJoin::callMSBFSRecursivePlan(const uint64_t* seen, const uint64_t* curFrontier,
    uint64_t* nextFrontier, common::offset_t parentOffset, bool& isBFSActive,
    kuzu::processor::ExecutionContext* context) {
    scanFrontier->setNodeID(common::nodeID_t{parentOffset, *begin(dataInfo->dstNodeTableIDs)});
    auto msBFSMorsel = (reinterpret_cast<MSBFSMorsel<false>*>(bfsMorsel.get()));
    while (recursiveRoot->getNextTuple(context)) {
        auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
        for (auto i = 0u; i < recursiveDstNodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
            auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
            uint64_t unseen = curFrontier[parentOffset] & ~seen[nodeID.offset];
            if (unseen) {
                nextFrontier[nodeID.offset] |= unseen;
            }
            isBFSActive |= unseen;
            auto count = 0u;
            while (unseen) {
                auto isVisited = unseen & 0xFF;
                switch (isVisited) {
                case 0: {
                    // Do nothing, none of the BFS lanes have been visited.
                } break;
                case 1: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 2: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 3: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 4: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 5: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 6: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 7: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 8: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 9: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 10: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 11: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 12: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 13: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 14: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 15: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 16: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 17: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 18: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 19: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 20: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 21: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 22: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 23: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 24: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 25: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 26: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 27: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 28: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 29: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 30: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 31: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 32: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 33: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 34: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 35: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 36: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 37: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 38: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 39: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 40: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 41: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 42: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 43: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 44: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 45: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 46: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 47: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 48: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 49: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 50: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 51: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 52: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 53: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 54: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 55: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 56: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 57: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 58: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 59: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 60: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 61: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 62: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 63: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 64: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 65: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 66: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 67: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 68: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 69: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 70: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 71: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 72: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 73: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 74: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 75: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 76: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 77: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 78: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 79: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 80: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 81: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 82: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 83: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 84: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 85: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 86: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 87: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 88: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 89: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 90: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 91: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 92: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 93: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 94: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 95: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 96: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 97: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 98: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 99: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 100: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 101: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 102: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 103: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 104: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 105: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 106: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 107: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 108: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 109: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 110: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 111: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 112: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 113: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 114: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 115: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 116: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 117: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 118: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 119: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 120: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 121: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 122: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 123: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 124: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 125: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 126: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 127: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 128: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 129: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 130: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 131: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 132: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 133: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 134: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 135: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 136: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 137: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 138: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 139: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 140: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 141: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 142: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 143: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 144: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 145: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 146: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 147: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 148: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 149: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 150: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 151: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 152: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 153: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 154: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 155: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 156: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 157: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 158: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 159: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 160: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 161: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 162: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 163: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 164: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 165: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 166: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 167: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 168: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 169: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 170: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 171: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 172: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 173: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 174: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 175: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 176: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 177: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 178: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 179: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 180: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 181: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 182: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 183: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 184: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 185: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 186: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 187: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 188: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 189: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 190: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 191: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 192: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 193: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 194: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 195: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 196: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 197: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 198: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 199: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 200: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 201: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 202: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 203: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 204: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 205: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 206: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 207: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 208: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 209: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 210: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 211: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 212: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 213: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 214: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 215: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 216: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 217: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 218: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 219: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 220: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 221: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 222: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 223: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 224: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 225: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 226: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 227: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 228: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 229: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 230: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 231: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 232: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 233: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 234: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 235: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 236: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 237: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 238: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 239: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 240: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 241: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 242: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 243: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 244: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 245: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 246: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 247: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 248: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 249: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 250: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 251: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 252: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 253: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 254: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                case 255: {
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 0] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 1] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 2] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 3] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 4] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 5] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 6] =
                        msBFSMorsel->currentLevel + 1;
                    msBFSMorsel->pathLengths[nodeID.offset][8 * count + 7] =
                        msBFSMorsel->currentLevel + 1;
                } break;
                }
                count++;
                unseen = unseen >> 8;
            }
        }
    }
}

void RecursiveJoin::updateVisitedNodes(common::nodeID_t boundNodeID) {
    auto boundNodeMultiplicity = bfsMorsel->getMultiplicity(boundNodeID);
    for (auto i = 0u; i < vectors->recursiveDstNodeIDVector->state->selVector->selectedSize; ++i) {
        auto pos = vectors->recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto edgeID = vectors->recursiveEdgeIDVector->getValue<common::relID_t>(pos);
        bfsMorsel->markVisited(boundNodeID, nbrNodeID, edgeID, boundNodeMultiplicity);
    }
}

void RecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto op = recursiveRoot.get();
    while (!op->isSource()) {
        assert(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    scanFrontier = (ScanFrontier*)op;
    localResultSet = std::make_unique<ResultSet>(
        dataInfo->localResultSetDescriptor.get(), context->memoryManager);
    vectors->recursiveDstNodeIDVector =
        localResultSet->getValueVector(dataInfo->recursiveDstNodeIDPos).get();
    vectors->recursiveEdgeIDVector =
        localResultSet->getValueVector(dataInfo->recursiveEdgeIDPos).get();
    recursiveRoot->initLocalState(localResultSet.get(), context);
}

void RecursiveJoin::populateTargetDstNodes() {
    frontier::node_id_set_t targetNodeIDs;
    uint64_t numTargetNodes = 0;
    for (auto& semiMask : sharedState->semiMasks) {
        auto nodeTable = semiMask->getNodeTable();
        auto numNodes = nodeTable->getMaxNodeOffset(transaction) + 1;
        if (semiMask->isEnabled()) {
            for (auto offset = 0u; offset < numNodes; ++offset) {
                if (semiMask->isNodeMasked(offset)) {
                    targetNodeIDs.insert(nodeID_t{offset, nodeTable->getTableID()});
                    numTargetNodes++;
                }
            }
        } else {
            assert(targetNodeIDs.empty());
            numTargetNodes += numNodes;
        }
    }
    targetDstNodes = std::make_unique<TargetDstNodes>(numTargetNodes, std::move(targetNodeIDs));
    for (auto tableID : dataInfo->recursiveDstNodeTableIDs) {
        if (!dataInfo->dstNodeTableIDs.contains(tableID)) {
            targetDstNodes->setTableIDFilter(dataInfo->dstNodeTableIDs);
            return;
        }
    }
}

} // namespace processor
} // namespace kuzu
