#include "processor/operator/recursive_extend/recursive_join.h"

#include "processor/operator/recursive_extend/all_shortest_path_state.h"
#include "processor/operator/recursive_extend/shortest_path_state.h"
#include "processor/operator/recursive_extend/variable_length_state.h"
#include "processor/operator/scan/offset_scan_node_table.h"
#include <processor/operator/scan/scan_rel_table.h>

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::vector<NodeSemiMask*> RecursiveJoin::getSemiMask() const {
    std::vector<NodeSemiMask*> result;
    for (auto& mask : sharedState->semiMasks) {
        result.push_back(mask.get());
    }
    return result;
}

// User's setting of scheduler policy is given preference. If user changes policy to 1T1S then
// before operator begins execution we change the policy globally here.
void RecursiveJoin::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    auto schedulerPolicy = context->clientContext->getClientConfigUnsafe()->bfsSchedulerType;
    if (schedulerPolicy == OneThreadOneMorsel) {
        sharedState->morselDispatcher->setSchedulerType(OneThreadOneMorsel);
        return;
    }
    if (sharedState->morselDispatcher->getSchedulerType() == nThreadkMorsel ||
        sharedState->morselDispatcher->getSchedulerType() == nThreadkMorselAdaptive) {
        auto maxConcurrentBFS = context->clientContext->getClientConfigUnsafe()->maxConcurrentBFS;
        printf("Setting max active bfs at any given point to: %lu\n", maxConcurrentBFS);
        sharedState->morselDispatcher->initActiveBFSSharedState(maxConcurrentBFS);
    }
}

void RecursiveJoin::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    for (auto& dataPos : vectorsToScanPos) {
        vectorsToScan.push_back(resultSet->getValueVector(dataPos).get());
    }
    auto& dataInfo = info.dataInfo;
    populateTargetDstNodes(context);
    vectors = std::make_unique<RecursiveJoinVectors>();
    vectors->srcNodeIDVector = resultSet->getValueVector(dataInfo.srcNodePos).get();
    vectors->dstNodeIDVector = resultSet->getValueVector(dataInfo.dstNodePos).get();
    vectors->pathLengthVector = resultSet->getValueVector(dataInfo.pathLengthPos).get();
    auto semantic = context->clientContext->getClientConfig()->recursivePatternSemantic;
    path_semantic_check_t semanticCheck = nullptr;
    std::vector<std::unique_ptr<BaseFrontierScanner>> scanners;
    auto joinType = info.joinType;
    auto lowerBound = info.lowerBound;
    auto upperBound = info.upperBound;
    auto extendInBWD = info.direction == ExtendDirection::BWD;
    auto bfsMorselSize = context->clientContext->getClientConfig()->recursiveJoinBFSMorselSize;
    switch (info.queryRelType) {
    case QueryRelType::VARIABLE_LENGTH: {
        switch (info.joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            switch (semantic) {
            case PathSemantic::TRIAL: {
                semanticCheck = PathScanner::trailSemanticCheck;
            } break;
            case PathSemantic::ACYCLIC: {
                semanticCheck = PathScanner::acyclicSemanticCheck;
            } break;
            default:
                semanticCheck = nullptr;
            }
            vectors->pathVector = resultSet->getValueVector(dataInfo.pathPos).get();
            bfsState = std::make_unique<VariableLengthState<true /* TRACK_PATH */>>(upperBound,
                lowerBound, targetDstNodes.get(), bfsMorselSize, dataInfo.tableIDToName);
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i,
                    dataInfo.tableIDToName, semanticCheck, extendInBWD));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            if (semantic != PathSemantic::WALK) {
                throw RuntimeException("Different path semantics for COUNT(*) optimization is not "
                                       "implemented. Try WALK semantic.");
            }
            bfsState = std::make_unique<VariableLengthState<false /* TRACK_PATH */>>(upperBound,
                lowerBound, targetDstNodes.get(), bfsMorselSize, dataInfo.tableIDToName);
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    case QueryRelType::SHORTEST: {
        if (semantic != PathSemantic::WALK) {
            throw RuntimeException("Different path semantics for shortest path is not implemented. "
                                   "Try WALK semantic.");
        }
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo.pathPos).get();
            bfsState = std::make_unique<ShortestPathState<true /* TRACK_PATH */>>(upperBound,
                lowerBound, targetDstNodes.get(), bfsMorselSize, dataInfo.tableIDToName);
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i,
                    dataInfo.tableIDToName, nullptr, extendInBWD));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsState = std::make_unique<ShortestPathState<false /* TRACK_PATH */>>(upperBound,
                lowerBound, targetDstNodes.get(), bfsMorselSize, dataInfo.tableIDToName);
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    case QueryRelType::ALL_SHORTEST: {
        if (semantic != PathSemantic::WALK) {
            throw RuntimeException("Different path semantics for all shortest path is not "
                                   "implemented. Try WALK semantic.");
        }
        switch (joinType) {
        case planner::RecursiveJoinType::TRACK_PATH: {
            vectors->pathVector = resultSet->getValueVector(dataInfo.pathPos).get();
            bfsState = std::make_unique<AllShortestPathState<true /* TRACK_PATH */>>(upperBound,
                lowerBound, targetDstNodes.get(), bfsMorselSize, dataInfo.tableIDToName);
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(std::make_unique<PathScanner>(targetDstNodes.get(), i,
                    dataInfo.tableIDToName, nullptr, extendInBWD));
            }
        } break;
        case planner::RecursiveJoinType::TRACK_NONE: {
            bfsState = std::make_unique<AllShortestPathState<false /* TRACK_PATH */>>(upperBound,
                lowerBound, targetDstNodes.get(), bfsMorselSize, dataInfo.tableIDToName);
            for (auto i = lowerBound; i <= upperBound; ++i) {
                scanners.push_back(
                    std::make_unique<DstNodeWithMultiplicityScanner>(targetDstNodes.get(), i));
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (vectors->pathVector != nullptr) {
        auto pathNodesFieldIdx =
            StructType::getFieldIdx(vectors->pathVector->dataType, InternalKeyword::NODES);
        vectors->pathNodesVector =
            StructVector::getFieldVector(vectors->pathVector, pathNodesFieldIdx).get();
        auto pathNodesDataVector = ListVector::getDataVector(vectors->pathNodesVector);
        auto pathNodesIDFieldIdx =
            StructType::getFieldIdx(pathNodesDataVector->dataType, InternalKeyword::ID);
        vectors->pathNodesIDDataVector =
            StructVector::getFieldVector(pathNodesDataVector, pathNodesIDFieldIdx).get();
        auto pathNodesLabelFieldIdx =
            StructType::getFieldIdx(pathNodesDataVector->dataType, InternalKeyword::LABEL);
        vectors->pathNodesLabelDataVector =
            StructVector::getFieldVector(pathNodesDataVector, pathNodesLabelFieldIdx).get();
        auto pathRelsFieldIdx =
            StructType::getFieldIdx(vectors->pathVector->dataType, InternalKeyword::RELS);
        vectors->pathRelsVector =
            StructVector::getFieldVector(vectors->pathVector, pathRelsFieldIdx).get();
        auto pathRelsDataVector = ListVector::getDataVector(vectors->pathRelsVector);
        auto pathRelsSrcIDFieldIdx =
            StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::SRC);
        vectors->pathRelsSrcIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsSrcIDFieldIdx).get();
        auto pathRelsDstIDFieldIdx =
            StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::DST);
        vectors->pathRelsDstIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsDstIDFieldIdx).get();
        auto pathRelsIDFieldIdx =
            StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::ID);
        vectors->pathRelsIDDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsIDFieldIdx).get();
        auto pathRelsLabelFieldIdx =
            StructType::getFieldIdx(pathRelsDataVector->dataType, InternalKeyword::LABEL);
        vectors->pathRelsLabelDataVector =
            StructVector::getFieldVector(pathRelsDataVector, pathRelsLabelFieldIdx).get();
    }
    frontiersScanner = std::make_unique<FrontiersScanner>(std::move(scanners));
    nbrScanState = std::make_unique<graph::NbrScanState>(context->clientContext->getMemoryManager(),
        bfsState->getRecursiveJoinType(), ExtendDirectionUtil::getRelDataDirection(info.direction));
    initLocalRecursivePlan(context);
}

bool RecursiveJoin::getNextTuplesInternal(ExecutionContext* context) {
    if (targetDstNodes->getNumNodes() == 0) {
        return false;
    }
    while (true) {
        if (scanOutput()) { // Phase 2
            return true;
        }
        if (!computeBFS(context)) {
            return false;
        }
        frontiersScanner->resetState(*bfsState);
    }
}

bool RecursiveJoin::scanOutput() {
    if (sharedState->getSchedulerType() == SchedulerType::OneThreadOneMorsel) {
        sel_t offsetVectorSize = 0u;
        sel_t nodeIDDataVectorSize = 0u;
        sel_t relIDDataVectorSize = 0u;
        if (vectors->pathVector != nullptr) {
            vectors->pathVector->resetAuxiliaryBuffer();
        }
        frontiersScanner->scan(vectors.get(), offsetVectorSize, nodeIDDataVectorSize,
            relIDDataVectorSize);
        if (offsetVectorSize == 0) {
            return false;
        }
        vectors->dstNodeIDVector->state->initOriginalAndSelectedSize(offsetVectorSize);
        return true;
    } else {
        while (true) {
            if (!bfsState->hasBFSSharedState()) {
                return false;
            }
            auto tableID = *std::begin(info.dataInfo.dstNodeTableIDs);
            auto ret = sharedState->writeDstNodeIDAndPathLength(vectorsToScan, colIndicesToScan,
                tableID, bfsState, vectors.get());
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

bool RecursiveJoin::computeBFS(kuzu::processor::ExecutionContext* context) {
    if (sharedState->getSchedulerType() == SchedulerType::OneThreadOneMorsel) {
        auto state = sharedState->getBFSMorsel(vectorsToScan, colIndicesToScan,
            vectors->srcNodeIDVector, bfsState.get(), info.queryRelType, info.joinType);
        if (state.first == COMPLETE) {
            return false;
        }
        computeBFSOneThreadOneMorsel(context);
        return true;
    }
    if (sharedState->getSchedulerType() == SchedulerType::nThreadkMorsel) {
        return doBFSnThreadkMorsel(context);
    }
    return doBFSnThreadkMorselAdaptive(context);
}

bool RecursiveJoin::doBFSnThreadkMorsel(kuzu::processor::ExecutionContext* context) {
    while (true) {
        if (bfsState->hasBFSSharedState()) {
            auto state = bfsState->getBFSMorsel();
            if (state == EXTEND_IN_PROGRESS) {
                computeBFSnThreadkMorsel(context);
                if (bfsState->finishBFSMorsel(info.queryRelType)) {
                    return true;
                }
                continue;
            }
            if (state == PATH_LENGTH_WRITE_IN_PROGRESS) {
                return true;
            }
        }
        // printf("Thread came here, bfs shared state is: %d \n", bfsState->hasBFSSharedState());
        auto state = sharedState->getBFSMorsel(vectorsToScan, colIndicesToScan,
            vectors->srcNodeIDVector, bfsState.get(), info.queryRelType, info.joinType);
        if (state.first == COMPLETE) {
            return false;
        }
        if (state.second == PATH_LENGTH_WRITE_IN_PROGRESS) {
            return true;
        }
        if (state.second == EXTEND_IN_PROGRESS) {
            // printf("got work from central coordinator, working ...\n");
            computeBFSnThreadkMorsel(context);
            if (bfsState->finishBFSMorsel(info.queryRelType)) {
                return true;
            }
        } else {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            printf("going to sleep again at time: %lu ms, failed to get work ...\n", millis);*/
            std::this_thread::sleep_for(
                std::chrono::microseconds(common::THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
            if (context->clientContext->interrupted()) {
                throw common::RuntimeException("Encountered Interrupt exception ...\n ");
            }
        }
    }
}

bool RecursiveJoin::doBFSnThreadkMorselAdaptive(kuzu::processor::ExecutionContext* context) {
    while (true) {
        // printf("Thread came here, bfs shared state is: %d \n", bfsState->hasBFSSharedState());
        auto state = sharedState->getBFSMorsel(vectorsToScan, colIndicesToScan,
            vectors->srcNodeIDVector, bfsState.get(), info.queryRelType, info.joinType);
        if (state.first == COMPLETE) {
            return false;
        }
        if (state.second == PATH_LENGTH_WRITE_IN_PROGRESS) {
            return true;
        }
        if (state.second == EXTEND_IN_PROGRESS) {
            // printf("got work from central coordinator, working ...\n");
            computeBFSnThreadkMorsel(context);
            if (bfsState->finishBFSMorsel(info.queryRelType)) {
                return true;
            }
        } else {
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            printf("going to sleep again at time: %lu ms, failed to get work ...\n", millis);*/
            std::this_thread::sleep_for(
                std::chrono::microseconds(common::THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
            if (context->clientContext->interrupted()) {
                throw common::RuntimeException("Encountered Interrupt exception ...\n ");
            }
        }
    }
}

void RecursiveJoin::computeBFSnThreadkMorsel(ExecutionContext* context) {
    // Cast the BaseBFSMorsel to ShortestPathMorsel, the TRACK_NONE RecursiveJoin is the case it is
    // applicable for. If true, indicates TRACK_PATH is true else TRACK_PATH is false.
    common::offset_t nodeOffset = bfsState->getNextNodeOffset();
    while (nodeOffset != common::INVALID_OFFSET) {
        uint64_t boundNodeMultiplicity = bfsState->getBoundNodeMultiplicity(nodeOffset);
        sharedState->diskGraph->initializeStateFwdNbrs(nodeOffset, nbrScanState.get());
        do {
            sharedState->diskGraph->getFwdNbrs(nbrScanState.get());
            bfsState->addToLocalNextBFSLevel(vectors.get(), boundNodeMultiplicity, nodeOffset);
        } while (sharedState->diskGraph->hasMoreFwdNbrs(nbrScanState.get()));
        nodeOffset = bfsState->getNextNodeOffset();
    }
}

void RecursiveJoin::computeBFSOneThreadOneMorsel(ExecutionContext* context) {
    auto nodeID = vectors->srcNodeIDVector->getValue<nodeID_t>(
        vectors->srcNodeIDVector->state->getSelVector()[0]);
    bfsState->markSrc(nodeID);
    vectors->recursiveNodePredicateExecFlagVector->setValue<bool>(0, true);
    while (!bfsState->isComplete()) {
        auto boundNodeID = bfsState->getNextNodeID();
        if (boundNodeID.offset != INVALID_OFFSET) {
            // Found a starting node from current frontier.
            recursiveSource->init(boundNodeID);
            while (recursiveRoot->getNextTuple(context)) { // Exhaust recursive plan.
                updateVisitedNodes(boundNodeID);
            }
        } else {
            // Otherwise move to the next frontier.
            bfsState->finalizeCurrentLevel();
            vectors->recursiveNodePredicateExecFlagVector->setValue<bool>(0, false);
        }
    }
}

void RecursiveJoin::updateVisitedNodes(nodeID_t boundNodeID) {
    auto boundNodeMultiplicity = bfsState->getMultiplicity(boundNodeID);
    auto& selVector = vectors->recursiveDstNodeIDVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); ++i) {
        auto pos = selVector[i];
        auto nbrNodeID = vectors->recursiveDstNodeIDVector->getValue<nodeID_t>(pos);
        auto edgeID = vectors->recursiveEdgeIDVector->getValue<relID_t>(pos);
        bfsState->markVisited(boundNodeID, nbrNodeID, edgeID, boundNodeMultiplicity);
    }
}

static PhysicalOperator* getSource(PhysicalOperator* op) {
    while (op->getNumChildren() != 0) {
        KU_ASSERT(op->getNumChildren() == 1);
        op = op->getChild(0);
    }
    return op;
}

void RecursiveJoin::initLocalRecursivePlan(ExecutionContext* context) {
    auto& dataInfo = info.dataInfo;
    localResultSet = std::make_unique<ResultSet>(dataInfo.localResultSetDescriptor.get(),
        context->clientContext->getMemoryManager());
    vectors->recursiveDstNodeIDVector = nbrScanState->dstNodeIDVector.get();
    vectors->recursiveEdgeIDVector = nbrScanState->relIDVector.get();
    /*auto isTrackPath = bfsState->getRecursiveJoinType();
    if (!isTrackPath) {
        auto temp = recursiveRoot.get();
        while (temp->getOperatorType() != PhysicalOperatorType::SCAN_REL_TABLE) {
            temp = recursiveRoot->getChild(0);
        }
        auto *scanRelTable = dynamic_cast<ScanRelTable *>(temp);
        scanRelTable->relInfo.columnIDs.clear();
    }
    recursiveRoot->initLocalState(localResultSet.get(), context);
    recursiveSource = getSource(recursiveRoot.get())->ptrCast<OffsetScanNodeTable>();*/
}

void RecursiveJoin::populateTargetDstNodes(ExecutionContext*) {
    node_id_set_t targetNodeIDs;
    uint64_t numTargetNodes = 0;
    for (auto& mask : sharedState->semiMasks) {
        auto numNodes = mask->getMaxOffset() + 1;
        if (mask->isEnabled()) {
            for (auto offset = 0u; offset < numNodes; ++offset) {
                if (mask->isMasked(offset)) {
                    targetNodeIDs.insert(nodeID_t{offset, mask->getTableID()});
                    numTargetNodes++;
                }
            }
        } else {
            KU_ASSERT(targetNodeIDs.empty());
            numTargetNodes += numNodes;
        }
    }
    targetDstNodes = std::make_unique<TargetDstNodes>(numTargetNodes, std::move(targetNodeIDs));
    auto& dataInfo = info.dataInfo;
    for (auto tableID : dataInfo.recursiveDstNodeTableIDs) {
        if (!dataInfo.dstNodeTableIDs.contains(tableID)) {
            targetDstNodes->setTableIDFilter(dataInfo.dstNodeTableIDs);
            return;
        }
    }
}

} // namespace processor
} // namespace kuzu
