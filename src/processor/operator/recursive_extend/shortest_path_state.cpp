#include "processor/operator/recursive_extend/shortest_path_state.h"

#include <snappy/snappy-stubs-public.h>

using namespace kuzu::common;

namespace kuzu {
namespace processor {

template<>
void ShortestPathState<false>::addToLocalNextBFSLevel(RecursiveJoinVectors* vectors,
    uint64_t boundNodeMultiplicity, unsigned long boundNodeOffset) {
    auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
    auto& selVector = recursiveDstNodeIDVector->state->getSelVectorUnsafe();
    for (auto i = 0u; i < selVector.getSelSize(); ++i) {
        auto pos = selVector.getSelectedPositions()[i];
        auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(&bfsSharedState->visitedNodes[nodeID.offset], state,
                    VISITED_DST)) {
                /// NOTE: This write is safe to do here without a CAS, because we have a full
                /// memory barrier once each thread merges its results (they have to hold a lock).
                /// A CAS would be required if a read had occurred before this full memory barrier
                /// - such as visitedNodes state. Those states are being written to and read from
                /// even before each thread holds the lock.
                bfsSharedState->pathLength[nodeID.offset] = bfsSharedState->currentLevel + 1;
                bfsSharedState->nextFrontier[nodeID.offset] = 1u;
                numVisitedDstNodes++;
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(&bfsSharedState->visitedNodes[nodeID.offset], state,
                VISITED)) {
                numVisitedNonDstNodes++;
                bfsSharedState->nextFrontier[nodeID.offset] = 1u;
            }
        }
    }
}

template<>
void ShortestPathState<true>::addToLocalNextBFSLevel(RecursiveJoinVectors* vectors,
    uint64_t boundNodeMultiplicity, common::offset_t boundNodeOffset) {
    auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
    auto recursiveEdgeIDVector = vectors->recursiveEdgeIDVector;
    auto& selVector = recursiveDstNodeIDVector->state->getSelVectorUnsafe();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector.getSelectedPositions()[i];
        auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(&bfsSharedState->visitedNodes[nodeID.offset], state,
                    VISITED_DST)) {
                numVisitedDstNodes++;
                auto edgeID = recursiveEdgeIDVector->getValue<common::relID_t>(pos);
                // TODO: Do we even need this ? Because once we track back the path to the source
                // we know the length of the path, so a separate vector to have the bfsLevel is not
                // needed. Remove this later if speed is slow.
                bfsSharedState->pathLength[nodeID.offset] = bfsSharedState->currentLevel + 1;
                bfsSharedState->nextFrontier[nodeID.offset] = 1u;
                bfsSharedState->srcNodeOffsetAndEdgeOffset[nodeID.offset] = {boundNodeOffset,
                    edgeID.offset};
                /// TEMP - to keep the edge table ID saved later for writing to the ValueVector
                if (bfsSharedState->edgeTableID == UINT64_MAX) {
                    bfsSharedState->edgeTableID = edgeID.tableID;
                }
            }
        } else if (state == NOT_VISITED) {
            if (__sync_bool_compare_and_swap(&bfsSharedState->visitedNodes[nodeID.offset], state,
                    VISITED)) {
                auto edgeID = recursiveEdgeIDVector->getValue<common::relID_t>(pos);
                bfsSharedState->srcNodeOffsetAndEdgeOffset[nodeID.offset] = {boundNodeOffset,
                    edgeID.offset};
                bfsSharedState->nextFrontier[nodeID.offset] = 1u;
                numVisitedNonDstNodes++;
            }
        }
    }
}

template<>
int64_t ShortestPathState<false>::writeToVector(
    const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
    RecursiveJoinVectors* vectors) {
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    auto dstNodeIDVector = vectors->dstNodeIDVector;
    auto pathLengthVector = vectors->pathLengthVector;
    while (startScanIdxAndSize.first < endIdx) {
        if (bfsSharedState->pathLength[startScanIdxAndSize.first] >= bfsSharedState->lowerBound) {
            dstNodeIDVector->setValue<common::nodeID_t>(size,
                common::nodeID_t{startScanIdxAndSize.first, tableID});
            pathLengthVector->setValue<int64_t>(size,
                bfsSharedState->pathLength[startScanIdxAndSize.first]);
            size++;
        }
        startScanIdxAndSize.first++;
    }
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->table->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        if (!vectorsToScan[0]->state->isFlat()) {
            vectorsToScan[0]->state->setToFlat();
        }
        return size;
    }
    return 0;
}

template<>
int64_t ShortestPathState<true>::writeToVector(
    const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize,
    RecursiveJoinVectors* vectors) {
    auto size = 0u, nodeIDDataVectorPos = 0u, relIDDataVectorPos = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    if (vectors->pathVector != nullptr) {
        vectors->pathVector->resetAuxiliaryBuffer();
    }
    uint8_t pathLength;
    auto nodeBuffer = std::vector<common::offset_t>(31u);
    auto relBuffer = std::vector<common::offset_t>(31u);
    std::string nodeLabelName, relLabelName;
    nodeLabelName = tableIDToName.at(tableID);
    if (bfsSharedState->edgeTableID != UINT64_MAX) {
        relLabelName = tableIDToName.at(bfsSharedState->edgeTableID);
    }
    while (startScanIdxAndSize.first < endIdx) {
        if (bfsSharedState->pathLength[startScanIdxAndSize.first] >= bfsSharedState->lowerBound) {
            pathLength = bfsSharedState->pathLength[startScanIdxAndSize.first];
            auto nodeEntry = common::ListVector::addList(vectors->pathNodesVector, pathLength - 1);
            auto relEntry = common::ListVector::addList(vectors->pathRelsVector, pathLength);
            vectors->pathNodesVector->setValue(size, nodeEntry);
            vectors->pathRelsVector->setValue(size, relEntry);
            vectors->dstNodeIDVector->setValue<common::nodeID_t>(size,
                common::nodeID_t{startScanIdxAndSize.first, tableID});
            vectors->pathLengthVector->setValue<int64_t>(size, pathLength);
            size++;
            auto entry = bfsSharedState->srcNodeOffsetAndEdgeOffset[startScanIdxAndSize.first];
            auto idx = 0u;
            nodeBuffer[pathLength - idx] = startScanIdxAndSize.first;
            while (entry.first != UINT64_MAX && entry.second != UINT64_MAX) {
                relBuffer[pathLength - idx - 1] = entry.second;
                nodeBuffer[pathLength - ++idx] = entry.first;
                entry = bfsSharedState->srcNodeOffsetAndEdgeOffset[entry.first];
            }
            for (auto i = 1u; i < pathLength; i++) {
                vectors->pathNodesIDDataVector->setValue<common::nodeID_t>(nodeIDDataVectorPos,
                    common::nodeID_t{nodeBuffer[i], tableID});
                common::StringVector::addString(vectors->pathNodesLabelDataVector,
                    nodeIDDataVectorPos++, nodeLabelName.data(), nodeLabelName.length());
            }
            for (auto i = 0u; i < pathLength; i++) {
                vectors->pathRelsSrcIDDataVector->setValue<common::nodeID_t>(relIDDataVectorPos,
                    common::nodeID_t{nodeBuffer[i], tableID});
                vectors->pathRelsIDDataVector->setValue<common::relID_t>(relIDDataVectorPos,
                    common::relID_t{relBuffer[i], bfsSharedState->edgeTableID});
                StringVector::addString(vectors->pathRelsLabelDataVector, relIDDataVectorPos,
                    relLabelName.data(), relLabelName.length());
                vectors->pathRelsDstIDDataVector->setValue<common::nodeID_t>(relIDDataVectorPos++,
                    common::nodeID_t{nodeBuffer[i + 1], tableID});
            }
        }
        startScanIdxAndSize.first++;
    }
    if (size > 0) {
        vectors->dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->table->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        if (!vectorsToScan[0]->state->isFlat()) {
            vectorsToScan[0]->state->setToFlat();
        }
        return size;
    }
    return 0;
}

} // namespace processor
} // namespace kuzu
