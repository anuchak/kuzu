#include "processor/operator/recursive_extend/ms_bfs_morsel.h"

namespace kuzu {
namespace processor {

void MSBFSMorsel::doMSBFS(ExecutionContext* context, RecursiveJoinVectors* vectors,
    ScanFrontier* scanFrontier, PhysicalOperator* recursiveRoot, common::table_id_t tableID) {
    uint64_t *visit_, *next_;
    if (currentLevel % 2) {
        visit_ = next;
        next_ = visit;
    } else {
        visit_ = visit;
        next_ = next;
    }
    if (extendCurFrontier(scanFrontier, recursiveRoot, tableID, visit_, next_, context, vectors)) {
        updateBFSLevel();
        for (auto offset = 0u; offset < (maxOffset + 1); offset++) {
            seen[offset] |= next_[offset];
            visit_[offset] = 0llu;
        }
    } else {
        isBFSComplete = true;
    }
}

bool MSBFSMorsel::extendCurFrontier(ScanFrontier* scanFrontier, PhysicalOperator* recursiveRoot,
    common::table_id_t tableID, const uint64_t* curFrontier, uint64_t* nextFrontier,
    kuzu::processor::ExecutionContext* context, RecursiveJoinVectors* vectors) {
    if (isComplete()) {
        return false;
    }
    bool active = false;
    for (auto offset = 0u; offset < (maxOffset + 1); offset++) {
        if (curFrontier[offset]) {
            auto parentNode = common::nodeID_t{offset, tableID};
            exploreNbrs(scanFrontier, recursiveRoot, curFrontier, nextFrontier, parentNode, active,
                context, vectors);
        }
    }
    return active;
}

bool MSBFSMorsel::exploreNbrs(ScanFrontier* scanFrontier, PhysicalOperator* recursiveRoot,
    const uint64_t* curFrontier, uint64_t* nextFrontier, common::nodeID_t parentNode,
    bool& isBFSActive, kuzu::processor::ExecutionContext* context,
    RecursiveJoinVectors* vectors) const {
    scanFrontier->setNodeID(parentNode);
    while (recursiveRoot->getNextTuple(context)) {
        auto recursiveDstNodeIDVector = vectors->recursiveDstNodeIDVector;
        for (auto i = 0u; i < recursiveDstNodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = recursiveDstNodeIDVector->state->selVector->selectedPositions[i];
            auto nodeID = recursiveDstNodeIDVector->getValue<common::nodeID_t>(pos);
            uint64_t unseen = curFrontier[parentNode.offset] & ~seen[nodeID.offset];
            if (unseen) {
                nextFrontier[nodeID.offset] |= unseen;
            }
            isBFSActive |= unseen;
        }
    }
}

int64_t MSBFSMorsel::writeToVector(
    common::table_id_t tableID, RecursiveJoinVectors* recursiveJoinVectors) {
    // If no sources to write destination for, then this will be 0, we return -1 to indicate this.
    if (totalSources == 0 || (dstLaneCount == totalSources)) {
        dstLaneCount = 0u;
        curSrcIdx = 0u;
        return -1;
    }
    auto srcNodeIDVector = recursiveJoinVectors->srcNodeIDVector;
    auto dstNodeIDVector = recursiveJoinVectors->dstNodeIDVector;
    auto pathLengthVector = recursiveJoinVectors->pathLengthVector;
    auto laneMask = (1llu << dstLaneCount);
    if (hasMoreToWrite()) {
        auto size = 0u;
        auto offset = lastDstOffsetWritten;
        auto curSrcPos = srcNodeIDVector->state->selVector->selectedPositions[0];
        auto srcNodeID = srcNodeIDVector->getValue<common::nodeID_t>(curSrcPos);
        while (size < common::DEFAULT_VECTOR_CAPACITY && offset < (maxOffset + 1)) {
            if (offset == srcNodeID.offset && lowerBound > 0) {
                offset++;
                continue;
            }
            /**
             * visit | next | laneMask = result
             * --------------------------------
             *    0  |   0  |     1    =    0
             *    0  |   1  |     1    =    1
             *    1  |   1  |     1    =    0
             */
            auto res = (visit[offset] ^ next[offset]) & laneMask;
            if (res) {
                dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{offset, tableID});
                pathLengthVector->setValue<int64_t>(size, currentLevel);
                size++;
            }
            offset++;
        }
        lastDstOffsetWritten = offset;
        if (offset == (maxOffset + 1)) {
            hasMoreDst = false;
            dstLaneCount++;
        }
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        return size;
    } else {
        auto size = 0u, offset = 0u;
        // Place the current source for which the destinations are being written to the ValueVector.
        srcNodeIDVector->state->selVector->resetSelectorToValuePosBufferWithSize(1);
        auto curSrcPos = srcNodeDataChunkSelectedPositions[curSrcIdx++];
        srcNodeIDVector->state->selVector->selectedPositions[0] = curSrcPos;
        auto srcNodeID = srcNodeIDVector->getValue<common::nodeID_t>(curSrcPos);
        while (size < common::DEFAULT_VECTOR_CAPACITY && offset < (maxOffset + 1)) {
            if (offset == srcNodeID.offset && lowerBound > 0) {
                offset++;
                continue;
            }
            /**
             * visit | next | laneMask = result
             * --------------------------------
             *    0  |   0  |     1    =    0
             *    0  |   1  |     1    =    1
             *    1  |   1  |     1    =    0
             */
            auto res = (visit[offset] ^ next[offset]) & laneMask;
            if (res) {
                dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{offset, tableID});
                pathLengthVector->setValue<int64_t>(size, currentLevel);
                size++;
            }
            offset++;
        }
        if (offset < (maxOffset + 1)) {
            hasMoreDst = true;
            lastDstOffsetWritten = offset;
        } else {
            dstLaneCount++;
        }
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        return size;
    }
}

} // namespace processor
} // namespace kuzu
