#include "processor/operator/recursive_extend/ms_bfs_morsel.h"

namespace kuzu {
namespace processor {

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
    uint64_t* curFrontier;
    if (currentLevel % 2) {
        curFrontier = next;
    } else {
        curFrontier = visit;
    }
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
            auto res = curFrontier[offset] & laneMask;
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
        // TODO (Anurag): This loop can be vectorized (SIMD). Set up an outer loop and inner loop
        // TODO: of 2048 and inside the loop use SIMD.
        while (size < common::DEFAULT_VECTOR_CAPACITY && offset < (maxOffset + 1)) {
            if (offset == srcNodeID.offset && lowerBound > 0) {
                offset++;
                continue;
            }
            auto res = curFrontier[offset] & laneMask;
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
