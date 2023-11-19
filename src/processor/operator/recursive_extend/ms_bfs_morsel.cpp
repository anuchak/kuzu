#include "processor/operator/recursive_extend/ms_bfs_morsel.h"

namespace kuzu {
namespace processor {

template<>
int64_t MSBFSMorsel<false>::writeToVector(
    common::table_id_t tableID, RecursiveJoinVectors* recursiveJoinVectors) {
    // If no sources to write destination for, then this will be 0, we return -1 to indicate this.
    if (totalSources == 0 || (dstLaneCount == totalSources)) {
        return -1;
    }
    auto srcNodeIDVector = recursiveJoinVectors->srcNodeIDVector;
    auto dstNodeIDVector = recursiveJoinVectors->dstNodeIDVector;
    auto laneMask = (1llu << dstLaneCount);
    if (hasMoreToWrite()) {
        auto size = 0u;
        auto offset = lastDstOffsetWritten;
        auto srcOffset = srcNodeIDVector
                             ->getValue<common::nodeID_t>(
                                 srcNodeIDVector->state->selVector->selectedPositions[0])
                             .offset;
        while (size < common::DEFAULT_VECTOR_CAPACITY && offset < (maxOffset + 1)) {
            if (offset == srcOffset && lowerBound > 0) {
                offset++;
                continue;
            }
            if (laneMask & seen[offset]) {
                dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{offset, tableID});
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
        srcNodeIDVector->state->selVector->selectedPositions[0] =
            srcNodeDataChunkSelectedPositions[curSrcIdx++];
        auto srcOffset = srcNodeIDVector
                             ->getValue<common::nodeID_t>(
                                 srcNodeIDVector->state->selVector->selectedPositions[0])
                             .offset;
        while (size < common::DEFAULT_VECTOR_CAPACITY && offset < (maxOffset + 1)) {
            if (offset == srcOffset && lowerBound > 0) {
                offset++;
                continue;
            }
            if (laneMask & seen[offset]) {
                dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{offset, tableID});
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

template<>
int64_t MSBFSMorsel<true>::writeToVector(
    common::table_id_t tableID, RecursiveJoinVectors* recursiveJoinVectors) {
    throw common::NotImplementedException("This function is not supported for MSBFSMorsel and "
                                          "TRACK_PATH as true case");
}

} // namespace processor
} // namespace kuzu
