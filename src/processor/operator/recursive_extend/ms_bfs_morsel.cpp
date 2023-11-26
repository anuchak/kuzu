#include "processor/operator/recursive_extend/ms_bfs_morsel.h"

namespace kuzu {
namespace processor {

void MSBFSMorsel::doMSBFS(ExecutionContext* context, RecursiveJoinVectors* vectors,
    ScanFrontier* scanFrontier, PhysicalOperator* recursiveRoot, common::table_id_t tableID) {
    if(hasMoreToWrite()) {

    } else {
        uint64_t *temp, *x = visit, *next_ = next;
        bool ret = extendCurFrontier(scanFrontier, recursiveRoot, tableID, x, next_, context, vectors);
        if(!ret) {
            // No new neighbours were found, exit from doing MS-BFS.
            return;
        }
        updateBFSLevel();
        temp = x;
        x = next_;
        next_ = temp;

    }
}

bool MSBFSMorsel::extendCurFrontier(ScanFrontier* scanFrontier, PhysicalOperator* recursiveRoot,
    common::table_id_t tableID, const uint64_t* curFrontier, uint64_t* nextFrontier,
    kuzu::processor::ExecutionContext* context, RecursiveJoinVectors* vectors) {
    for (auto offset = 0u; offset < (maxOffset + 1); offset++) {
        seen[offset] |= curFrontier[offset];
        nextFrontier[offset] = 0llu;
    }
    if (isComplete()) {
        return false;
    }
    bool active = false;
    for (auto offset = 0u; offset < (maxOffset + 1); offset++) {
        if (curFrontier[offset]) {
            auto parentNode = common::nodeID_t{offset, tableID};
            exploreNbrs(scanFrontier, recursiveRoot, curFrontier, nextFrontier, parentNode, active,
                context, vectors);
            if (isComplete())
                return false;
        }
    }
    return active;
}

bool MSBFSMorsel::exploreNbrs(ScanFrontier* scanFrontier, PhysicalOperator* recursiveRoot,
    const uint64_t* curFrontier, uint64_t* nextFrontier, common::nodeID_t parentNode,
    bool& isBFSActive, kuzu::processor::ExecutionContext* context, RecursiveJoinVectors* vectors) {
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
            writePathLengths(parentNode.tableID, unseen, nodeID.offset, vectors);
        }
    }
}

bool MSBFSMorsel::writePathLengths(common::table_id_t tableID, uint64_t unseenBFSLanes,
    common::offset_t offset, RecursiveJoinVectors* recursiveJoinVectors) {
    auto count = 0u;
    auto dstNodeIDVector = recursiveJoinVectors->dstNodeIDVector;
    auto pathLengthVector = recursiveJoinVectors->pathLengthVector;
    while (unseenBFSLanes) {
        auto isVisited = unseenBFSLanes & 0xFF;
        switch (isVisited) {
        case 0: {
            // Do nothing, none of the BFS lanes have been visited.
        } break;
        case 1: {
            auto size = dstNodeIDVector->state->selVector->selectedSize;
            dstNodeIDVector->setValue<common::nodeID_t>(size, common::nodeID_t{offset, tableID});
            pathLengthVector->setValue<int64_t>(size, currentLevel + 1);
            //pathLengths[offset][8 * count + 0] = currentLevel + 1;
        } break;
        case 2: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
        } break;
        case 3: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
        } break;
        case 4: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
        } break;
        case 5: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
        } break;
        case 6: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
        } break;
        case 7: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
        } break;
        case 8: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
        } break;
        case 9: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
        } break;
        case 10: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
        } break;
        case 11: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
        } break;
        case 12: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
        } break;
        case 13: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
        } break;
        case 14: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
        } break;
        case 15: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
        } break;
        case 16: {
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 17: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 18: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 19: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 20: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 21: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 22: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 23: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 24: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 25: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 26: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 27: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 28: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 29: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 30: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 31: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
        } break;
        case 32: {
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 33: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 34: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 35: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 36: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 37: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 38: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 39: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 40: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 41: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 42: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 43: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 44: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 45: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 46: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 47: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 48: {
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 49: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 50: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 51: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 52: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 53: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 54: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 55: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 56: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 57: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 58: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 59: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 60: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 61: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 62: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 63: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
        } break;
        case 64: {
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 65: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 66: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 67: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 68: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 69: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 70: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 71: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 72: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 73: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 74: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 75: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 76: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 77: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 78: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 79: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 80: {
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 81: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 82: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 83: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 84: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 85: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 86: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 87: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 88: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 89: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 90: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 91: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 92: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 93: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 94: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 95: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 96: {
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 97: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 98: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 99: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 100: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 101: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 102: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 103: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 104: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 105: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 106: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 107: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 108: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 109: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 110: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 111: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 112: {
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 113: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 114: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 115: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 116: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 117: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 118: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 119: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 120: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 121: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 122: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 123: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 124: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 125: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 126: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 127: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
        } break;
        case 128: {
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 129: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 130: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 131: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 132: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 133: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 134: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 135: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 136: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 137: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 138: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 139: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 140: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 141: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 142: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 143: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 144: {
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 145: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 146: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 147: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 148: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 149: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 150: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 151: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 152: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 153: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 154: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 155: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 156: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 157: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 158: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 159: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 160: {
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 161: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 162: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 163: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 164: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 165: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 166: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 167: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 168: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 169: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 170: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 171: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 172: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 173: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 174: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 175: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 176: {
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 177: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 178: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 179: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 180: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 181: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 182: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 183: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 184: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 185: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 186: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 187: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 188: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 189: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 190: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 191: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 192: {
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 193: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 194: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 195: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 196: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 197: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 198: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 199: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 200: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 201: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 202: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 203: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 204: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 205: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 206: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 207: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 208: {
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 209: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 210: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 211: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 212: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 213: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 214: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 215: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 216: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 217: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 218: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 219: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 220: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 221: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 222: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 223: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 224: {
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 225: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 226: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 227: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 228: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 229: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 230: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 231: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 232: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 233: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 234: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 235: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 236: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 237: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 238: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 239: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 240: {
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 241: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 242: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 243: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 244: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 245: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 246: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 247: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 248: {
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 249: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 250: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 251: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 252: {
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 253: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 254: {
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        case 255: {
            pathLengths[offset][8 * count + 0] = currentLevel + 1;
            pathLengths[offset][8 * count + 1] = currentLevel + 1;
            pathLengths[offset][8 * count + 2] = currentLevel + 1;
            pathLengths[offset][8 * count + 3] = currentLevel + 1;
            pathLengths[offset][8 * count + 4] = currentLevel + 1;
            pathLengths[offset][8 * count + 5] = currentLevel + 1;
            pathLengths[offset][8 * count + 6] = currentLevel + 1;
            pathLengths[offset][8 * count + 7] = currentLevel + 1;
        } break;
        }
        count++;
        unseenBFSLanes = unseenBFSLanes >> 8;
    }
}

int64_t MSBFSMorsel::writeToVector(
    common::table_id_t tableID, RecursiveJoinVectors* recursiveJoinVectors) {
    // If no sources to write destination for, then this will be 0, we return -1 to indicate this.
    if (totalSources == 0 || (dstLaneCount == totalSources)) {
        return -1;
    }
    auto srcNodeIDVector = recursiveJoinVectors->srcNodeIDVector;
    auto dstNodeIDVector = recursiveJoinVectors->dstNodeIDVector;
    auto pathLengthVector = recursiveJoinVectors->pathLengthVector;
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
                pathLengthVector->setValue<int64_t>(size, pathLengths[offset][dstLaneCount]);
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
                pathLengthVector->setValue<int64_t>(size, pathLengths[offset][dstLaneCount]);
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
