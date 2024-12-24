#pragma once

#include "processor/operator/recursive_extend/bfs_state.h"
#include "processor/operator/table_scan/ftable_scan_function.h"

namespace kuzu {
namespace processor {

struct MorselDispatcher {
public:
    MorselDispatcher(common::SchedulerType schedulerType, uint64_t lowerBound, uint64_t upperBound,
        uint64_t maxOffset)
        : schedulerType{schedulerType}, maxOffset{maxOffset}, upperBound{upperBound},
          lowerBound{lowerBound}, numActiveBFSSharedState{0u}, globalState{IN_PROGRESS} {}

    inline void initActiveBFSSharedState(uint32_t numThreads) {
        activeBFSSharedState = std::vector<std::shared_ptr<BFSSharedState>>(numThreads, nullptr);
    }

    std::pair<GlobalSSSPState, SSSPLocalState> getBFSMorsel(
        const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
        const std::vector<common::ValueVector*> vectorsToScan,
        const std::vector<ft_col_idx_t> colIndicesToScan, common::ValueVector* srcNodeIDVector,
        BaseBFSState* bfsMorsel, common::QueryRelType queryRelType,
        planner::RecursiveJoinType recursiveJoinType);

    static void setUpNewBFSSharedState(std::shared_ptr<BFSSharedState>& newBFSSharedState,
        BaseBFSState* bfsMorsel, FTableScanMorsel* inputFTableMorsel,
        common::nodeID_t nodeID, common::QueryRelType queryRelType,
        planner::RecursiveJoinType recursiveJoinType);

    uint32_t getNextAvailableSSSPWork() const;

    std::pair<GlobalSSSPState, SSSPLocalState> findAvailableSSSP(BaseBFSState* bfsMorsel);

    int64_t writeDstNodeIDAndPathLength(
        const std::shared_ptr<FTableScanSharedState>& inputFTableSharedState,
        std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
        common::table_id_t tableID, std::unique_ptr<kuzu::processor::BaseBFSState>& baseBfsMorsel,
        RecursiveJoinVectors* vectors);

    inline common::SchedulerType getSchedulerType() { return schedulerType; }

    inline void setSchedulerType(common::SchedulerType schedulerType_) {
        schedulerType = schedulerType_;
    }

private:
    common::SchedulerType schedulerType;
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    std::vector<std::shared_ptr<BFSSharedState>> activeBFSSharedState;
    uint32_t numActiveBFSSharedState;
    GlobalSSSPState globalState;
    std::mutex mutex;
};

} // namespace processor
} // namespace kuzu
