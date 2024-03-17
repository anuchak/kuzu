#include "processor/operator/csr_index_build.h"

#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

void CSRIndexBuild::initGlobalStateInternal(kuzu::processor::ExecutionContext* context) {
    auto child = children[0].get();
    while (child && !child->isSource()) {
        child = child->getChild(0);
    }
    auto scanNodeID = (ScanNodeID*)child;
    auto nodeTable = scanNodeID->getSharedState()->getTableState(0)->getTable();
    auto maxNodeOffset = nodeTable->getMaxNodeOffset(context->transaction);
    csrSharedState->csr_v = std::vector<csrEntry*>(maxNodeOffset + 1, nullptr);
}

void CSRIndexBuild::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    boundNodeVector = resultSet->getValueVector(boundNodeVectorPos).get();
    nbrNodeVector = resultSet->getValueVector(nbrNodeVectorPos).get();
    relIDVector = resultSet->getValueVector(relIDVectorPos).get();
}

void CSRIndexBuild::executeInternal(kuzu::processor::ExecutionContext* context) {
    auto duration1 = std::chrono::system_clock::now().time_since_epoch();
    auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    printf("Thread %s starting at %ld\n", oss.str().c_str(), millis1);
    auto& csr_v = csrSharedState->csr_v;
    uint64_t totalNodes = 0;
    uint64_t totalSizeAllocated = 0;
    while (children[0]->getNextTuple(context)) {
        auto pos = boundNodeVector->state->selVector->selectedPositions[0];
        auto boundNode = boundNodeVector->getValue<common::nodeID_t>(pos);
        auto totalNbrOffsets = nbrNodeVector->state->selVector->selectedSize;
        if (!csr_v[boundNode.offset]) {
            totalNodes++;
            csr_v[boundNode.offset] = new csrEntry(totalNbrOffsets);
            totalSizeAllocated +=
                (sizeof(csrEntry) + 2 * totalNbrOffsets * sizeof(common::offset_t));
        } else {
            auto newCSREntry = new csrEntry(totalNbrOffsets);
            totalSizeAllocated +=
                (sizeof(csrEntry) + 2 * totalNbrOffsets * sizeof(common::offset_t));
            newCSREntry->next = csr_v[boundNode.offset];
            __atomic_store_n(&csr_v[boundNode.offset], newCSREntry, __ATOMIC_RELAXED);
        }
        for (auto i = 0u; i < totalNbrOffsets; i++) {
            pos = nbrNodeVector->state->selVector->selectedPositions[i];
            auto nbrNode = nbrNodeVector->getValue<common::nodeID_t>(pos);
            auto relID = relIDVector->getValue<common::relID_t>(pos);
            csr_v[boundNode.offset]->nbrNodeOffsets[i] = nbrNode.offset;
            csr_v[boundNode.offset]->relIDOffsets[i] = relID.offset;
        }
    }
    // If this causes performance problems, switch to memory_order_acq_rel
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto duration2 = std::chrono::system_clock::now().time_since_epoch();
    auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
    double totalInMB = (((totalSizeAllocated / 1024) / 1024));
    printf("Thread %s finished at %lu, total time taken %lu ms, total nodes %lu, total size %lf\n",
        oss.str().c_str(), millis2, (millis2 - millis1), totalNodes, totalInMB);
}

} // namespace processor
} // namespace kuzu
