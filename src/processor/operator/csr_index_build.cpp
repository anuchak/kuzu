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
    size_t totalSize = std::ceil( (double)(maxNodeOffset + 1) / MORSEL_SIZE);
    csrSharedState->csr = std::vector<CSREntry*>(totalSize, nullptr);
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
    auto& csr = csrSharedState->csr;
    uint64_t totalNodes = 0;
    uint64_t totalSizeAllocated = 0;
    while (children[0]->getNextTuple(context)) {
        auto pos = boundNodeVector->state->selVector->selectedPositions[0];
        auto boundNode = boundNodeVector->getValue<common::nodeID_t>(pos);
        auto totalNbrOffsets = nbrNodeVector->state->selVector->selectedSize;
        auto csrPos = (boundNode.offset >> RIGHT_SHIFT);
        auto &entry = csr[csrPos];
        if (!entry) {
            auto newEntry = new CSREntry();
            __atomic_store_n(&csr[csrPos], newEntry, __ATOMIC_RELAXED);
            totalSizeAllocated += (sizeof(CSREntry) + 8); // add size of struct + ptr to struct
            totalNodes += boundNodeVector->state->getOriginalSize();
            if (lastCSREntryHandled) {
                for(auto i = 1; i < (MORSEL_SIZE + 1); i++) {
                    lastCSREntryHandled->csr_v[i] += lastCSREntryHandled->csr_v[i-1];
                }
                // add size of all (edge id + rel id) in previous csr entry handled
                totalSizeAllocated += (lastCSREntryHandled->nbrNodeOffsets.capacity() * 2 * 8);
            }
            lastCSREntryHandled = entry;
        }
        for (auto i = 0u; i < totalNbrOffsets; i++) {
            pos = nbrNodeVector->state->selVector->selectedPositions[i];
            auto nbrNode = nbrNodeVector->getValue<common::nodeID_t>(pos);
            auto relID = relIDVector->getValue<common::relID_t>(pos);
            entry->nbrNodeOffsets.push_back(nbrNode.offset);
            entry->relIDOffsets.push_back(relID.offset);
        }
        entry->csr_v[(boundNode.offset & OFFSET_DIV) + 1] += totalNbrOffsets;
    }
    if (lastCSREntryHandled) {
        for(auto i = 1; i < (MORSEL_SIZE + 1); i++) {
            lastCSREntryHandled->csr_v[i] += lastCSREntryHandled->csr_v[i-1];
        }
        // add size of all (edge id + rel id) in previous csr entry handled
        totalSizeAllocated += (lastCSREntryHandled->nbrNodeOffsets.capacity() * 2 * 8);
    }
    // If this causes performance problems, switch to memory_order_acq_rel
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto duration2 = std::chrono::system_clock::now().time_since_epoch();
    auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
    double totalInMB = ((((double) totalSizeAllocated / (double) 1024) / (double) 1024));
    printf("Thread %s finished at %lu, total time taken %lu ms, total nodes %lu, total size %lf\n",
        oss.str().c_str(), millis2, (millis2 - millis1), totalNodes, totalInMB);
}

} // namespace processor
} // namespace kuzu
