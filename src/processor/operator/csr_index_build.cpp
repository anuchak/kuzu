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
    csr_v = std::vector<csrEntry*>(maxNodeOffset + 1, nullptr);
    // initialisation of common node table ID not done
    // initialisation of common rel table ID not done
}

void CSRIndexBuild::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    boundNodeVector = resultSet->getValueVector(boundNodeVectorPos).get();
    nbrNodeVector = resultSet->getValueVector(nbrNodeVectorPos).get();
    relIDVector = resultSet->getValueVector(relIDVectorPos).get();
}

void CSRIndexBuild::executeInternal(kuzu::processor::ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        auto pos = boundNodeVector->state->selVector->selectedPositions[0];
        auto boundNode = boundNodeVector->getValue<common::nodeID_t>(pos);
        auto totalNbrOffsets = nbrNodeVector->state->selVector->selectedSize;
        if (!csr_v[boundNode.offset]) {
            csr_v[boundNode.offset] = new csrEntry(totalNbrOffsets);
        } else {
            auto newCSREntry = new csrEntry(totalNbrOffsets);
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
}

} // namespace processor
} // namespace kuzu
