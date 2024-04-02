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
    csrSharedState->csr = std::vector<MorselCSR*>(totalSize, nullptr);
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
    int totalResize = 0;
    int totalNbrsHandled = 0;
    // use this to determine when to sum up the csr_v vector
    // basically we keep summing at (offset + 1) position the total no. of neighbours
    // at the end we need to sum from position 1 to 65 to update neighbour ranges
    MorselCSR* lastMorselCSRHandled;
    // track how much of the block has been used to write neighbour offsets
    // when blockSize < (currBlockSizeUsed + next value vector size)
    uint64_t currBlockSizeUsed;
    while (children[0]->getNextTuple(context)) {
        auto pos = boundNodeVector->state->selVector->selectedPositions[0];
        auto boundNode = boundNodeVector->getValue<common::nodeID_t>(pos);
        auto totalNbrOffsets = nbrNodeVector->state->selVector->selectedSize;
        auto csrPos = (boundNode.offset >> RIGHT_SHIFT);
        auto & morselCSR = csr[csrPos];
        // If encountering a new group for the 1st time, create a new CSR Entry.
        if (!morselCSR) {
            auto newEntry = new MorselCSR();
            __atomic_store_n(&csr[csrPos], newEntry, __ATOMIC_RELAXED);
            // Sum up the cs_v array to get the range of neighbours for each offset.
            if (lastMorselCSRHandled) {
                for(auto i = 1; i < (MORSEL_SIZE + 1); i++) {
                    lastMorselCSRHandled->csr_v[i] += lastMorselCSRHandled->csr_v[i-1];
                }
            }
            lastMorselCSRHandled = morselCSR;
            currBlockSizeUsed = 0u;
        }
        // Resizing logic
        if (morselCSR->blockSize < (currBlockSizeUsed + totalNbrOffsets)) {
            morselCSR->resize();
            totalResize++;
        }
        for (auto i = 0u; i < totalNbrOffsets; i++) {
            pos = nbrNodeVector->state->selVector->selectedPositions[i];
            auto nbrNode = nbrNodeVector->getValue<common::nodeID_t>(pos);
            auto relID = relIDVector->getValue<common::relID_t>(pos);
            morselCSR->nbrNodeOffsets[currBlockSizeUsed] = nbrNode.offset;
            morselCSR->relIDOffsets[currBlockSizeUsed] = relID.offset;
            currBlockSizeUsed++;
        }
        morselCSR->csr_v[(boundNode.offset & OFFSET_DIV) + 1] += totalNbrOffsets;
        totalNbrsHandled += totalNbrOffsets;
    }
    if (lastMorselCSRHandled) {
        for(auto i = 1; i < (MORSEL_SIZE + 1); i++) {
            lastMorselCSRHandled->csr_v[i] += lastMorselCSRHandled->csr_v[i-1];
        }
    }
    // If this causes performance problems, switch to memory_order_acq_rel
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto duration2 = std::chrono::system_clock::now().time_since_epoch();
    auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
    printf("Thread %s finished at %lu, total time taken %lu ms, no. of resize requests %d,"
           " total neighbours: %d \n", oss.str().c_str(), millis2, (millis2 - millis1), totalResize,
        totalNbrsHandled);
}

} // namespace processor
} // namespace kuzu
