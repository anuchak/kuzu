#include "processor/operator/scan/scan_node_table.h"

#include "storage/local_storage/local_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ScanNodeTableSharedState::initialize(transaction::Transaction* transaction, NodeTable* table) {
    this->table = table;
    this->currentCommittedGroupIdx = 0;
    this->nextCommittedGroupIdx = 0;
    this->currentNodeGroupVectorIdx = 0u;
    this->totalNodeGroupVectors = 0u;
    this->currentUnCommittedGroupIdx = 0;
    this->numCommittedNodeGroups = table->getNumCommittedNodeGroups();
    if (transaction->isWriteTransaction()) {
        if (const auto localTable = transaction->getLocalStorage()->getLocalTable(
                this->table->getTableID(), LocalStorage::NotExistAction::RETURN_NULL)) {
            localNodeGroups = ku_dynamic_cast<LocalTable*, LocalNodeTable*>(localTable)
                                  ->getTableData()
                                  ->getNodeGroups();
        }
    }
}

void ScanNodeTableSharedState::nextMorsel(transaction::Transaction* txn,
    NodeTableScanState& scanState) {
    std::unique_lock lck{mtx};
    if (currentCommittedGroupIdx < numCommittedNodeGroups) {
        auto& nodeDataScanState = scanState.dataScanState->cast<NodeDataScanState>();
        if (currentNodeGroupVectorIdx < totalNodeGroupVectors) {
            if (scanState.nodeGroupIdx != currentCommittedGroupIdx) {
                scanState.nodeGroupIdx = currentCommittedGroupIdx;
                scanState.source = TableScanSource::COMMITTED;
                table->initializeScanState(txn, scanState);
            }
            nodeDataScanState.vectorIdx = currentNodeGroupVectorIdx;
            auto startOffsetInNodeGroup = currentNodeGroupVectorIdx * DEFAULT_VECTOR_CAPACITY;
            nodeDataScanState.numRowsToScan = std::min(common::DEFAULT_VECTOR_CAPACITY,
                nodeDataScanState.numRowsInNodeGroup - startOffsetInNodeGroup);
            currentNodeGroupVectorIdx++;
            return;
        }
        if (nextCommittedGroupIdx == numCommittedNodeGroups) {
            scanState.source = TableScanSource::NONE;
            return;
        }
        currentCommittedGroupIdx = nextCommittedGroupIdx;
        nextCommittedGroupIdx++;
        scanState.nodeGroupIdx = currentCommittedGroupIdx;
        scanState.source = TableScanSource::COMMITTED;
        table->initializeScanState(txn, scanState);
        totalNodeGroupVectors = std::ceil(
            (double)nodeDataScanState.numRowsInNodeGroup / common::DEFAULT_VECTOR_CAPACITY);
        currentNodeGroupVectorIdx = 0u;
        auto startOffsetInNodeGroup = currentNodeGroupVectorIdx * DEFAULT_VECTOR_CAPACITY;
        nodeDataScanState.numRowsToScan = std::min(common::DEFAULT_VECTOR_CAPACITY,
            nodeDataScanState.numRowsInNodeGroup - startOffsetInNodeGroup);
        nodeDataScanState.vectorIdx = currentNodeGroupVectorIdx++;
        return;
    }
    if (currentUnCommittedGroupIdx < localNodeGroups.size()) {
        scanState.localNodeGroup = ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(
            localNodeGroups[currentUnCommittedGroupIdx++]);
        scanState.source = TableScanSource::UNCOMMITTED;
        return;
    }
    scanState.source = TableScanSource::NONE;
}

void ScanNodeTableInfo::initScanState(NodeSemiMask* semiMask) {
    localScanState =
        std::make_unique<NodeTableScanState>(columnIDs, copyVector(columnPredicates), semiMask);
}

std::vector<NodeSemiMask*> ScanNodeTable::getSemiMasks() {
    std::vector<NodeSemiMask*> result;
    for (auto& sharedState : sharedStates) {
        result.push_back(sharedState->getSemiMask());
    }
    return result;
}

void ScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto i = 0u; i < nodeInfos.size(); ++i) {
        auto& nodeInfo = nodeInfos[i];
        nodeInfo.initScanState(sharedStates[i]->getSemiMask());
        initVectors(*nodeInfo.localScanState, *resultSet);
    }
}

void ScanNodeTable::initGlobalStateInternal(ExecutionContext* context) {
    KU_ASSERT(sharedStates.size() == nodeInfos.size());
    for (auto i = 0u; i < nodeInfos.size(); i++) {
        sharedStates[i]->initialize(context->clientContext->getTx(), nodeInfos[i].table);
    }
}

bool ScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    while (currentTableIdx < nodeInfos.size()) {
        const auto& info = nodeInfos[currentTableIdx];
        auto& scanState = *info.localScanState;
        // TODO (ANURAG): This code has been changed, not sure how this Zone Map thing is impacted.
        auto skipScan =
            transaction->isReadOnly() && scanState.zoneMapResult == ZoneMapCheckResult::SKIP_SCAN;
        if (!skipScan) {
            sharedStates[currentTableIdx]->nextMorsel(transaction, scanState);
            if (scanState.source != TableScanSource::NONE &&
                info.table->scan(transaction, scanState)) {
                if (scanState.nodeIDVector->state->getSelVector().getSelSize() > 0) {
                    return true;
                }
            }
            currentTableIdx++;
        } else {
            sharedStates[currentTableIdx]->nextMorsel(transaction, scanState);
            if (scanState.source != TableScanSource::NONE &&
                info.table->scan(transaction, scanState)) {
                if (scanState.nodeIDVector->state->getSelVector().getSelSize() > 0) {
                    return true;
                }
            }
            currentTableIdx++;
        }
    }
    return false;
}

std::unique_ptr<PhysicalOperator> ScanNodeTable::clone() {
    return make_unique<ScanNodeTable>(info.copy(), copyVector(nodeInfos), sharedStates, id,
        printInfo->copy());
}

} // namespace processor
} // namespace kuzu
