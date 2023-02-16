#include "processor/operator/shortestpath/src_dest_collector.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void BFSFTableSharedState::initTableIfNecessary(
    MemoryManager* memoryManager, std::unique_ptr<FactorizedTableSchema> tableSchema) {
    if (table == nullptr) {
        nextTupleIdxToScan = 0u;
        table = std::make_unique<FactorizedTable>(memoryManager, std::move(tableSchema));
    }
}

void BFSFTableSharedState::mergeLocalTable(kuzu::processor::FactorizedTable& localTable) {
    std::lock_guard<std::mutex> lck{mtx};
    table->merge(localTable);
}

void SrcDestCollector::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    vectorsToCollect.push_back(resultSet->dataChunks[srcPosAndType.first.dataChunkPos]
                                   ->valueVectors[srcPosAndType.first.valueVectorPos]);
    vectorsToCollect.push_back(resultSet->dataChunks[destPosAndType.first.dataChunkPos]
                                   ->valueVectors[destPosAndType.first.valueVectorPos]);
    for (auto [dataPos, _] : payloadsPosAndType) {
        auto vector =
            resultSet->dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
        vectorsToCollect.push_back(vector);
    }
    localTable = std::make_unique<FactorizedTable>(context->memoryManager, populateTableSchema());
}

std::unique_ptr<FactorizedTableSchema> SrcDestCollector::populateTableSchema() {
    std::unique_ptr<FactorizedTableSchema> tableSchema = std::make_unique<FactorizedTableSchema>();

    // currently we consider 1 src node at a time
    tableSchema->appendColumn(std::make_unique<ColumnSchema>(false /* is unFlat */,
        srcPosAndType.first.dataChunkPos, Types::getDataTypeSize(srcPosAndType.second)));

    // dest nodes will be sent in unFlat manner
    tableSchema->appendColumn(std::make_unique<ColumnSchema>(true /* is unFlat */,
        destPosAndType.first.dataChunkPos, (uint32_t)sizeof(overflow_value_t)));

    for (auto i = 0u; i < payloadsPosAndType.size(); ++i) {
        auto [dataPos, dataType] = payloadsPosAndType[i];
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(!isPayloadFlat[i], dataPos.dataChunkPos,
                isPayloadFlat[i] ? Types::getDataTypeSize(dataType) :
                                   (uint32_t)sizeof(overflow_value_t)));
    }
    return tableSchema;
}

void SrcDestCollector::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->initTableIfNecessary(context->memoryManager, populateTableSchema());
}

std::unique_ptr<BFSScanMorsel> BFSFTableSharedState::getMorsel(uint64_t maxMorselSize) {
    std::lock_guard<std::mutex> lck{mtx};
    auto numTuplesToScan = std::min(maxMorselSize, table->getNumTuples() - nextTupleIdxToScan);
    auto morsel = std::make_unique<BFSScanMorsel>(table, nextTupleIdxToScan, numTuplesToScan);
    nextTupleIdxToScan += numTuplesToScan;
    return morsel;
}

void SrcDestCollector::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple()) {
        if (!vectorsToCollect.empty()) {
            for (auto i = 0u; i < resultSet->multiplicity; i++) {
                localTable->append(vectorsToCollect);
            }
        }
    }
    if (!vectorsToCollect.empty()) {
        sharedState->mergeLocalTable(*localTable);
    }
}

} // namespace processor
} // namespace kuzu