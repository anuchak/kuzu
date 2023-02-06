#pragma once

#include "catalog/catalog.h"
#include "common/types/value.h"
#include "storage/storage_structure/disk_overflow_file.h"
#include "storage/storage_structure/storage_structure.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace std;

namespace kuzu {
namespace storage {

class Column : public BaseColumnOrList {

public:
    Column(const StorageStructureIDAndFName& structureIDAndFName, const DataType& dataType,
        size_t elementSize, BufferManager& bufferManager, WAL* wal)
        : BaseColumnOrList{structureIDAndFName, dataType, elementSize, bufferManager,
              true /*hasNULLBytes*/, wal} {};

    Column(const StorageStructureIDAndFName& structureIDAndFName, const DataType& dataType,
        BufferManager& bufferManager, WAL* wal)
        : Column(structureIDAndFName, dataType, Types::getDataTypeSize(dataType), bufferManager,
              wal){};

    void read(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector);

    void writeValues(const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& vectorToWriteFrom);

    // Currently, used only in CopyCSV tests.
    virtual Value readValue(offset_t offset);
    bool isNull(offset_t nodeOffset, Transaction* transaction);
    void setNodeOffsetToNull(offset_t nodeOffset);

protected:
    void lookup(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector, uint32_t vectorPos);

    virtual void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor);
    virtual inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) {
        readBySequentialCopy(transaction, resultVector, cursor, identityMapper);
    }
    virtual void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageElementCursor& cursor) {
        readBySequentialCopyWithSelState(transaction, resultVector, cursor, identityMapper);
    }
    virtual void writeValueForSingleNodeIDPosition(offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    WALPageIdxPosInPageAndFrame beginUpdatingPage(offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom);

private:
    // The reason why we make this function virtual is: we can't simply do memcpy on nodeIDs if
    // the adjColumn has tableIDCompression, in this case we only store the nodeOffset in
    // persistent store of adjColumn.
    virtual inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
        memcpy(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage),
            vectorToWriteFrom->getData() + getElemByteOffset(posInVectorToWriteFrom), elementSize);
    }
    // If necessary creates a second version (backed by the WAL) of a page that contains the fixed
    // length part of the value that will be written to.
    // Obtains *and does not release* the lock original page. Pins and updates the WAL version of
    // the page. Finally updates the page with the new value from vectorToWriteFrom.
    // Note that caller must ensure to unpin and release the WAL version of the page by calling
    // StorageStructure::unpinWALPageAndReleaseOriginalPageLock.
    WALPageIdxPosInPageAndFrame beginUpdatingPageAndWriteOnlyNullBit(
        offset_t nodeOffset, bool isNull);

protected:
    // no logical-physical page mapping is required for columns
    std::function<page_idx_t(page_idx_t)> identityMapper = [](uint32_t i) { return i; };
};

class PropertyColumnWithOverflow : public Column {
public:
    PropertyColumnWithOverflow(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, WAL* wal)
        : Column{structureIDAndFNameOfMainColumn, dataType, bufferManager, wal},
          diskOverflowFile{structureIDAndFNameOfMainColumn, bufferManager, wal} {}

    inline void read(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
        const shared_ptr<ValueVector>& resultVector) {
        resultVector->resetOverflowBuffer();
        Column::read(transaction, nodeIDVector, resultVector);
    }
    inline DiskOverflowFile* getDiskOverflowFile() { return &diskOverflowFile; }

    inline VersionedFileHandle* getDiskOverflowFileHandle() {
        return diskOverflowFile.getFileHandle();
    }

protected:
    DiskOverflowFile diskOverflowFile;
};

class StringPropertyColumn : public PropertyColumnWithOverflow {

public:
    StringPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {};

    void writeValueForSingleNodeIDPosition(offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    // Currently, used only in CopyCSV tests.
    Value readValue(offset_t offset) override;

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        Column::lookup(transaction, resultVector, vectorPos, cursor);
        if (!resultVector->isNull(vectorPos)) {
            diskOverflowFile.scanSingleStringOverflow(
                transaction->getType(), *resultVector, vectorPos);
        }
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        Column::scan(transaction, resultVector, cursor);
        diskOverflowFile.scanSequentialStringOverflow(transaction->getType(), *resultVector);
    }
    void scanWithSelState(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        Column::scanWithSelState(transaction, resultVector, cursor);
        diskOverflowFile.scanSequentialStringOverflow(transaction->getType(), *resultVector);
    }
};

class ListPropertyColumn : public PropertyColumnWithOverflow {

public:
    ListPropertyColumn(const StorageStructureIDAndFName& structureIDAndFNameOfMainColumn,
        const DataType& dataType, BufferManager& bufferManager, WAL* wal)
        : PropertyColumnWithOverflow{
              structureIDAndFNameOfMainColumn, dataType, bufferManager, wal} {};

    void writeValueForSingleNodeIDPosition(offset_t nodeOffset,
        const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    Value readValue(offset_t offset) override;

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        Column::lookup(transaction, resultVector, vectorPos, cursor);
        if (!resultVector->isNull(vectorPos)) {
            diskOverflowFile.scanSingleListOverflow(
                transaction->getType(), *resultVector, vectorPos);
        }
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        Column::scan(transaction, resultVector, cursor);
        diskOverflowFile.readListsToVector(transaction->getType(), *resultVector);
    }
    inline void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageElementCursor& cursor) override {
        Column::scanWithSelState(transaction, resultVector, cursor);
        diskOverflowFile.readListsToVector(transaction->getType(), *resultVector);
    }
};

class RelIDColumn : public Column {

public:
    RelIDColumn(const StorageStructureIDAndFName& structureIDAndFName, BufferManager& bufferManager,
        WAL* wal)
        : Column{structureIDAndFName, DataType(INTERNAL_ID), sizeof(offset_t), bufferManager, wal},
          commonTableID{structureIDAndFName.storageStructureID.columnFileID.relPropertyColumnID
                            .relNodeTableAndDir.relTableID} {
        assert(structureIDAndFName.storageStructureID.columnFileID.columnType ==
               ColumnType::REL_PROPERTY_COLUMN);
        assert(structureIDAndFName.storageStructureID.storageStructureType ==
               StorageStructureType::COLUMN);
    }

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        readInternalIDsFromAPageBySequentialCopy(transaction, resultVector, vectorPos,
            cursor.pageIdx, cursor.elemPosInPage, 1 /* numValuesToCopy */, commonTableID,
            false /* hasNoNullGuarantee */);
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        readInternalIDsBySequentialCopy(transaction, resultVector, cursor, identityMapper,
            commonTableID, false /* hasNoNullGuarantee */);
    }
    inline void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageElementCursor& cursor) override {
        readInternalIDsBySequentialCopyWithSelState(
            transaction, resultVector, cursor, identityMapper, commonTableID);
    }
    inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        const shared_ptr<ValueVector>& vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) override {
        auto relID = vectorToWriteFrom->getValue<relID_t>(posInVectorToWriteFrom);
        memcpy(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage), &relID.offset,
            sizeof(relID.offset));
    }

private:
    table_id_t commonTableID;
};

class AdjColumn : public Column {

public:
    AdjColumn(const StorageStructureIDAndFName& structureIDAndFName, table_id_t commonNbrTableID,
        BufferManager& bufferManager, WAL* wal)
        : Column{structureIDAndFName, DataType(INTERNAL_ID), sizeof(offset_t), bufferManager, wal},
          commonNbrTableID{commonNbrTableID} {};

private:
    inline void lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        uint32_t vectorPos, PageElementCursor& cursor) override {
        readInternalIDsFromAPageBySequentialCopy(transaction, resultVector, vectorPos,
            cursor.pageIdx, cursor.elemPosInPage, 1 /* numValuesToCopy */, commonNbrTableID,
            false /* hasNoNullGuarantee */);
    }
    inline void scan(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
        PageElementCursor& cursor) override {
        readInternalIDsBySequentialCopy(transaction, resultVector, cursor, identityMapper,
            commonNbrTableID, false /* hasNoNullGuarantee */);
    }
    inline void scanWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& resultVector, PageElementCursor& cursor) override {
        readInternalIDsBySequentialCopyWithSelState(
            transaction, resultVector, cursor, identityMapper, commonNbrTableID);
    }
    inline void writeToPage(WALPageIdxPosInPageAndFrame& walPageInfo,
        const shared_ptr<ValueVector>& vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) override {
        *(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage)) =
            vectorToWriteFrom->getValue<nodeID_t>(posInVectorToWriteFrom).offset;
    }

private:
    table_id_t commonNbrTableID;
};

class ColumnFactory {

public:
    static unique_ptr<Column> getColumn(const StorageStructureIDAndFName& structureIDAndFName,
        const DataType& dataType, BufferManager& bufferManager, WAL* wal) {
        switch (dataType.typeID) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
        case INTERVAL:
            return make_unique<Column>(structureIDAndFName, dataType, bufferManager, wal);
        case STRING:
            return make_unique<StringPropertyColumn>(
                structureIDAndFName, dataType, bufferManager, wal);
        case LIST:
            return make_unique<ListPropertyColumn>(
                structureIDAndFName, dataType, bufferManager, wal);
        case INTERNAL_ID:
            assert(structureIDAndFName.storageStructureID.storageStructureType ==
                   StorageStructureType::COLUMN);
            assert(structureIDAndFName.storageStructureID.columnFileID.columnType ==
                   ColumnType::REL_PROPERTY_COLUMN);
            return make_unique<RelIDColumn>(structureIDAndFName, bufferManager, wal);
        default:
            throw StorageException("Invalid type for property column creation.");
        }
    }
};

} // namespace storage
} // namespace kuzu
