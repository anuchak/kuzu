#include "storage/storage_structure/in_mem_file.h"

#include <mutex>

#include "common/type_utils.h"

namespace kuzu {
namespace storage {

InMemFile::InMemFile(
    string filePath, uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages)
    : filePath{std::move(filePath)}, numBytesForElement{numBytesForElement}, hasNullMask{
                                                                                 hasNullMask} {
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesForElement, hasNullMask);
    for (auto i = 0u; i < numPages; i++) {
        addANewPage();
    }
}

void InMemFile::addNewPages(uint64_t numNewPagesToAdd, bool setToZero) {
    for (uint64_t i = 0; i < numNewPagesToAdd; ++i) {
        addANewPage(setToZero);
    }
}

uint32_t InMemFile::addANewPage(bool setToZero) {
    auto newPageIdx = pages.size();
    pages.push_back(make_unique<InMemPage>(numElementsInAPage, numBytesForElement, hasNullMask));
    if (setToZero) {
        memset(pages[newPageIdx]->data, 0, DEFAULT_PAGE_SIZE);
    }
    return newPageIdx;
}

void InMemFile::flush() {
    if (filePath.empty()) {
        throw CopyException("InMemPages: Empty filename");
    }
    auto fileInfo = FileUtils::openFile(filePath, O_CREAT | O_WRONLY);
    for (auto pageIdx = 0u; pageIdx < pages.size(); pageIdx++) {
        pages[pageIdx]->encodeNullBits();
        FileUtils::writeToFile(
            fileInfo.get(), pages[pageIdx]->data, DEFAULT_PAGE_SIZE, pageIdx * DEFAULT_PAGE_SIZE);
    }
}

ku_string_t InMemOverflowFile::appendString(const char* rawString) {
    ku_string_t result;
    auto length = strlen(rawString);
    result.len = length;
    std::memcpy(result.prefix, rawString,
        length <= ku_string_t::SHORT_STR_LENGTH ? length : ku_string_t::PREFIX_LENGTH);
    if (length > ku_string_t::SHORT_STR_LENGTH) {
        unique_lock lck{lock};
        // Allocate a new page if necessary.
        if (nextOffsetInPageToAppend + length >= DEFAULT_PAGE_SIZE) {
            addANewPage();
            nextOffsetInPageToAppend = 0;
            nextPageIdxToAppend++;
        }
        pages[nextPageIdxToAppend]->write(
            nextOffsetInPageToAppend, nextOffsetInPageToAppend, (uint8_t*)rawString, length);
        TypeUtils::encodeOverflowPtr(
            result.overflowPtr, nextPageIdxToAppend, nextOffsetInPageToAppend);
        nextOffsetInPageToAppend += length;
    }
    return result;
}

ku_string_t InMemOverflowFile::copyString(const char* rawString, PageByteCursor& overflowCursor) {
    ku_string_t kuString;
    kuString.len = strlen(rawString);
    std::memcpy(kuString.prefix, rawString,
        kuString.len <= ku_string_t::SHORT_STR_LENGTH ? kuString.len : ku_string_t::PREFIX_LENGTH);
    if (kuString.len > ku_string_t::SHORT_STR_LENGTH) {
        copyStringOverflow(overflowCursor, (uint8_t*)rawString, &kuString);
    }
    return kuString;
}

void InMemOverflowFile::copyFixedSizedValuesInList(
    const Value& listVal, PageByteCursor& overflowCursor, uint64_t numBytesOfListElement) {
    shared_lock lck(lock);
    for (auto& value : listVal.getListValReference()) {
        pages[overflowCursor.pageIdx]->write(overflowCursor.offsetInPage,
            overflowCursor.offsetInPage, (uint8_t*)&value->val, numBytesOfListElement);
        overflowCursor.offsetInPage += numBytesOfListElement;
    }
}

template<DataTypeID DT>
void InMemOverflowFile::copyVarSizedValuesInList(ku_list_t& resultKUList, const Value& listVal,
    PageByteCursor& overflowCursor, uint64_t numBytesOfListElement) {
    auto overflowPageIdx = overflowCursor.pageIdx;
    auto overflowPageOffset = overflowCursor.offsetInPage;
    // Reserve space for ku_list or ku_string objects.
    overflowCursor.offsetInPage += (resultKUList.size * numBytesOfListElement);
    if constexpr (DT == STRING) {
        vector<ku_string_t> kuStrings(listVal.listVal.size());
        for (auto i = 0u; i < listVal.listVal.size(); i++) {
            assert(listVal.listVal[i]->dataType.typeID == STRING);
            kuStrings[i] = copyString(listVal.listVal[i]->strVal.c_str(), overflowCursor);
        }
        shared_lock lck(lock);
        for (auto i = 0u; i < listVal.listVal.size(); i++) {
            pages[overflowPageIdx]->write(overflowPageOffset + (i * numBytesOfListElement),
                overflowPageOffset + (i * numBytesOfListElement), (uint8_t*)&kuStrings[i],
                numBytesOfListElement);
        }
    } else {
        assert(DT == LIST);
        vector<ku_list_t> kuLists(listVal.listVal.size());
        for (auto i = 0u; i < listVal.listVal.size(); i++) {
            assert(listVal.listVal[i]->dataType.typeID == LIST);
            kuLists[i] = copyList(*listVal.listVal[i], overflowCursor);
        }
        shared_lock lck(lock);
        for (auto i = 0u; i < listVal.listVal.size(); i++) {
            pages[overflowPageIdx]->write(overflowPageOffset + (i * numBytesOfListElement),
                overflowPageOffset + (i * numBytesOfListElement), (uint8_t*)&kuLists[i],
                numBytesOfListElement);
        }
    }
}

ku_list_t InMemOverflowFile::copyList(const Value& listValue, PageByteCursor& overflowCursor) {
    assert(listValue.dataType.typeID == LIST);
    ku_list_t resultKUList;
    auto numBytesOfListElement = Types::getDataTypeSize(*listValue.dataType.childType);
    resultKUList.size = listValue.listVal.size();
    // Allocate a new page if necessary.
    if (overflowCursor.offsetInPage + (resultKUList.size * numBytesOfListElement) >=
            DEFAULT_PAGE_SIZE ||
        overflowCursor.pageIdx == UINT32_MAX) {
        overflowCursor.offsetInPage = 0;
        overflowCursor.pageIdx = addANewOverflowPage();
    }
    TypeUtils::encodeOverflowPtr(
        resultKUList.overflowPtr, overflowCursor.pageIdx, overflowCursor.offsetInPage);
    switch (listValue.dataType.childType->typeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        copyFixedSizedValuesInList(listValue, overflowCursor, numBytesOfListElement);
    } break;
    case STRING: {
        copyVarSizedValuesInList<STRING>(
            resultKUList, listValue, overflowCursor, numBytesOfListElement);
    } break;
    case LIST: {
        copyVarSizedValuesInList<LIST>(
            resultKUList, listValue, overflowCursor, numBytesOfListElement);
    } break;
    default: {
        throw CopyException("Unsupported data type inside LIST.");
    }
    }
    return resultKUList;
}

void InMemOverflowFile::copyStringOverflow(
    PageByteCursor& overflowCursor, uint8_t* srcOverflow, ku_string_t* dstKUString) {
    // Allocate a new page if necessary.
    if (overflowCursor.offsetInPage + dstKUString->len >= DEFAULT_PAGE_SIZE ||
        overflowCursor.pageIdx == UINT32_MAX) {
        overflowCursor.offsetInPage = 0;
        overflowCursor.pageIdx = addANewOverflowPage();
    }
    TypeUtils::encodeOverflowPtr(
        dstKUString->overflowPtr, overflowCursor.pageIdx, overflowCursor.offsetInPage);
    shared_lock lck(lock);
    pages[overflowCursor.pageIdx]->write(
        overflowCursor.offsetInPage, overflowCursor.offsetInPage, srcOverflow, dstKUString->len);
    overflowCursor.offsetInPage += dstKUString->len;
}

void InMemOverflowFile::copyListOverflow(InMemOverflowFile* srcInMemOverflowFile,
    const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
    ku_list_t* dstKUList, DataType* listChildDataType) {
    auto numBytesOfListElement = Types::getDataTypeSize(*listChildDataType);
    // Allocate a new page if necessary.
    if (dstOverflowCursor.offsetInPage + (dstKUList->size * numBytesOfListElement) >=
            DEFAULT_PAGE_SIZE ||
        dstOverflowCursor.pageIdx == UINT32_MAX) {
        dstOverflowCursor.offsetInPage = 0;
        dstOverflowCursor.pageIdx = addANewOverflowPage();
    }
    TypeUtils::encodeOverflowPtr(
        dstKUList->overflowPtr, dstOverflowCursor.pageIdx, dstOverflowCursor.offsetInPage);
    auto dataToCopyFrom = srcInMemOverflowFile->pages[srcOverflowCursor.pageIdx]->data +
                          srcOverflowCursor.offsetInPage;
    auto offsetToCopyInto = dstOverflowCursor.offsetInPage;
    dstOverflowCursor.offsetInPage += dstKUList->size * numBytesOfListElement;
    if (listChildDataType->typeID == LIST) {
        auto elementsInList = (ku_list_t*)dataToCopyFrom;
        for (auto i = 0u; i < dstKUList->size; i++) {
            PageByteCursor elementCursor;
            TypeUtils::decodeOverflowPtr(
                elementsInList[i].overflowPtr, elementCursor.pageIdx, elementCursor.offsetInPage);
            copyListOverflow(srcInMemOverflowFile, elementCursor, dstOverflowCursor,
                &elementsInList[i], listChildDataType->childType.get());
        }
    } else if (listChildDataType->typeID == STRING) {
        auto elementsInList = (ku_string_t*)dataToCopyFrom;
        for (auto i = 0u; i < dstKUList->size; i++) {
            if (elementsInList[i].len > ku_string_t::SHORT_STR_LENGTH) {
                PageByteCursor elementCursor;
                TypeUtils::decodeOverflowPtr(elementsInList[i].overflowPtr, elementCursor.pageIdx,
                    elementCursor.offsetInPage);
                copyStringOverflow(dstOverflowCursor,
                    srcInMemOverflowFile->pages[elementCursor.pageIdx]->data +
                        elementCursor.offsetInPage,
                    &elementsInList[i]);
            }
        }
    }
    shared_lock lck(lock);
    pages[dstOverflowCursor.pageIdx]->write(offsetToCopyInto, offsetToCopyInto, dataToCopyFrom,
        dstKUList->size * numBytesOfListElement);
}

page_idx_t InMemOverflowFile::addANewOverflowPage() {
    unique_lock lck(lock);
    auto newPageIdx = pages.size();
    addANewPage();
    return newPageIdx;
}

string InMemOverflowFile::readString(ku_string_t* strInInMemOvfFile) {
    if (ku_string_t::isShortString(strInInMemOvfFile->len)) {
        return strInInMemOvfFile->getAsShortString();
    } else {
        page_idx_t pageIdx = UINT32_MAX;
        uint16_t pagePos = UINT16_MAX;
        TypeUtils::decodeOverflowPtr(strInInMemOvfFile->overflowPtr, pageIdx, pagePos);
        return string(
            reinterpret_cast<char*>(pages[pageIdx]->data + pagePos), strInInMemOvfFile->len);
    }
}

} // namespace storage
} // namespace kuzu
