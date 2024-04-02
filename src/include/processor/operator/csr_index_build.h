#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include <immintrin.h> // Include Intel Intrinsics header

namespace kuzu {
namespace processor {

#define MORSEL_SIZE 64
#define RIGHT_SHIFT (uint64_t) log2(MORSEL_SIZE)
#define OFFSET_DIV 0x3F

struct MorselCSR {
    uint64_t csr_v[MORSEL_SIZE + 1]{0};
    common::offset_t *nbrNodeOffsets;
    common::offset_t *relIDOffsets;
    uint64_t blockSize;

    MorselCSR() {
        posix_memalign((void **)(&nbrNodeOffsets), 32, 2048 * MORSEL_SIZE * sizeof(uint64_t));
        posix_memalign((void **)(&relIDOffsets), 32, 2048 * MORSEL_SIZE * sizeof(uint64_t));
        blockSize = 2048 * MORSEL_SIZE;
    }

    static void memcpy_simd(const void* src, void* dest, size_t size) {
        // Cast the source and destination pointers to the appropriate data types
        const char* src_ptr = static_cast<const char*>(src);
        char* dest_ptr = static_cast<char*>(dest);

        // Iterate over the memory block in 256-bit (32-byte) chunks
        for (size_t i = 0; i < size; i += 32) {
            // Load 256 bits (32 bytes) from source into a SIMD register
            __m256i chunk = _mm256_load_si256(reinterpret_cast<const __m256i*>(src_ptr + i));
            // Store the loaded data to destination
            _mm256_store_si256(reinterpret_cast<__m256i*>(dest_ptr + i), chunk);
        }

        // copy the remainder of the array from src to dst, subtract remainder from size to get pos
        // this is done in case the array's size is not an exact multiple of 32
        for (size_t i = size - (size % 32); i < size; ++i) {
            dest_ptr[i] = src_ptr[i];
        }
    }

    void resize() {
        auto oldBlockSize = blockSize;
        blockSize = std::ceil((double) oldBlockSize * 2);

        common::offset_t *newNbrNodeOffsets;
        posix_memalign((void **)(&newNbrNodeOffsets), 32, blockSize * sizeof(uint64_t));
        memcpy_simd(nbrNodeOffsets, newNbrNodeOffsets, oldBlockSize * sizeof(uint64_t));
        auto temp = nbrNodeOffsets;
        nbrNodeOffsets = newNbrNodeOffsets;
        delete [] temp;

        common::offset_t *newRelIDOffsets;
        posix_memalign((void **)(&newRelIDOffsets), 32, blockSize * sizeof(uint64_t));
        memcpy_simd(relIDOffsets, newRelIDOffsets, oldBlockSize * sizeof(uint64_t));
        auto temp1 = relIDOffsets;
        relIDOffsets = newRelIDOffsets;
        delete [] temp1;
    }

    ~MorselCSR() {
        delete[] nbrNodeOffsets;
        delete[] relIDOffsets;
    }

};

struct csrIndexSharedState {
    std::vector<MorselCSR*> csr; // stores a pointer to the CSREntry struct

    ~csrIndexSharedState() {
        auto duration1 = std::chrono::system_clock::now().time_since_epoch();
        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
        printf("starting to release back memory ...\n");
        auto totalMemoryAllocated = 0lu;
        auto totalMemoryActuallyUsed = 0lu;
        for (auto &entry : csr) {
            if (entry) {
                totalMemoryAllocated += sizeof(MorselCSR) + 8;
                totalMemoryAllocated += (entry->blockSize * 8 * 2);
                totalMemoryActuallyUsed += sizeof(MorselCSR) + 8 +
                                           (entry->csr_v[MORSEL_SIZE] * 8 * 2);
                delete entry;
            }
        }
        auto duration2 = std::chrono::system_clock::now().time_since_epoch();
        auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
        printf("time taken to free memory %lu ms\n", millis2 - millis1);
        printf("total memory allocated: %lu bytes | total memory actually used: %lu bytes\n",
            totalMemoryAllocated, totalMemoryActuallyUsed);
    }
};

class CSRIndexBuild : public Sink {
public:
    CSRIndexBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        common::table_id_t commonNodeTableID, common::table_id_t commonEdgeTableID,
        DataPos& boundNodeVectorPos, DataPos& nbrNodeVectorPos, DataPos& relIDVectorPos,
        std::shared_ptr<csrIndexSharedState>& csrIndexSharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::CSR_INDEX_BUILD,
              std::move(child), id, paramsString}, csrSharedState{csrIndexSharedState},
          commonNodeTableID{commonNodeTableID}, commonEdgeTableID{commonEdgeTableID},
          boundNodeVectorPos{boundNodeVectorPos}, nbrNodeVectorPos{nbrNodeVectorPos},
          relIDVectorPos{relIDVectorPos} {}

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    std::shared_ptr<csrIndexSharedState> getCSRSharedState() { return csrSharedState; }

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CSRIndexBuild>(resultSetDescriptor->copy(), commonNodeTableID,
            commonEdgeTableID, boundNodeVectorPos, nbrNodeVectorPos, relIDVectorPos, csrSharedState,
            children[0]->clone(), id, paramsString);
    }

private:
    common::table_id_t commonNodeTableID;
    common::table_id_t commonEdgeTableID;
    std::shared_ptr<csrIndexSharedState> csrSharedState;

    DataPos boundNodeVectorPos;           // constructor
    common::ValueVector* boundNodeVector; // initLocalStateInternal
    DataPos nbrNodeVectorPos;             // constructor
    common::ValueVector* nbrNodeVector;   // initLocalStateInternal
    DataPos relIDVectorPos;               // constructor
    common::ValueVector* relIDVector;     // initLocalStateInternal
};

} // namespace processor
} // namespace kuzu
