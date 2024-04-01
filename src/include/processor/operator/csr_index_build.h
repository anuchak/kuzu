#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

#define MORSEL_SIZE 64
#define RIGHT_SHIFT (uint64_t) log2(MORSEL_SIZE)
#define OFFSET_DIV 0x3F

struct CSREntry {
    uint64_t csr_v[MORSEL_SIZE + 1]{0};
    common::offset_t *nbrNodeOffsets;
    common::offset_t *relIDOffsets;
    uint64_t blockSize;

    CSREntry() {
        nbrNodeOffsets = new common::offset_t [2048 * MORSEL_SIZE];
        relIDOffsets = new common::offset_t [2048 * MORSEL_SIZE];
        blockSize = 2048 * MORSEL_SIZE;
    }

    void resize() {
        auto oldBlockSize = blockSize;
        blockSize = std::ceil((double) oldBlockSize * 2);

        auto newNbrNodeOffsets = new uint64_t[blockSize];
        std::memcpy(newNbrNodeOffsets, nbrNodeOffsets, sizeof(uint64_t) * oldBlockSize);
        auto temp = nbrNodeOffsets;
        nbrNodeOffsets = newNbrNodeOffsets;
        delete [] temp;

        auto newRelIDOffsets = new uint64_t[blockSize];
        std::memcpy(newRelIDOffsets, relIDOffsets, sizeof(uint64_t) * oldBlockSize);
        auto temp1 = relIDOffsets;
        relIDOffsets = newRelIDOffsets;
        delete [] temp1;
    }

    ~CSREntry() {
        delete[] nbrNodeOffsets;
        delete[] relIDOffsets;
    }

};

struct csrIndexSharedState {
    std::vector<CSREntry*> csr; // stores a pointer to the CSREntry struct

    ~csrIndexSharedState() {
        auto duration1 = std::chrono::system_clock::now().time_since_epoch();
        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
        printf("starting to release back memory ...\n");
        auto totalMemoryAllocated = 0lu;
        auto totalMemoryActuallyUsed = 0lu;
        for (auto &entry : csr) {
            if (entry) {
                totalMemoryAllocated += sizeof(CSREntry) + 8;
                totalMemoryAllocated += (entry->blockSize * 8 * 2);
                totalMemoryActuallyUsed += sizeof(CSREntry) + 8 +
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
