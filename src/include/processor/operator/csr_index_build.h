#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

// each entry of "csr_v" vector will be a pointer to an entry of this type
// size of each csrEntry (total neighbours being stored) will be
struct csrEntry {
    common::offset_t* nbrNodeOffsets;
    common::offset_t* relIDOffsets;
    int64_t* relWeightProperties;
    csrEntry* next;
    size_t blockSize;

    explicit csrEntry(size_t blockSize) {
        nbrNodeOffsets = new common::offset_t[blockSize];
        relIDOffsets = new common::offset_t[blockSize];
        relWeightProperties = new int64_t[blockSize];
        next = nullptr;
        this->blockSize = blockSize;
    }

    ~csrEntry() {
        delete[] nbrNodeOffsets;
        delete[] relIDOffsets;
        delete[] relWeightProperties;
    }
};

struct csrIndexSharedState {
    std::vector<csrEntry*> csr_v; // stores a pointer to the csrEntry struct

    csrIndexSharedState() : csr_v{std::vector<csrEntry*>()} {}

    ~csrIndexSharedState() {
        auto duration1 = std::chrono::system_clock::now().time_since_epoch();
        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
        for (auto temp : csr_v) {
            while (temp) {
                auto next = temp->next;
                delete temp;
                temp = next;
            }
        }
        auto duration2 = std::chrono::system_clock::now().time_since_epoch();
        auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
        printf("Total time taken to free memory ... %lu ms\n", millis2 - millis1);
    }
};

class CSRIndexBuild : public Sink {
public:
    CSRIndexBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        common::table_id_t commonNodeTableID, common::table_id_t commonEdgeTableID,
        DataPos& boundNodeVectorPos, DataPos& nbrNodeVectorPos, DataPos& relIDVectorPos,
        DataPos& relWeightPropertyVectorPos,
        std::shared_ptr<csrIndexSharedState>& csrIndexSharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::CSR_INDEX_BUILD,
              std::move(child), id, paramsString},
          csrSharedState{csrIndexSharedState}, commonNodeTableID{commonNodeTableID},
          commonEdgeTableID{commonEdgeTableID}, boundNodeVectorPos{boundNodeVectorPos},
          nbrNodeVectorPos{nbrNodeVectorPos}, relIDVectorPos{relIDVectorPos},
          relWeightPropertyVectorPos{relWeightPropertyVectorPos} {}

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    std::shared_ptr<csrIndexSharedState> getCSRSharedState() { return csrSharedState; }

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CSRIndexBuild>(resultSetDescriptor->copy(), commonNodeTableID,
            commonEdgeTableID, boundNodeVectorPos, nbrNodeVectorPos, relIDVectorPos,
            relWeightPropertyVectorPos, csrSharedState, children[0]->clone(), id, paramsString);
    }

private:
    common::table_id_t commonNodeTableID;
    common::table_id_t commonEdgeTableID;
    std::shared_ptr<csrIndexSharedState> csrSharedState;

    DataPos boundNodeVectorPos;                   // constructor
    common::ValueVector* boundNodeVector;         // initLocalStateInternal
    DataPos nbrNodeVectorPos;                     // constructor
    common::ValueVector* nbrNodeVector;           // initLocalStateInternal
    DataPos relIDVectorPos;                       // constructor
    common::ValueVector* relIDVector;             // initLocalStateInternal
    DataPos relWeightPropertyVectorPos;           // constructor
    common::ValueVector* relWeightPropertyVector; // initLocalStateInternal
};

} // namespace processor
} // namespace kuzu
