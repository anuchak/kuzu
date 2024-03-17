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
    csrEntry* next;
    size_t blockSize;

    explicit csrEntry(size_t blockSize) {
        nbrNodeOffsets = new common::offset_t[blockSize];
        relIDOffsets = new common::offset_t[blockSize];
        next = nullptr;
        this->blockSize = blockSize;
    }

    ~csrEntry() {
        delete[] nbrNodeOffsets;
        delete[] relIDOffsets;
    }
};

struct csrIndexSharedState {
    std::vector<csrEntry*> csr_v; // stores a pointer to the csrEntry struct

    csrIndexSharedState() : csr_v{std::vector<csrEntry*>()} {}

    ~csrIndexSharedState() {
        for (auto temp : csr_v) {
            while (temp) {
                auto next = temp->next;
                delete temp;
                temp = next;
            }
        }
    }
};

class CSRIndexBuild : public Sink {
public:
    CSRIndexBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        common::table_id_t commonNodeTableID, common::table_id_t commonEdgeTableID,
        DataPos& boundNodeVectorPos, DataPos& nbrNodeVectorPos, DataPos& relIDVectorPos,
        std::shared_ptr<csrIndexSharedState>& csrIndexSharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), operatorType, std::move(child), id, paramsString},
          csrSharedState{csrIndexSharedState}, commonNodeTableID{commonNodeTableID},
          commonEdgeTableID{commonEdgeTableID}, boundNodeVectorPos{boundNodeVectorPos},
          nbrNodeVectorPos{nbrNodeVectorPos}, relIDVectorPos{relIDVectorPos} {}

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
