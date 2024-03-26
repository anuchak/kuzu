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
    std::vector<common::offset_t> nbrNodeOffsets;
    std::vector<common::offset_t> relIDOffsets;
};

struct csrIndexSharedState {
    std::vector<CSREntry*> csr; // stores a pointer to the CSREntry struct

    ~csrIndexSharedState() {
        for(auto &entry : csr) {
            delete entry;
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
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::CSR_INDEX_BUILD,
              std::move(child), id, paramsString}, csrSharedState{csrIndexSharedState},
          lastCSREntryHandled{nullptr}, commonNodeTableID{commonNodeTableID},
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

    // use this to determine when to sum up the csr_v vector
    // basically we keep summing at (offset + 1) position the total no. of neighbours
    // at the end we need to sum from position 1 to 65 to update neighbour ranges
    CSREntry *lastCSREntryHandled;

    DataPos boundNodeVectorPos;           // constructor
    common::ValueVector* boundNodeVector; // initLocalStateInternal
    DataPos nbrNodeVectorPos;             // constructor
    common::ValueVector* nbrNodeVector;   // initLocalStateInternal
    DataPos relIDVectorPos;               // constructor
    common::ValueVector* relIDVector;     // initLocalStateInternal
};

} // namespace processor
} // namespace kuzu
