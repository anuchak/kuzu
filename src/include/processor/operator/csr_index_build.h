#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

// each entry of "csr_v" vector will be a pointer to an entry of this type
// size of each csrEntry (total neighbours being stored) will be
struct csrEntry {
    std::vector<common::offset_t> nbrNodeOffsets;
    std::vector<common::offset_t> relIDOffsets;
    csrEntry* next;

    explicit csrEntry(size_t blockSize)
        : nbrNodeOffsets{std::vector<common::offset_t>(blockSize, 0u)},
          relIDOffsets{std::vector<common::offset_t>(blockSize, 0u)}, next{nullptr} {}
};

class CSRIndexBuild : public Sink {
public:
    CSRIndexBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), operatorType, std::move(child), id, paramsString} {}

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

private:
    common::table_id_t commonNodeTableID;
    common::table_id_t commonEdgeTableID;
    std::vector<csrEntry*> csr_v; // stores a pointer to the csrEntry struct

    DataPos boundNodeVectorPos;           // constructor
    common::ValueVector* boundNodeVector; // initLocalStateInternal
    DataPos nbrNodeVectorPos;             // constructor
    common::ValueVector* nbrNodeVector;   // initLocalStateInternal
    DataPos relIDVectorPos;               // constructor
    common::ValueVector* relIDVector;     // initLocalStateInternal
};

} // namespace processor
} // namespace kuzu
