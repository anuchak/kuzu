#include "planner/operator/extend/logical_recursive_extend.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include <graph/on_disk_graph.h>

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static std::shared_ptr<RecursiveJoinSharedState> createSharedState(const NodeExpression& nbrNode,
    const NodeExpression& boundNode, const RelExpression& rel, RecursiveJoinDataInfo* dataInfo,
    std::shared_ptr<FTableScanSharedState>& fTableSharedState, main::ClientContext* context) {
    std::vector<std::unique_ptr<NodeOffsetLevelSemiMask>> semiMasks;
    for (auto tableID : nbrNode.getTableIDs()) {
        auto table = context->getStorageManager()->getTable(tableID)->ptrCast<storage::NodeTable>();
        semiMasks.push_back(std::make_unique<NodeOffsetLevelSemiMask>(tableID,
            table->getMaxNodeOffset(context->getTx())));
    }
    std::shared_ptr<MorselDispatcher> morselDispatcher;
    bool isSingleLabel = boundNode.getTableIDs().size() == 1 && nbrNode.getTableIDs().size() == 1 &&
                         dataInfo->recursiveDstNodeTableIDs.size() == 1;
    auto maxNodeOffset = context->getStorageManager()->getNodeTableMaxNodes(
                             nbrNode.getTableIDs()[0], context->getTx()) -
                         1;
#if defined(_MSC_VER)
    morselDispatcher = std::make_shared<MorselDispatcher>(common::SchedulerType::OneThreadOneMorsel,
        rel.getLowerBound(), rel.getUpperBound(), maxNodeOffset);
#else
    if (isSingleLabel) {
        morselDispatcher =
            std::make_shared<MorselDispatcher>(context->getClientConfig()->bfsSchedulerType,
                rel.getLowerBound(), rel.getUpperBound(), maxNodeOffset);
    } else {
        morselDispatcher =
            std::make_shared<MorselDispatcher>(common::SchedulerType::OneThreadOneMorsel,
                rel.getLowerBound(), rel.getUpperBound(), maxNodeOffset);
    }
#endif
    auto diskGraph = std::make_unique<graph::OnDiskGraph>(context, nbrNode.getSingleTableID(),
        rel.getSingleTableID());
    return std::make_shared<RecursiveJoinSharedState>(morselDispatcher, fTableSharedState,
        std::move(semiMasks), std::move(diskGraph));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapRecursiveExtend(LogicalOperator* logicalOperator) {
    auto extend = logicalOperator->constPtrCast<LogicalRecursiveExtend>();
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto recursiveInfo = rel->getRecursiveInfo();
    // Map recursive plan
    auto logicalRecursiveRoot = extend->getRecursiveChild();
    auto recursiveRoot = mapOperator(logicalRecursiveRoot.get());
    auto recursivePlanSchema = logicalRecursiveRoot->getSchema();
    // Generate RecursiveJoin
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto expressions = inSchema->getExpressionsInScope();
    // Data info
    auto dataInfo = RecursiveJoinDataInfo();
    dataInfo.srcNodePos = getDataPos(*boundNode->getInternalID(), *inSchema);
    dataInfo.dstNodePos = getDataPos(*nbrNode->getInternalID(), *outSchema);
    dataInfo.dstNodeTableIDs = nbrNode->getTableIDsSet();
    dataInfo.pathLengthPos = getDataPos(*rel->getLengthExpression(), *outSchema);
    dataInfo.localResultSetDescriptor = std::make_unique<ResultSetDescriptor>(recursivePlanSchema);
    dataInfo.recursiveSrcNodeIDPos =
        getDataPos(*recursiveInfo->node->getInternalID(), *recursivePlanSchema);
    dataInfo.recursiveNodePredicateExecFlagPos =
        getDataPos(*recursiveInfo->nodePredicateExecFlag, *recursivePlanSchema);
    dataInfo.recursiveDstNodeIDPos =
        getDataPos(*recursiveInfo->nodeCopy->getInternalID(), *recursivePlanSchema);
    dataInfo.recursiveDstNodeTableIDs = recursiveInfo->node->getTableIDsSet();
    dataInfo.recursiveEdgeIDPos =
        getDataPos(*recursiveInfo->rel->getInternalIDProperty(), *recursivePlanSchema);
    if (extend->getJoinType() == RecursiveJoinType::TRACK_PATH) {
        dataInfo.pathPos = getDataPos(*rel, *outSchema);
    } else {
        dataInfo.pathPos = DataPos::getInvalidPos();
    }
    for (auto& entry : clientContext->getCatalog()->getTableEntries(clientContext->getTx())) {
        dataInfo.tableIDToName.insert({entry->getTableID(), entry->getName()});
    }
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto resultCollector = createResultCollector(common::AccumulateType::REGULAR, expressions,
        inSchema, std::move(prevOperator));
    auto sharedFTable = ((ResultCollector*)resultCollector.get())->getResultFactorizedTable();
    auto fTableSharedState = std::make_shared<FTableScanSharedState>(sharedFTable, 1u);
    auto sharedState =
        createSharedState(*nbrNode, *boundNode, *rel, &dataInfo, fTableSharedState, clientContext);
    // Info
    auto info = RecursiveJoinInfo();
    info.dataInfo = std::move(dataInfo);
    info.lowerBound = rel->getLowerBound();
    info.upperBound = rel->getUpperBound();
    info.queryRelType = rel->getRelType();
    info.joinType = extend->getJoinType();
    info.direction = extend->getDirection();
    auto printInfo = std::make_unique<OPPrintInfo>(extend->getExpressionsForPrinting());
    std::vector<DataPos> outDataPoses;
    std::vector<uint32_t> colIndicesToScan;
    for (auto i = 0u; i < expressions.size(); ++i) {
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expressions[i]));
        colIndicesToScan.push_back(i);
    }
    return std::make_unique<RecursiveJoin>(std::move(info), sharedState, outDataPoses,
        colIndicesToScan, std::move(resultCollector), getOperatorID(), std::move(recursiveRoot),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
