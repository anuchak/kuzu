#pragma once

#include "catalog/table_schema.h"

namespace kuzu {
namespace binder {

struct BoundExtraCreateTableInfo {
    virtual ~BoundExtraCreateTableInfo() = default;
    virtual inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const = 0;
};

struct BoundCreateTableInfo {
    common::TableType type;
    std::string tableName;
    std::unique_ptr<BoundExtraCreateTableInfo> extraInfo;

    BoundCreateTableInfo(common::TableType type, std::string tableName,
        std::unique_ptr<BoundExtraCreateTableInfo> extraInfo)
        : type{type}, tableName{std::move(tableName)}, extraInfo{std::move(extraInfo)} {}
    BoundCreateTableInfo(const BoundCreateTableInfo& other)
        : type{other.type}, tableName{other.tableName}, extraInfo{other.extraInfo->copy()} {}

    inline std::unique_ptr<BoundCreateTableInfo> copy() const {
        return std::make_unique<BoundCreateTableInfo>(*this);
    }

    static std::vector<std::unique_ptr<BoundCreateTableInfo>> copy(
        const std::vector<std::unique_ptr<BoundCreateTableInfo>>& infos);
};

struct BoundExtraCreateNodeTableInfo : public BoundExtraCreateTableInfo {
    common::property_id_t primaryKeyIdx;
    std::vector<std::unique_ptr<catalog::Property>> properties;

    BoundExtraCreateNodeTableInfo(common::property_id_t primaryKeyIdx,
        std::vector<std::unique_ptr<catalog::Property>> properties)
        : primaryKeyIdx{primaryKeyIdx}, properties{std::move(properties)} {}
    BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo& other)
        : primaryKeyIdx{other.primaryKeyIdx}, properties{
                                                  catalog::Property::copy(other.properties)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateNodeTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableInfo : public BoundExtraCreateTableInfo {
    catalog::RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    std::unique_ptr<common::LogicalType> srcPkDataType;
    std::unique_ptr<common::LogicalType> dstPkDataType;
    std::vector<std::unique_ptr<catalog::Property>> properties;

    BoundExtraCreateRelTableInfo(catalog::RelMultiplicity relMultiplicity,
        common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::unique_ptr<common::LogicalType> srcPkDataType,
        std::unique_ptr<common::LogicalType> dstPkDataType,
        std::vector<std::unique_ptr<catalog::Property>> properties)
        : relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPkDataType{std::move(srcPkDataType)}, dstPkDataType{std::move(dstPkDataType)},
          properties{std::move(properties)} {}
    BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo& other)
        : relMultiplicity{other.relMultiplicity}, srcTableID{other.srcTableID},
          dstTableID{other.dstTableID}, srcPkDataType{other.srcPkDataType->copy()},
          dstPkDataType{other.dstPkDataType->copy()}, properties{catalog::Property::copy(
                                                          other.properties)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRelTableInfo>(*this);
    }
};

struct BoundExtraCreateRelTableGroupInfo : public BoundExtraCreateTableInfo {
    std::vector<std::unique_ptr<BoundCreateTableInfo>> infos;

    BoundExtraCreateRelTableGroupInfo(std::vector<std::unique_ptr<BoundCreateTableInfo>> infos)
        : infos{std::move(infos)} {}
    BoundExtraCreateRelTableGroupInfo(const BoundExtraCreateRelTableGroupInfo& other)
        : infos{BoundCreateTableInfo::copy(other.infos)} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRelTableGroupInfo>(*this);
    }
};

struct BoundExtraCreateRdfGraphInfo : public BoundExtraCreateTableInfo {
    std::unique_ptr<BoundCreateTableInfo> nodeInfo;
    std::unique_ptr<BoundCreateTableInfo> relInfo;

    BoundExtraCreateRdfGraphInfo(std::unique_ptr<BoundCreateTableInfo> nodeInfo,
        std::unique_ptr<BoundCreateTableInfo> relInfo)
        : nodeInfo{std::move(nodeInfo)}, relInfo{std::move(relInfo)} {}
    BoundExtraCreateRdfGraphInfo(const BoundExtraCreateRdfGraphInfo& other)
        : nodeInfo{other.nodeInfo->copy()}, relInfo{other.relInfo->copy()} {}

    inline std::unique_ptr<BoundExtraCreateTableInfo> copy() const final {
        return std::make_unique<BoundExtraCreateRdfGraphInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
