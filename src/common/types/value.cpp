#include "common/types/value.h"

#include <utility>

namespace kuzu {
namespace common {

Value Value::createNullValue() {
    return {};
}

Value Value::createNullValue(DataType dataType) {
    return Value(std::move(dataType));
}

Value Value::createDefaultValue(const DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
        return Value(0);
    case BOOL:
        return Value(true);
    case DOUBLE:
        return Value(0.0);
    case DATE:
        return Value(date_t());
    case TIMESTAMP:
        return Value(timestamp_t());
    case INTERVAL:
        return Value(interval_t());
    case NODE_ID:
        return Value(nodeID_t());
    case STRING:
        return Value(string(""));
    case LIST:
        return Value(dataType, vector<unique_ptr<Value>>{});
    default:
        throw RuntimeException("Data type " + Types::dataTypeToString(dataType) +
                               " is not supported for Value::createDefaultValue");
    }
}

Value::Value(bool val_) : dataType{BOOL}, isNull_{false} {
    val.booleanVal = val_;
}

Value::Value(int32_t val_) : dataType{INT64}, isNull_{false} {
    val.int64Val = (int64_t)val_;
}

Value::Value(int64_t val_) : dataType{INT64}, isNull_{false} {
    val.int64Val = val_;
}

Value::Value(double val_) : dataType{DOUBLE}, isNull_{false} {
    val.doubleVal = val_;
}

Value::Value(date_t val_) : dataType{DATE}, isNull_{false} {
    val.dateVal = val_;
}

Value::Value(kuzu::common::timestamp_t val_) : dataType{TIMESTAMP}, isNull_{false} {
    val.timestampVal = val_;
}

Value::Value(kuzu::common::interval_t val_) : dataType{INTERVAL}, isNull_{false} {
    val.intervalVal = val_;
}

Value::Value(kuzu::common::nodeID_t val_) : dataType{NODE_ID}, isNull_{false} {
    val.nodeIDVal = val_;
}

Value::Value(const char* val_) : dataType{STRING}, isNull_{false} {
    strVal = string(val_);
}

Value::Value(const std::string& val_) : dataType{STRING}, isNull_{false} {
    strVal = val_;
}

Value::Value(DataType dataType, vector<unique_ptr<Value>> vals)
    : dataType{std::move(dataType)}, isNull_{false} {
    listVal = std::move(vals);
}

Value::Value(unique_ptr<NodeVal> val_) : dataType{NODE}, isNull_{false} {
    nodeVal = std::move(val_);
}

Value::Value(unique_ptr<RelVal> val_) : dataType{REL}, isNull_{false} {
    relVal = std::move(val_);
}

Value::Value(DataType dataType, const uint8_t* val_)
    : dataType{std::move(dataType)}, isNull_{false} {
    copyValueFrom(val_);
}

Value::Value(const Value& other) : dataType{other.dataType}, isNull_{other.isNull_} {
    copyValueFrom(other);
}

void Value::copyValueFrom(const uint8_t* value) {
    switch (dataType.typeID) {
    case INT64: {
        val.int64Val = *((int64_t*)value);
    } break;
    case BOOL: {
        val.booleanVal = *((bool*)value);
    } break;
    case DOUBLE: {
        val.doubleVal = *((double*)value);
    } break;
    case DATE: {
        val.dateVal = *((date_t*)value);
    } break;
    case TIMESTAMP: {
        val.timestampVal = *((timestamp_t*)value);
    } break;
    case INTERVAL: {
        val.intervalVal = *((interval_t*)value);
    } break;
    case NODE_ID: {
        val.nodeIDVal = *((nodeID_t*)value);
    } break;
    case STRING: {
        strVal = ((ku_string_t*)value)->getAsString();
    } break;
    case LIST: {
        listVal = convertKUListToVector(*(ku_list_t*)value);
    } break;
    default:
        throw RuntimeException(
            "Data type " + Types::dataTypeToString(dataType) + " is not supported for Value::set");
    }
}

void Value::copyValueFrom(const Value& other) {
    if (other.isNull()) {
        isNull_ = true;
        return;
    }
    isNull_ = false;
    assert(dataType == other.dataType);
    switch (dataType.typeID) {
    case BOOL: {
        val.booleanVal = other.val.booleanVal;
    } break;
    case INT64: {
        val.int64Val = other.val.int64Val;
    } break;
    case DOUBLE: {
        val.doubleVal = other.val.doubleVal;
    } break;
    case DATE: {
        val.dateVal = other.val.dateVal;
    } break;
    case TIMESTAMP: {
        val.timestampVal = other.val.timestampVal;
    } break;
    case INTERVAL: {
        val.intervalVal = other.val.intervalVal;
    } break;
    case NODE_ID: {
        val.nodeIDVal = other.val.nodeIDVal;
    } break;
    case STRING: {
        strVal = other.strVal;
    } break;
    case LIST: {
        for (auto& value : other.listVal) {
            listVal.push_back(value->copy());
        }
    } break;
    case NODE: {
        nodeVal = other.nodeVal->copy();
    } break;
    case REL: {
        relVal = other.relVal->copy();
    } break;
    default:
        throw NotImplementedException("Value::Value(const Value&) for type " +
                                      Types::dataTypeToString(dataType) + " is not implemented.");
    }
}

string Value::toString() const {
    if (isNull_) {
        return "";
    }
    switch (dataType.typeID) {
    case BOOL:
        return TypeUtils::toString(val.booleanVal);
    case INT64:
        return TypeUtils::toString(val.int64Val);
    case DOUBLE:
        return TypeUtils::toString(val.doubleVal);
    case DATE:
        return TypeUtils::toString(val.dateVal);
    case TIMESTAMP:
        return TypeUtils::toString(val.timestampVal);
    case INTERVAL:
        return TypeUtils::toString(val.intervalVal);
    case NODE_ID:
        return TypeUtils::toString(val.nodeIDVal);
    case STRING:
        return strVal;
    case LIST: {
        string result = "[";
        for (auto i = 0u; i < listVal.size(); ++i) {
            result += listVal[i]->toString();
            result += (i == listVal.size() - 1 ? "]" : ",");
        }
        return result;
    }
    case NODE:
        return nodeVal->toString();
    case REL:
        return relVal->toString();
    default:
        throw NotImplementedException("Value::toString for type " +
                                      Types::dataTypeToString(dataType) + " is not implemented.");
    }
}

void Value::validateType(const DataType& type) const {
    if (type != dataType) {
        throw RuntimeException(
            StringUtils::string_format("Cannot get %s value from the %s result value.",
                Types::dataTypeToString(type).c_str(), Types::dataTypeToString(dataType).c_str()));
    }
}

vector<unique_ptr<Value>> Value::convertKUListToVector(ku_list_t& list) const {
    vector<unique_ptr<Value>> listResultValue;
    auto numBytesPerElement = Types::getDataTypeSize(*dataType.childType);
    for (auto i = 0; i < list.size; i++) {
        auto childValue = make_unique<Value>(Value::createDefaultValue(*dataType.childType));
        childValue->copyValueFrom(
            reinterpret_cast<uint8_t*>(list.overflowPtr + i * numBytesPerElement));
        listResultValue.emplace_back(std::move(childValue));
    }
    return listResultValue;
}

static std::string propertiesToString(
    const vector<pair<std::string, unique_ptr<Value>>>& properties) {
    std::string result = "{";
    for (auto i = 0u; i < properties.size(); ++i) {
        auto& [name, value] = properties[i];
        result += name + ":" + value->toString();
        result += (i == properties.size() - 1 ? "" : ", ");
    }
    result += "}";
    return result;
}

NodeVal::NodeVal(const NodeVal& other) {
    idVal = other.idVal->copy();
    labelVal = other.labelVal->copy();
    for (auto& [key, val] : other.properties) {
        addProperty(key, val->copy());
    }
}

string NodeVal::toString() const {
    std::string result = "(";
    result += idVal->toString();
    result += ":" + labelVal->toString() + " ";
    result += propertiesToString(properties);
    result += ")";
    return result;
}

RelVal::RelVal(const RelVal& other) {
    srcNodeIDVal = other.srcNodeIDVal->copy();
    dstNodeIDVal = other.dstNodeIDVal->copy();
    for (auto& [key, val] : other.properties) {
        addProperty(key, val->copy());
    }
}

string RelVal::toString() const {
    std::string result;
    result += "(" + srcNodeIDVal->toString() + ")";
    result += "-[" + propertiesToString(properties) + "]->";
    result += "(" + dstNodeIDVal->toString() + ")";
    return result;
}

} // namespace common
} // namespace kuzu
