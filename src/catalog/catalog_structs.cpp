#include "catalog/catalog_structs.h"

#include "common/exception.h"
#include "common/utils.h"

namespace kuzu {
namespace catalog {

RelMultiplicity getRelMultiplicityFromString(const string& relMultiplicityString) {
    if ("ONE_ONE" == relMultiplicityString) {
        return ONE_ONE;
    } else if ("MANY_ONE" == relMultiplicityString) {
        return MANY_ONE;
    } else if ("ONE_MANY" == relMultiplicityString) {
        return ONE_MANY;
    } else if ("MANY_MANY" == relMultiplicityString) {
        return MANY_MANY;
    }
    throw CatalogException("Invalid relMultiplicity string '" + relMultiplicityString + "'.");
}

string getRelMultiplicityAsString(RelMultiplicity relMultiplicity) {
    switch (relMultiplicity) {
    case MANY_MANY: {
        return "MANY_MANY";
    }
    case MANY_ONE: {
        return "MANY_ONE";
    }
    case ONE_ONE: {
        return "ONE_ONE";
    }
    case ONE_MANY: {
        return "ONE_MANY";
    }
    default:
        throw CatalogException("Cannot convert rel multiplicity to string.");
    }
}

string TableSchema::getPropertyName(property_id_t propertyID) const {
    for (auto& property : properties) {
        if (property.propertyID == propertyID) {
            return property.name;
        }
    }
    throw RuntimeException(StringUtils::string_format(
        "Table: %s doesn't have a property with propertyID=%d.", tableName.c_str(), propertyID));
}

property_id_t TableSchema::getPropertyID(string propertyName) const {
    for (auto& property : properties) {
        if (property.name == propertyName) {
            return property.propertyID;
        }
    }
    throw RuntimeException(
        StringUtils::string_format("Table: %s doesn't have a property with propertyName=%s.",
            tableName.c_str(), propertyName.c_str()));
}

Property TableSchema::getProperty(property_id_t propertyID) const {
    for (auto& property : properties) {
        if (property.propertyID == propertyID) {
            return property;
        }
    }
    throw RuntimeException(StringUtils::string_format(
        "Table: %s doesn't have a property with propertyID=%d.", tableName.c_str(), propertyID));
}

void TableSchema::renameProperty(property_id_t propertyID, const string& newName) {
    for (auto& property : properties) {
        if (property.propertyID == propertyID) {
            property.name = newName;
            return;
        }
    }
    throw InternalException("Property with id=" + to_string(propertyID) + " not found.");
}

} // namespace catalog
} // namespace kuzu
