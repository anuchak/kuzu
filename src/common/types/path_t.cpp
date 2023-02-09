#pragma once

#include "common/types/path_t.h"

namespace kuzu {
namespace common {

std::vector<internalID_t> common::ku_path_t::getPath() {
    return path;
}

void common::ku_path_t::setPathInternalID(internalID_t internalID) {
    path.push_back(internalID);
}

}
}



