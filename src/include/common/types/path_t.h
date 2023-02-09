#include <vector>
#include "common/types/internal_id_t.h"

namespace kuzu {
namespace common {

struct ku_path_t {
    std::vector <internalID_t> path;

public:
    void setPathInternalID(internalID_t internalID);
    std::vector<internalID_t> getPath();
};
}
}
