#include "common/types/internal_id_t.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

using namespace common;

struct PathCreation {

    static inline void operation(ku_list_t& input, ku_path_t& result) {
        for (int i = 0; i < input.size; i++) {
            auto nodeOrRelID = ((internalID_t*)(input.overflowPtr))[i];
            result.getPath().push_back(nodeOrRelID);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu