namespace kuzu {
namespace function {
namespace operation {

struct PathLength {

    static inline void operation(common::ku_list_t& input, int64_t& result) { result = input.size; }
};
} // namespace operation
} // namespace function
} // namespace kuzu