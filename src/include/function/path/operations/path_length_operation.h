namespace kuzu {
namespace function {
namespace operation {

struct PathLength {

    static inline void operation(common::ku_path_t& input, int64_t& result) {
        result = input.getPath().size();
    }
};
} // namespace operation
} // namespace function
} // namespace kuzu