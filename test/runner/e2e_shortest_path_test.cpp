#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;

class SingleSourceShortestPathTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/shortest-path-tests/");
    }
};

TEST_F(SingleSourceShortestPathTest, BFS_SSSP) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/shortest_path/bfs_sssp.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/shortest_path/bfs_sssp_large.test"));
}
