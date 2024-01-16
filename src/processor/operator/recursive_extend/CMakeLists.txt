add_library(kuzu_processor_operator_ver_length_extend
        OBJECT
        all_shortest_path_state.cpp
        bfs_scheduler.cpp
        bfs_state.cpp
        frontier.cpp
        frontier_scanner.cpp
        recursive_join.cpp
        path_property_probe.cpp
        scan_frontier.cpp
        variable_length_state.cpp
        weighted_shortest_path_state.cpp)

set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -latomic")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ver_length_extend>
        PARENT_SCOPE)
