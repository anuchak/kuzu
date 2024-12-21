add_library(kuzu_processor_operator_ver_length_extend
        OBJECT
        bfs_state.cpp
        shortest_path_state.cpp
        all_shortest_path_state.cpp
        variable_length_path_state.cpp
        frontier.cpp
        frontier_scanner.cpp
        recursive_join.cpp
        path_property_probe.cpp
        morsel_dispatcher.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ver_length_extend>
        PARENT_SCOPE)
