add_library(kuzu_function_algorithm
        OBJECT
        ife_morsel.cpp
        sp_ife_morsel.cpp
        msbfs_ife_morsel.cpp
        asp_ife_morsel.cpp
        varlen_ife_morsel.cpp
        gds.cpp
        gds_task.cpp
        page_rank.cpp
        parallel_utils.cpp
        parallel_sp_length.cpp
        parallel_msbfs_length.cpp
        parallel_sp_path.cpp
        parallel_asp_length.cpp
        parallel_asp_path.cpp
        parallel_varlen_length.cpp
        parallel_varlen_path.cpp
        shortest_paths.cpp
        gds_utils.cpp
        weakly_connected_components.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_algorithm>
        PARENT_SCOPE)
