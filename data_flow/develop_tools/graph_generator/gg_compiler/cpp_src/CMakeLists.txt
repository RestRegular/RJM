cmake_minimum_required(VERSION 4.0)
project(gg)

set(CMAKE_CXX_STANDARD 20)

add_executable(gg src/main.cpp
        include/analyzer/rcc_lexer.h
        include/gg_core.h
        include/gg_base.h
        src/gg_base.cpp
        src/gg_core.cpp
        include/lib/rcc_utils.h
        src/lib/rcc_utils.cpp
        include/lib/newrcc.h
        src/lib/newrcc.cpp
        src/analyzer/rcc_lexer.cpp
        src/lib/RJson/RJson.cpp
        src/lib/RJson/RJson_error.cpp
        src/lib/RJson/RJson.cpp
        src/lib/RJson/RJson_error.cpp
        include/components/gg_model.h
        src/components/gg_model.cpp
        include/parser/gg_parser.h
        src/parser/gg_parser.cpp
        include/components/gg_manager.h
        src/components/gg_manager.cpp
        include/test.h
        src/test.cpp
)
