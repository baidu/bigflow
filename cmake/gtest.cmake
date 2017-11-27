
IF(WITH_TESTING)
    ENABLE_TESTING()
    INCLUDE(ExternalProject)

    SET(GTEST_SOURCES_DIR ${THIRD_PARTY_PATH}/gtest)
    SET(GTEST_INSTALL_DIR ${THIRD_PARTY_PATH})
    SET(GTEST_INCLUDE_DIR "${GTEST_INSTALL_DIR}/include" CACHE PATH "gtest include directory." FORCE)

    INCLUDE_DIRECTORIES(${GTEST_INCLUDE_DIR})

    IF(WIN32)
        set(GTEST_LIBRARIES
            "${GTEST_INSTALL_DIR}/lib/gtest.lib" CACHE FILEPATH "gtest libraries." FORCE)
        set(GTEST_MAIN_LIBRARIES
            "${GTEST_INSTALL_DIR}/lib/gtest_main.lib" CACHE FILEPATH "gtest main libraries." FORCE)
    ELSE(WIN32)
        set(GTEST_LIBRARIES
            "${GTEST_INSTALL_DIR}/lib/libgtest.a" CACHE FILEPATH "gtest libraries." FORCE)
        set(GTEST_MAIN_LIBRARIES
            "${GTEST_INSTALL_DIR}/lib/libgtest_main.a" CACHE FILEPATH "gtest main libraries." FORCE)
        set(GMOCK_LIBRARIES
            "${GTEST_INSTALL_DIR}/lib/libgmock.a" CACHE FILEPATH "gmock libraries." FORCE)
    ENDIF(WIN32)

    ExternalProject_Add(
        extern_gtest
        ${EXTERNAL_PROJECT_LOG_ARGS}
        GIT_REPOSITORY  "https://github.com/google/googletest.git"
        GIT_TAG         "release-1.8.0"
        PREFIX          ${GTEST_SOURCES_DIR}
        UPDATE_COMMAND  ""
        CMAKE_ARGS      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        CMAKE_ARGS      -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        CMAKE_ARGS      -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        CMAKE_ARGS      -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        CMAKE_ARGS      -DCMAKE_INSTALL_PREFIX=${GTEST_INSTALL_DIR}
        CMAKE_ARGS      -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        CMAKE_ARGS      -DBUILD_GMOCK=ON
        CMAKE_ARGS      -Dgtest_disable_pthreads=OFF
        CMAKE_ARGS      -Dgtest_force_shared_crt=ON
        CMAKE_ARGS      -DCMAKE_BUILD_TYPE=Release
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${GTEST_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=Release
        )

    ADD_LIBRARY(gtest STATIC IMPORTED GLOBAL)
    SET_PROPERTY(TARGET gtest PROPERTY IMPORTED_LOCATION ${GTEST_LIBRARIES})
    ADD_DEPENDENCIES(gtest extern_gtest)

    ADD_LIBRARY(gtest_main STATIC IMPORTED GLOBAL)
    SET_PROPERTY(TARGET gtest_main PROPERTY IMPORTED_LOCATION ${GTEST_MAIN_LIBRARIES})
    ADD_DEPENDENCIES(gtest_main extern_gtest)

    ADD_LIBRARY(gmock STATIC IMPORTED GLOBAL)
    SET_PROPERTY(TARGET gmock PROPERTY IMPORTED_LOCATION ${GMOCK_LIBRARIES})
    ADD_DEPENDENCIES(gmock extern_gtest)

    LIST(APPEND external_project_dependencies gmock)
ENDIF(WITH_TESTING)
