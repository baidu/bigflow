
INCLUDE(ExternalProject)

SET(BOOST_SOURCES_DIR ${THIRD_PARTY_PATH}/boost_1_64_0)
SET(BOOST_INCLUDE_DIR "${BOOST_SOURCES_DIR}/" CACHE PATH "boost include directory." FORCE)

ExternalProject_Add(
    extern_boost
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND  wget https://dl.bintray.com/boostorg/release/1.64.0/source/boost_1_64_0.tar.gz -O boost_1_64_0.tar.gz
    CONFIGURE_COMMAND cd ${THIRD_PARTY_PATH}/ && tar xzf boost_1_64_0.tar.gz && cd boost_1_64_0 &&
        ./bootstrap.sh --with-libraries=system,regex,filesystem,python,test,random --with-python=${PYTHON_CMD}  --with-python-root=${PYTHON_INSTALL_DIR} --with-python-version=2.7
    BUILD_COMMAND cd ${THIRD_PARTY_PATH}/boost_1_64_0 && ./b2 cxxflags=-fPIC --with-system --with-regex --with-filesystem --with-test --with-random --with-python include=${PYTHON_INCLUDE_DIR}/python2.7
    INSTALL_COMMAND rm -rf ./include/boost && cd  ${THIRD_PARTY_PATH} && cp -r boost_1_64_0/boost ./include/
    )

ADD_LIBRARY(boost_system STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET boost_system PROPERTY IMPORTED_LOCATION "${BOOST_SOURCES_DIR}/stage/lib/libboost_system.a")

ADD_LIBRARY(boost_filesystem STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET boost_filesystem PROPERTY IMPORTED_LOCATION "${BOOST_SOURCES_DIR}/stage/lib/libboost_filesystem.a")

ADD_LIBRARY(boost_regex STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET boost_regex PROPERTY IMPORTED_LOCATION "${BOOST_SOURCES_DIR}/stage/lib/libboost_regex.a")

ADD_LIBRARY(boost_python STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET boost_python PROPERTY IMPORTED_LOCATION "${BOOST_SOURCES_DIR}/stage/lib/libboost_python.a")

ADD_LIBRARY(boost_random STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET boost_random PROPERTY IMPORTED_LOCATION "${BOOST_SOURCES_DIR}/stage/lib/libboost_random.a")

merge_static_libs(boost boost_system boost_filesystem boost_python boost_regex boost_random)

#SET(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} "${BOOST_SOURCES_DIR}")
ADD_DEPENDENCIES(boost extern_boost)
ADD_DEPENDENCIES(extern_boost extern_python)

LIST(APPEND external_project_dependencies boost)

