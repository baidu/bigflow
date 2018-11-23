INCLUDE(ExternalProject)

SET(LEVELDB_SOURCES_DIR ${THIRD_PARTY_PATH}/leveldb)

ExternalProject_Add(
    extern_leveldb
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${THIRD_PARTY_PATH}/leveldb && git clone https://github.com/google/leveldb.git
    CONFIGURE_COMMAND cd ${THIRD_PARTY_PATH}/leveldb && rm -fr build && mkdir build && cd build && CXXFLAGS=-fPIC cmake ..
    BUILD_COMMAND cd ${THIRD_PARTY_PATH}/leveldb/build && CC=gcc CXX=g++ CXXFLAGS=-fPIC make -j 8
    INSTALL_COMMAND cp -r ${THIRD_PARTY_PATH}/leveldb/include/leveldb ${THIRD_PARTY_PATH}/include/ && cp ${LEVELDB_SOURCES_DIR}/build/libleveldb.a ${THIRD_PARTY_PATH}/lib
    )

ADD_LIBRARY(leveldb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET leveldb PROPERTY IMPORTED_LOCATION ${THIRD_PARTY_PATH}/lib/libleveldb.a)
ADD_DEPENDENCIES(leveldb extern_leveldb)

LIST(APPEND external_project_dependencies leveldb)

