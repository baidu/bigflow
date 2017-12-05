INCLUDE(ExternalProject)

SET(BRPC_SOURCES_DIR ${THIRD_PARTY_PATH}/brpc)
#SET(BRPC_INSTALL_DIR ${THIRD_PARTY_PATH})
SET(BRPC_INCLUDE_DIR "${BRPC_SOURCES_DIR}/output/include" CACHE PATH "brpc include directory." FORCE)


ExternalProject_Add(
    extern_brpc
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${THIRD_PARTY_PATH}/brpc && git clone https://github.com/acmol/brpc.git --depth=1
    CONFIGURE_COMMAND cd ${BRPC_SOURCES_DIR} && PATH="${PROTOBUF_INSTALL_DIR}/bin:$ENV{PATH}" sh config_brpc.sh --with-glog --headers=${THIRD_PARTY_PATH}/include --libs=${THIRD_PARTY_PATH}/lib 
    BUILD_COMMAND cd ${THIRD_PARTY_PATH}/brpc && make -j 8 CC='ccache gcc'
    INSTALL_COMMAND rm -rf ${THIRD_PARTY_PATH}/include/brpc && cp -r ${BRPC_SOURCES_DIR}/src ${THIRD_PARTY_PATH}/include/brpc && cp ${BRPC_SOURCES_DIR}/libbrpc.a ${THIRD_PARTY_PATH}/lib/
)

add_dependencies(extern_brpc protobuf leveldb gflags glog gtest)

set(BRPC_LIBRARIES "${THIRD_PARTY_PATH}/lib/libbrpc.a" CACHE FILEPATH "BRPC_LIBRARIES" FORCE)
ADD_LIBRARY(brpc STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET brpc PROPERTY IMPORTED_LOCATION ${BRPC_LIBRARIES})
ADD_DEPENDENCIES(brpc extern_brpc)

include_directories(${BRPC_INCLUDE_DIR})

LIST(APPEND external_project_dependencies brpc)

