INCLUDE(ExternalProject)

SET(HDFS_SOURCES_DIR ${THIRD_PARTY_PATH}/pivotalrd-libhdfs3)
SET(HDFS_INSTALL_DIR ${HDFS_SOURCES_DIR}/build/output)
SET(HDFS_INCLUDE_DIR "${HDFS_INSTALL_DIR}/include" CACHE PATH "hdfs include directory." FORCE)

ExternalProject_Add(
    extern_hdfs
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${HDFS_SOURCES_DIR} && git clone https://github.com/acmol/pivotalrd-libhdfs3.git
    CONFIGURE_COMMAND cd ${HDFS_SOURCES_DIR} && mkdir -p build && cd build && ../bootstrap --prefix=${HDFS_INSTALL_DIR} --dependency=${UUID_INSTALL_DIR}:${PROTOBUF_INSTALL_DIR}:${LIBGSASL_INSTALL_DIR}:${LIBXML2_INSTALL_DIR}:${KRB_INSTALL_DIR}
    BUILD_COMMAND cd ${HDFS_SOURCES_DIR}/build && make -j 6
    INSTALL_COMMAND cd ${HDFS_SOURCES_DIR}/build && make install
    )

include_directories(${HDFS_INCLUDE_DIR})
ADD_LIBRARY(hdfs STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET hdfs PROPERTY IMPORTED_LOCATION ${HDFS_INSTALL_DIR}/lib/libhdfs3.a)
ADD_DEPENDENCIES(extern_hdfs boost krb uuid libgsasl libxml2 protobuf)
ADD_DEPENDENCIES(hdfs extern_hdfs)

LIST(APPEND external_project_dependencies hdfs)
