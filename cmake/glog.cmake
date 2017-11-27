INCLUDE(ExternalProject)

SET(GLOG_SOURCES_DIR ${THIRD_PARTY_PATH}/glog)
#SET(GLOG_INSTALL_DIR ${THIRD_PARTY_PATH})
SET(GLOG_INCLUDE_DIR "${GLOG_SOURCES_DIR}/src" CACHE PATH "gflags include directory." FORCE)

ExternalProject_Add(
    extern_glog
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf  ${GLOG_SOURCES_DIR} && git clone https://github.com/google/glog.git
    #CONFIGURE_COMMAND cd ${GLOG_SOURCES_DIR} && git checkout v3.4.0 && ./autogen.sh && CXXFLAGS=-fPIC ./configure
    CONFIGURE_COMMAND cd ${GLOG_SOURCES_DIR} && ./autogen.sh && CXXFLAGS=-fPIC ./configure --with-gflags=${THIRD_PARTY_PATH}
    BUILD_COMMAND cd ${GLOG_SOURCES_DIR} && make -j 4
    INSTALL_COMMAND cp ${GLOG_SOURCES_DIR}/.libs/libglog.a ${THIRD_PARTY_PATH}/lib/ && cp -r ${GLOG_SOURCES_DIR}/src/glog ${THIRD_PARTY_PATH}/include/
)

ADD_LIBRARY(glog STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET glog PROPERTY IMPORTED_LOCATION ${THIRD_PARTY_PATH}/lib/libglog.a)
ADD_DEPENDENCIES(extern_glog gflags)
ADD_DEPENDENCIES(glog extern_glog)

LIST(APPEND external_project_dependencies glog)
