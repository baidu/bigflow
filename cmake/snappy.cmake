INCLUDE(ExternalProject)

SET(SNAPPY_SOURCES_DIR ${THIRD_PARTY_PATH}/snappy)
#SET(SNAPPY_INSTALL_DIR ${THIRD_PARTY_PATH})
#SET(SNAPPY_INCLUDE_DIR "${SNAPPY_SOURCES_DIR}/src" CACHE PATH "gflags include directory." FORCE)

ExternalProject_Add(
    extern_snappy
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${SNAPPY_SOURCES_DIR} && git clone https://github.com/google/snappy.git
    CONFIGURE_COMMAND cd ${THIRD_PARTY_PATH}/snappy && cmake -DSNAPPY_BUILD_TESTS=OFF .
    BUILD_COMMAND cd ${THIRD_PARTY_PATH}/snappy && make -j 8
    INSTALL_COMMAND  rm -rf ${THIRD_PARTY_PATH}/include/snappy && cp -r ${THIRD_PARTY_PATH}/snappy ${THIRD_PARTY_PATH}/include
    )

ADD_LIBRARY(snappy STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION ${SNAPPY_SOURCES_DIR}/libsnappy.a)
ADD_DEPENDENCIES(snappy extern_snappy)

LIST(APPEND external_project_dependencies snappy)
