
INCLUDE(ExternalProject)

SET(CITYHASH_SOURCES_DIR ${THIRD_PARTY_PATH}/cityhash)
#SET(CITYHASH_INSTALL_DIR ${THIRD_PARTY_PATH})
SET(CITYHASH_INCLUDE_DIR "${CITYHASH_SOURCES_DIR}/src" CACHE PATH "gflags include directory." FORCE)

ExternalProject_Add(
    extern_cityhash
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${THIRD_PARTY_PATH}/cityhash && git clone https://github.com/google/cityhash.git
    CONFIGURE_COMMAND cd ${THIRD_PARTY_PATH}/cityhash && CPPFLAGS=-fPIC CXXFLAGS=-fPIC ./configure
    BUILD_COMMAND cd ${THIRD_PARTY_PATH}/cityhash && make -j 8
    INSTALL_COMMAND rm -rf ${THIRD_PARTY_PATH}/include/cityhash && cp -r ${THIRD_PARTY_PATH}/cityhash/src ${THIRD_PARTY_PATH}/include/cityhash
    )

ADD_LIBRARY(cityhash STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET cityhash PROPERTY IMPORTED_LOCATION ${CITYHASH_SOURCES_DIR}/src/.libs/libcityhash.a)
ADD_DEPENDENCIES(cityhash extern_cityhash)

LIST(APPEND external_project_dependencies cityhash)
