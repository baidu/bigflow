INCLUDE(ExternalProject)

SET(LIBEV_SOURCES_DIR ${THIRD_PARTY_PATH}/libev)
#SET(LIBEV_INSTALL_DIR ${THIRD_PARTY_PATH})
SET(LIBEV_INCLUDE_DIR "${LIBEV_SOURCES_DIR}/src" CACHE PATH "gflags include directory." FORCE)

ExternalProject_Add(
    extern_libev
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${THIRD_PARTY_PATH}/libev && git clone https://github.com/enki/libev.git
    CONFIGURE_COMMAND cd ${THIRD_PARTY_PATH}/libev && ./configure
    BUILD_COMMAND cd ${THIRD_PARTY_PATH}/libev && make -j 8
    INSTALL_COMMAND cp -r ${THIRD_PARTY_PATH}/libev ${THIRD_PARTY_PATH}/include/libev || echo ""
    )

ADD_LIBRARY(libev STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET libev PROPERTY IMPORTED_LOCATION ${LIBEV_SOURCES_DIR}/.libs/libev.a)
ADD_DEPENDENCIES(libev extern_libev)

LIST(APPEND external_project_dependencies libev)
