INCLUDE(ExternalProject)

SET(LIBXML2_SOURCES_DIR ${THIRD_PARTY_PATH}/libxml2)
SET(LIBXML2_INSTALL_DIR ${LIBXML2_SOURCES_DIR}/output)
SET(LIBXML2_INCLUDE_DIR "${LIBXML2_INSTALL_DIR}/include" CACHE PATH "gflags include directory." FORCE)

ExternalProject_Add(
    extern_libxml2
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${LIBXML2_SOURCES_DIR} && git clone git://git.gnome.org/libxml2 --depth=1
    CONFIGURE_COMMAND cd ${LIBXML2_SOURCES_DIR} && autoreconf -fiv && CPPFLAGS=-fPIC ./configure --with-python=no --prefix=${LIBXML2_INSTALL_DIR}
    BUILD_COMMAND cd ${LIBXML2_SOURCES_DIR} && make -j 8
    INSTALL_COMMAND cd ${LIBXML2_SOURCES_DIR} && make install
    )

ADD_LIBRARY(libxml2 STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET libxml2 PROPERTY IMPORTED_LOCATION ${LIBXML2_INSTALL_DIR}/lib/libxml2.a)
ADD_DEPENDENCIES(libxml2 extern_libxml2)

LIST(APPEND external_project_dependencies libxml2)
