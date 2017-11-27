INCLUDE(ExternalProject)

SET(LIBGSASL_SOURCES_DIR ${THIRD_PARTY_PATH}/libgsasl-1.8.0)
SET(LIBGSASL_INSTALL_DIR ${LIBGSASL_SOURCES_DIR}/output)
SET(LIBGSASL_INCLUDE_DIR "${LIBGSASL_INSTALL_DIR}/include" CACHE PATH "libgsasl include directory." FORCE)


ExternalProject_Add(
    extern_libgsasl
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${LIBGSASL_SOURCES_DIR} && wget http://ftp.gnu.org/gnu/gsasl/libgsasl-1.8.0.tar.gz -O libgsasl-1.8.0.tar.gz && tar xzf libgsasl-1.8.0.tar.gz
    CONFIGURE_COMMAND  cd ${LIBGSASL_SOURCES_DIR} && CPPFLAGS=-fPIC ./configure --prefix=${LIBGSASL_INSTALL_DIR} --enable-shared=yes --enable-static=yes
    BUILD_COMMAND cd ${LIBGSASL_SOURCES_DIR} && make -j 8
    INSTALL_COMMAND cd ${LIBGSASL_SOURCES_DIR} && make install
)

set(LIBGSASL_LIBRARIES "${LIBGSASL_INSTALL_DIR}/lib/libgsasl.a" CACHE FILEPATH "LIBGSASL_LIBRARIES" FORCE)
ADD_LIBRARY(libgsasl STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET libgsasl PROPERTY IMPORTED_LOCATION ${LIBGSASL_LIBRARIES})
ADD_DEPENDENCIES(libgsasl extern_libgsasl)

include_directories(${LIBGSASL_INCLUDE_DIR})

LIST(APPEND external_project_dependencies libgsasl)

