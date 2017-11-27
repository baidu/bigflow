INCLUDE(ExternalProject)

SET(UUID_SOURCES_DIR ${THIRD_PARTY_PATH}/libuuid-1.0.3)
SET(UUID_INSTALL_DIR ${UUID_SOURCES_DIR}/output)
SET(UUID_INCLUDE_DIR "${UUID_INSTALL_DIR}/output/include" CACHE PATH "uuid include directory." FORCE)


ExternalProject_Add(
    extern_uuid
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${UUID_SOURCES_DIR} && wget https://jaist.dl.sourceforge.net/project/libuuid/libuuid-1.0.3.tar.gz -O libuuid-1.0.3.tar.gz && tar xzf libuuid-1.0.3.tar.gz
    CONFIGURE_COMMAND  cd ${UUID_SOURCES_DIR} && CPPFLAGS=-fPIC ./configure --prefix=${UUID_INSTALL_DIR}
    BUILD_COMMAND cd ${UUID_SOURCES_DIR} && make -j 8
    INSTALL_COMMAND cd ${UUID_SOURCES_DIR} && make install && cp -rf ${UUID_INSTALL_DIR} ${LIBGSASL_SOURCES_DIR}/
)

set(UUID_LIBRARIES "${UUID_INSTALL_DIR}/lib/libuuid.a" CACHE FILEPATH "UUID_LIBRARIES" FORCE)
ADD_LIBRARY(uuid STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET uuid PROPERTY IMPORTED_LOCATION ${UUID_LIBRARIES})
ADD_DEPENDENCIES(uuid extern_uuid)
ADD_DEPENDENCIES(extern_uuid libxml2)

include_directories(${UUID_INCLUDE_DIR})

LIST(APPEND external_project_dependencies uuid)

