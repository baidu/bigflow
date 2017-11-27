INCLUDE(ExternalProject)

SET(CTEMPLATE_SOURCES_DIR ${THIRD_PARTY_PATH}/ctemplate)

ExternalProject_Add(
    extern_ctemplate
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${CTEMPLATE_SOURCES_DIR} && git clone https://github.com/OlafvdSpek/ctemplate.git
    CONFIGURE_COMMAND cd ${CTEMPLATE_SOURCES_DIR} && ./autogen.sh && CPPFLAGS=-fPIC  CXXFLAGS=-std=c++11 ./configure
    BUILD_COMMAND cd ${CTEMPLATE_SOURCES_DIR} && make -j 8
    INSTALL_COMMAND echo ""
    )

ADD_LIBRARY(ctemplate STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET ctemplate PROPERTY IMPORTED_LOCATION ${CTEMPLATE_SOURCES_DIR}/.libs/libctemplate.a)
include_directories(${CTEMPLATE_SOURCES_DIR}/src)
ADD_DEPENDENCIES(ctemplate extern_ctemplate)

LIST(APPEND external_project_dependencies ctemplate)




