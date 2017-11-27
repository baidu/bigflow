
INCLUDE(ExternalProject)

SET(STRINGENCODERS_SOURCE_DIR ${THIRD_PARTY_PATH}/stringencoders)
# use src instead of output/include as stringencode don't include extern_c_begin.h, which causes problem
SET(STRINGENCODERS_INCLUDE_DIR "${STRINGENCODERS_SOURCE_DIR}/src" CACHE PATH "gflags include directory." FORCE)

ExternalProject_Add(
        extern_stringencoders
        DOWNLOAD_DIR ${THIRD_PARTY_PATH}
        DOWNLOAD_COMMAND rm -rf ${STRINGENCODERS_SOURCE_DIR} && git clone https://github.com/client9/stringencoders.git --depth=1
        CONFIGURE_COMMAND cd ${STRINGENCODERS_SOURCE_DIR} &&  ./bootstrap.sh && ./configure --prefix=${STRINGENCODERS_SOURCE_DIR}/output
        BUILD_COMMAND cd ${STRINGENCODERS_SOURCE_DIR} && make -j 8
        INSTALL_COMMAND cd ${STRINGENCODERS_SOURCE_DIR} && make install
)

ADD_LIBRARY(stringencoders STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET stringencoders PROPERTY IMPORTED_LOCATION ${STRINGENCODERS_SOURCE_DIR}/output/lib/libmodpbase64.a)
ADD_DEPENDENCIES(stringencoders  extern_stringencoders)

include_directories(${STRINGENCODERS_INCLUDE_DIR})
LIST(APPEND external_project_dependencies stringencoders)
