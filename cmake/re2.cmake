
INCLUDE(ExternalProject)

SET(RE2_SOURCES_DIR ${THIRD_PARTY_PATH}/re2)

ExternalProject_Add(
    extern_re2
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${THIRD_PARTY_PATH}/re2 && git clone https://github.com/google/re2.git
    CONFIGURE_COMMAND echo ""
    BUILD_COMMAND cd ${THIRD_PARTY_PATH}/re2 && CXXFLAGS=-fPIC make -j 4
    INSTALL_COMMAND cp -r ${THIRD_PARTY_PATH}/re2/re2 ${THIRD_PARTY_PATH}/include/
    )

ADD_LIBRARY(re2 STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET re2 PROPERTY IMPORTED_LOCATION ${RE2_SOURCES_DIR}/obj/libre2.a)
ADD_DEPENDENCIES(re2 extern_re2)

LIST(APPEND external_project_dependencies re2)


