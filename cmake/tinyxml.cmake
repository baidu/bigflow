INCLUDE(ExternalProject)

SET(TINYXML_SOURCES_DIR ${THIRD_PARTY_PATH}/tinyxml)

ExternalProject_Add(
    extern_tinyxml
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${THIRD_PARTY_PATH}/tinyxml && git clone -b 5.0.1 https://github.com/leethomason/tinyxml2.git && mv tinyxml2 tinyxml
    CONFIGURE_COMMAND echo ""
    BUILD_COMMAND cd ${THIRD_PARTY_PATH}/tinyxml && make -j 8
    INSTALL_COMMAND echo ""
    )

ADD_LIBRARY(tinyxml STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET tinyxml PROPERTY IMPORTED_LOCATION ${TINYXML_SOURCES_DIR}/libtinyxml2.a)
include_directories(${EXTERNAL_INSTALL_LOCATION}/tinyxml)
ADD_DEPENDENCIES(tinyxml extern_tinyxml)

LIST(APPEND external_project_dependencies tinyxml)



