INCLUDE(ExternalProject)

SET(THRIFT_SOURCES_DIR ${THIRD_PARTY_PATH}/thrift)
SET(THRIFT_INSTALL_DIR ${THRIFT_SOURCES_DIR}/output)
SET(THRIFT_INCLUDE_DIR "${THRIFT_INSTALL_DIR}/include/" CACHE PATH "Thrift include directory." FORCE)

ExternalProject_Add(
    extern_thrift
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${THRIFT_SOURCES_DIR} && git clone git@github.com:apache/thrift.git
    # 0.9.1 cannot be built on mac, we have to choose 0.9.3, which requires a high version of bison
    # todo: find another way to specify /usr/local/opt/bison/bin, but it shoudn't affect linux's build
    # bison is also known as byacc
    CONFIGURE_COMMAND  cd ${THRIFT_SOURCES_DIR} &&
        git checkout 0.9.3 &&
        export PATH=/usr/local/opt/bison/bin/:$ENV{PATH} &&
        ./bootstrap.sh &&
        CPPFLAGS=-I${THIRD_PARTY_PATH}/include BOOST_LDFLAGS=-L${THIRD_PARTY_PATH}/boost_1_64_0/stage/lib WITH_TESTS_FALSE=yes CXXFLAGS=-fPIC CFLAGS=-fPIC ./configure --with-cpp=yes --with-openssl=/usr/local/opt/openssl --with-php=no --with-python=no --prefix=${THRIFT_INSTALL_DIR}
    BUILD_COMMAND cd ${THRIFT_SOURCES_DIR} && make -j 8
    INSTALL_COMMAND cd ${THRIFT_SOURCES_DIR} && make install
)

ADD_LIBRARY(thrift STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET thrift PROPERTY IMPORTED_LOCATION ${THRIFT_INSTALL_DIR}/lib/libthrift.a)

include_directories(${THRIFT_INCLUDE_DIR})
ADD_DEPENDENCIES(extern_thrift glog gflags protobuf boost)
ADD_DEPENDENCIES(thrift extern_thrift)

SET(ENV{PATH} "${THRIFT_INSTALL_DIR}/bin:$ENV{PATH}")

LIST(APPEND external_project_dependencies thrift)


