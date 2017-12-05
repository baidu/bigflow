
INCLUDE(ExternalProject)

SET(PYTHON_SOURCES_DIR ${THIRD_PARTY_PATH}/Python-2.7.12)
SET(PYTHON_INSTALL_DIR ${PYTHON_SOURCES_DIR}/build)
SET(PYTHON_INCLUDE_DIR ${PYTHON_INSTALL_DIR}/include)
IF(APPLE)
  SET(PYTHON_CONFIGURE_COMMAND
    # note 'sh -c' here as CMake doesn't deal with space in values well
    sh -c "./configure --enable-shared --prefix=${PYTHON_INSTALL_DIR} --enable-unicode=ucs4 CFLAGS='-fPIC -I/usr/local/opt/openssl/include' LDFLAGS=-L/usr/local/opt/openssl/lib")
ELSE()
  SET(PYTHON_CONFIGURE_COMMAND
      ./configure --enable-shared --prefix=${PYTHON_INSTALL_DIR} CFLAGS=-fPIC --enable-unicode=ucs4 --with-system-ffi)
ENDIF()


ExternalProject_Add(
    extern_python
    DOWNLOAD_DIR ${THIRD_PARTY_PATH}
    DOWNLOAD_COMMAND rm -rf ${PYTHON_SOURCES_DIR} && rm -f ${THIRD_PARTY_PATH}/Python-2.7.12.tgz  && wget https://www.python.org/ftp/python/2.7.12/Python-2.7.12.tgz -O Python-2.7.12.tgz && tar xzf Python-2.7.12.tgz
    CONFIGURE_COMMAND cd ${PYTHON_SOURCES_DIR} && ${PYTHON_CONFIGURE_COMMAND}
    BUILD_COMMAND cd ${PYTHON_SOURCES_DIR} && make -j 8
    INSTALL_COMMAND cd ${PYTHON_SOURCES_DIR} && make install && cp -r ${PYTHON_INCLUDE_DIR} ${THIRD_PARTY_PATH} && cp -rf ${PYTHON_INSTALL_DIR}/lib ${THIRD_PARTY_PATH}
    )

SET(PYTHON_CMD ${PYTHON_INSTALL_DIR}/bin/python)
ADD_LIBRARY(python SHARED IMPORTED GLOBAL)
SET_PROPERTY(TARGET python PROPERTY IMPORTED_LOCATION ${PYTHON_INSTALL_DIR}/lib/libpython2.7${CMAKE_SHARED_LIBRARY_SUFFIX})
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_DIR}/python2.7)
ADD_DEPENDENCIES(python extern_python)

LIST(APPEND external_project_dependencies python)


