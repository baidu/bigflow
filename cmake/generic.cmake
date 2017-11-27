# Copyright (c) 2017 bigflow Authors. All Rights Reserve.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# generic.cmake defines CMakes functions that look like Bazel's
# building rules (https://bazel.build/).
#
#
# ------------
#     C++
# ------------
# cc_library
# cc_binary
# cc_test
# ------------
#
# To build a static library example.a from example.cc using the system
#  compiler (like GCC):
#
#   cc_library(example SRCS example.cc)
#
# To build a static library example.a from multiple source files
# example{1,2,3}.cc:
#
#   cc_library(example SRCS example1.cc example2.cc example3.cc)
#
# To build a shared library example.so from example.cc:
#
#   cc_library(example SHARED SRCS example.cc)
#
# To specify that a library new_example.a depends on other libraies:
#
#   cc_library(new_example SRCS new_example.cc DEPS example)
#
# Static libraries can be composed of other static libraries:
#
#   cc_library(composed DEPS dependent1 dependent2 dependent3)
#
# To build an executable binary file from some source files and
# dependent libraries:
#
#   cc_binary(example SRCS main.cc something.cc DEPS example1 example2)
#
# To build a unit test binary, which is an executable binary with
# GoogleTest linked:
#
#   cc_test(example_test SRCS example_test.cc DEPS example)
#
# It is pretty often that executable and test binaries depend on
# pre-defined external libaries like glog and gflags defined in
# cmake/*.cmake:
#
#   cc_test(example_test SRCS example_test.cc DEPS example glog gflags)
#

# including binary directory for generated headers.
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR})

IF(NOT APPLE AND NOT ANDROID)
    FIND_PACKAGE(Threads REQUIRED)
    LINK_LIBRARIES(${CMAKE_THREAD_LIBS_INIT})
    SET(CMAKE_CXX_LINK_EXECUTABLE "${CMAKE_CXX_LINK_EXECUTABLE} -ldl -lrt")
ENDIF()

FUNCTION(PROTO_GENERATE_CPP SRCS HDRS)
  CMAKE_PARSE_ARGUMENTS(protobuf "" "EXPORT_MACRO" "" ${ARGN})

  SET(PROTO_FILES "${protobuf_UNPARSED_ARGUMENTS}")
  IF(NOT PROTO_FILES)
    MESSAGE(SEND_ERROR "Error: PROTOBUF_GENERATE_CPP() called without any proto files")
    RETURN()
  ENDIF()

  IF(protobuf_EXPORT_MACRO)
    SET(DLL_EXPORT_DECL "dllexport_decl=${protobuf_EXPORT_MACRO}:")
  ENDIF()

  IF(DEFINED PROTOBUF_IMPORT_DIRS AND NOT DEFINED Protobuf_IMPORT_DIRS)
    SET(Protobuf_IMPORT_DIRS "${PROTOBUF_IMPORT_DIRS}")
  ENDIF()

  IF(DEFINED Protobuf_IMPORT_DIRS)
    FOREACH(DIR ${Protobuf_IMPORT_DIRS})
      GET_FILENAME_COMPONENT(ABS_PATH ${DIR} ABSOLUTE)
      LIST(FIND _protobuf_include_path ${ABS_PATH} _contains_already)
      IF(${_contains_already} EQUAL -1)
          LIST(APPEND _protobuf_include_path -I ${ABS_PATH})
      ENDIF()
    ENDFOREACH()
  ENDIF()

  SET(${SRCS})
  SET(${HDRS})
  FOREACH(FIL ${PROTO_FILES})
    GET_FILENAME_COMPONENT(ABS_FIL ${FIL} ABSOLUTE)
    GET_FILENAME_COMPONENT(FIL_WE ${FIL} NAME_WE)
    IF(NOT PROTOBUF_GENERATE_CPP_APPEND_PATH)
      GET_FILENAME_COMPONENT(FIL_DIR ${FIL} DIRECTORY)
      IF(FIL_DIR)
        SET(FIL_WE "${FIL_DIR}/${FIL_WE}")
      ENDIF()
    ENDIF()

    LIST(APPEND ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc")
    LIST(APPEND ${HDRS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h")

    #message("${ABS_FIL} ${_protobuf_include_path}")
    ADD_CUSTOM_COMMAND(
      OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc"
             "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h"
      COMMAND  ${Protobuf_PROTOC_EXECUTABLE}
      ARGS "--cpp_out=${DLL_EXPORT_DECL}${CMAKE_BINARY_DIR}" ${_protobuf_include_path} ${ABS_FIL}
      DEPENDS ${ABS_FIL} ${Protobuf_PROTOC_EXECUTABLE}
      COMMENT "Running C++ protocol buffer compiler on ${FIL}"
      VERBATIM )
  ENDFOREACH()

  SET_SOURCE_FILES_PROPERTIES(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  SET(${SRCS} ${${SRCS}} PARENT_SCOPE)
  SET(${HDRS} ${${HDRS}} PARENT_SCOPE)
ENDFUNCTION()

FUNCTION(merge_static_libs TARGET_NAME)
  SET(libs ${ARGN})
  LIST(REMOVE_DUPLICATES libs)

  # Get all propagation dependencies from the merged libraries
  SET(libs_deps "")
  FOREACH(lib ${libs})
    LIST(APPEND libs_deps ${${lib}_LIB_DEPENDS})
  ENDFOREACH()

  LIST(REMOVE_DUPLICATES libs_deps)

  IF(APPLE) # Use OSX's libtool to merge archives
    # To produce a library we need at least one source file.
    # It is created by add_custom_command below and will helps
    # also help to track dependencies.
    SET(dummyfile ${CMAKE_BINARY_DIR}/${TARGET_NAME}_dummy.c)

    # Make the generated dummy source file depended on all static input
    # libs. If input lib changes,the source file is touched
    # which causes the desired effect (relink).
    ADD_CUSTOM_COMMAND(OUTPUT ${dummyfile}
      COMMAND ${CMAKE_COMMAND} -E touch ${dummyfile}
      DEPENDS ${libs})

    # Generate dummy staic lib
    STRING(REPLACE - _ _id ${TARGET_NAME})
    FILE(WRITE ${dummyfile} "const char * ${_id}_dummy = \"${dummyfile}\";")
    ADD_LIBRARY(${TARGET_NAME} STATIC ${dummyfile})
    TARGET_LINK_LIBRARIES(${TARGET_NAME} ${libs_deps})

    FOREACH(lib ${libs})
      # Get the file names of the libraries to be merged
      set(libfiles ${libfiles} $<TARGET_FILE:${lib}>)
    ENDFOREACH()
    ADD_CUSTOM_COMMAND(TARGET ${TARGET_NAME} POST_BUILD
      COMMAND rm -rf "${CMAKE_BINARY_DIR}/lib${TARGET_NAME}.a"
      COMMAND /usr/bin/libtool -static -o "${CMAKE_BINARY_DIR}/lib${TARGET_NAME}.a" ${libfiles})
  ELSE() # general UNIX: use "ar" to extract objects and re-add to a common lib
    FOREACH(lib ${libs})
      SET(objlistfile ${lib}.objlist) # list of objects in the input library
      SET(objdir ${lib}.objdir)

      ADD_CUSTOM_COMMAND(OUTPUT ${objdir}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${objdir}
        DEPENDS ${lib})

      ADD_CUSTOM_COMMAND(OUTPUT ${objlistfile}
        COMMAND ${CMAKE_AR} -x "$<TARGET_FILE:${lib}>"
        COMMAND ${CMAKE_AR} -t "$<TARGET_FILE:${lib}>" > ../${objlistfile}
        DEPENDS ${lib} ${objdir}
        WORKING_DIRECTORY ${objdir})

      # Empty dummy source file that goes into merged library
      SET(mergebase ${lib}.mergebase.c)
      ADD_CUSTOM_COMMAND(OUTPUT ${mergebase}
        COMMAND ${CMAKE_COMMAND} -E touch ${mergebase}
        DEPENDS ${objlistfile})

      LIST(APPEND mergebases "${mergebase}")
    ENDFOREACH()

    ADD_LIBRARY(${TARGET_NAME} STATIC ${mergebases})
    TARGET_LINK_LIBRARIES(${TARGET_NAME} ${libs_deps})

    # Get the file name of the generated library
    SET(outlibfile "$<TARGET_FILE:${TARGET_NAME}>")

    FOREACH(lib ${libs})
      ADD_CUSTOM_COMMAND(TARGET ${TARGET_NAME} POST_BUILD
        COMMAND ${CMAKE_AR} cr ${outlibfile} *.o
        COMMAND ${CMAKE_RANLIB} ${outlibfile}
        WORKING_DIRECTORY ${lib}.objdir)
    ENDFOREACH()
  ENDIF()
ENDFUNCTION(merge_static_libs)

FUNCTION(cc_library TARGET_NAME)
  SET(options STATIC static SHARED shared)
  SET(oneValueArgs LINK_ALL_SYMBOLS)
  SET(multiValueArgs SRCS DEPS ALL_SYMBOLS_DEPS)
  CMAKE_PARSE_ARGUMENTS(cc_library "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
  IF(cc_library_SRCS)
    IF(cc_library_SHARED OR cc_library_shared) # build *.so
      add_library(${TARGET_NAME} SHARED ${cc_library_SRCS})
    ELSE()
      add_library(${TARGET_NAME} STATIC ${cc_library_SRCS})
    ENDIF()

    IF (cc_library_DEPS)
	  SET(links "")
      FOREACH(dep ${cc_library_DEPS})
        LIST(APPEND links ${dep})
      ENDFOREACH()

      FOREACH(dep ${cc_library_ALL_SYMBOLS_DEPS})
        IF(APPLE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
          LIST(APPEND links -force_load ${dep} )
        ELSE()
          LIST(APPEND links -Wl,--whole-archive ${dep} -Wl,--no-whole-archive)
        ENDIF()
      ENDFOREACH()

      ADD_DEPENDENCIES(${TARGET_NAME} ${cc_library_DEPS} ${cc_library_ALL_SYMBOLS_DEPS})
      TARGET_LINK_LIBRARIES(${TARGET_NAME} ${links})
    ENDIF()

    # cpplint code style
    #add_style_check_target(${TARGET_NAME} ${cc_library_SRCS})

  ELSE()
    IF (cc_library_DEPS)
	    merge_static_libs(${TARGET_NAME} ${cc_library_DEPS})
    ELSE()
      MESSAGE(FATAL "Please specify source file or library in cc_library.")
    ENDIF()
  ENDIF()

  IF (cc_library_LINK_ALL_SYMBOLS)
      SET_TARGET_PROPERTIES(${TARGET_NAME} PROPERTIES link_all_symbols "true")
  ENDIF()

ENDFUNCTION(cc_library)

FUNCTION(cc_binary TARGET_NAME)
  SET(options "")
  SET(oneValueArgs "")
  SET(multiValueArgs SRCS DEPS)
  CMAKE_PARSE_ARGUMENTS(cc_binary "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
  ADD_EXECUTABLE(${TARGET_NAME} ${cc_binary_SRCS})

  SET(links "")
  FOREACH(dep ${cc_binary_DEPS})
      GET_TARGET_PROPERTY(prop ${dep} link_all_symbols)
      IF(${prop} STREQUAL "true")
        IF(APPLE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
          LIST(APPEND links -fource_load ${dep} )
        ELSE()
          LIST(APPEND links -Wl,--whole-archive ${dep} -Wl,--no-whole-archive)
        ENDIF()
      ELSE()
          LIST(APPEND links ${dep})
      ENDIF()
  ENDFOREACH()

  IF(cc_binary_DEPS)
      TARGET_LINK_LIBRARIES(${TARGET_NAME} ${links})
      ADD_DEPENDENCIES(${TARGET_NAME} ${cc_binary_DEPS})
  ENDIF()
ENDFUNCTION(cc_binary)

FUNCTION(cc_test TARGET_NAME)
  IF(WITH_TESTING)
    SET(options "")
    SET(oneValueArgs "")
    SET(multiValueArgs SRCS DEPS)
    CMAKE_PARSE_ARGUMENTS(cc_test "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    ADD_EXECUTABLE(${TARGET_NAME} ${cc_test_SRCS})
    # disable link all symbols until we reordered cc_libarary and cc_binary/cc_test
#    SET(links "")
#    FOREACH(dep ${cc_test_DEPS})
#      GET_TARGET_PROPERTY(prop ${dep} link_all_symbols)
#      IF(${prop} STREQUAL "true")
#        IF(APPLE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
#          LIST(APPEND links -force_load ${dep} )
#        ELSE()
#          LIST(APPEND links -Wl,--whole-archive ${dep} -Wl,--no-whole-archive)
#        ENDIF()
#      ELSE()
#        LIST(APPEND links ${dep})
#      ENDIF()
#    ENDFOREACH()

    TARGET_LINK_LIBRARIES(${TARGET_NAME} ${cc_test_DEPS} gtest gtest_main)
    ADD_DEPENDENCIES(${TARGET_NAME} ${cc_test_DEPS} gtest gtest_main)
    ADD_TEST(NAME ${TARGET_NAME} COMMAND ${TARGET_NAME} WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  ENDIF()
ENDFUNCTION(cc_test)

FUNCTION(proto_library TARGET_NAME)
  SET(oneValueArgs "")
  SET(multiValueArgs SRCS DEPS IMPORT_DIRS)
  CMAKE_PARSE_ARGUMENTS(proto_library "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
  SET(Protobuf_IMPORT_DIRS ${PROJECT_SOURCE_DIR})
  #message("${Protobuf_IMPORT_DIRS}")
  SET(proto_srcs)
  SET(proto_hdrs)
  PROTO_GENERATE_CPP(proto_srcs proto_hdrs ${proto_library_SRCS})
  cc_library(${TARGET_NAME} SRCS ${proto_srcs} DEPS ${proto_library_DEPS} protobuf)
ENDFUNCTION()

FUNCTION(resource_library TARGET_NAME)
  SET(oneValueArgs "")
  SET(multiValueArgs SRCS OUTPUTS DEPS)
  CMAKE_PARSE_ARGUMENTS(resource_library "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
  SET(tmp_target "resource_library${TARGET_NAME}")
  SET(abs_src_list "")
  SET(generated_src_list "")
  STRING(LENGTH ${PROJECT_SOURCE_DIR}/ _source_dir_len)
  FOREACH(src ${resource_library_SRCS})
    GET_FILENAME_COMPONENT(abs_file ${src} ABSOLUTE)
    STRING(SUBSTRING ${abs_file} ${_source_dir_len} -1 _component)
    STRING(REPLACE / _ _new_file ${_component})
    STRING(REPLACE . _ _new_file ${_new_file})
      SET(generated_src ${_new_file}.c)
      SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/${generated_src} PROPERTIES GENERATED TRUE)
    LIST(APPEND abs_src_list ${abs_file})
    LIST(APPEND generated_src_list ${generated_src})
  ENDFOREACH()
  ADD_CUSTOM_TARGET(${tmp_target}
    COMMAND python ${PROJECT_SOURCE_DIR}/resource_library.py ${PROJECT_SOURCE_DIR} ${TARGET_NAME}.h ${abs_src_list}
  )
  cc_library(${TARGET_NAME} SRCS ${generated_src_list})
  ADD_DEPENDENCIES(${TARGET_NAME} ${tmp_target})
  IF(resource_library_DEPS)
    ADD_DEPENDENCIES(${tmp_target} ${resource_library_DEPS})
  ENDIF()
ENDFUNCTION()

FUNCTION(py_proto_compile TARGET_NAME)
  SET(oneValueArgs "")
  SET(multiValueArgs SRCS)
  CMAKE_PARSE_ARGUMENTS(py_proto_compile "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
  SET(py_srcs)
  PROTOBUF_GENERATE_PYTHON(py_srcs ${py_proto_compile_SRCS})
  ADD_CUSTOM_TARGET(${TARGET_NAME} ALL DEPENDS ${py_srcs})
ENDFUNCTION()
