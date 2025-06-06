#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# If cmake variable HDFSPP_LIBRARY_ONLY is set, then tests, examples, and
# tools will not be built. This allows for faster builds of the libhdfspp
# library alone, avoids looking for a JDK, valgrind, and gmock, and
# prevents the generation of multiple binaries that might not be relevant
# to other projects during normal use.
# Example of cmake invocation with HDFSPP_LIBRARY_ONLY enabled:
# cmake -DHDFSPP_LIBRARY_ONLY=1

project (libhdfspp)

cmake_minimum_required(VERSION 2.8)

find_package (Boost 1.86.0 REQUIRED COMPONENTS date_time)

enable_testing()
set(CMAKE_CXX_STANDARD 17)
include (CTest)

SET(BUILD_SHARED_HDFSPP TRUE CACHE STRING "BUILD_SHARED_HDFSPP defaulting to 'TRUE'")
SET(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})

# If there's a better way to inform FindCyrusSASL.cmake, let's make this cleaner:
SET(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};${CYRUS_SASL_DIR};${GSASL_DIR};$ENV{PROTOBUF_HOME}")

# Specify PROTOBUF_HOME so that find_package picks up the correct version
SET(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};$ENV{PROTOBUF_HOME}")

find_package(Doxygen)
find_package(OpenSSL REQUIRED)
find_package(Protobuf REQUIRED)
find_package(CyrusSASL)
find_package(GSasl)
find_package(Threads)

include(CheckCXXSourceCompiles)
include(CheckSymbolExists)
include(FetchContent)

# Install googletest
# As per the approach documented in the GoogleTest repo -
# https://github.com/google/googletest/blob/e649993a402d96afe25fbf3413749adf0f2947f6/googletest/README.md#incorporating-into-an-existing-cmake-project
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/703bd9caab50b139428cea1aaff9974ebee5742e.zip
)
# The commit SHA 703bd9caab50b139428cea1aaff9974ebee5742e in the URL above corresponds to the
# tag release-1.10.0 in the googletest repository.
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Check if thread_local is supported
unset (THREAD_LOCAL_SUPPORTED CACHE)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_REQUIRED_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
check_cxx_source_compiles(
    "#include <thread>
    int main(void) {
      thread_local int s;
      return 0;
    }"
    THREAD_LOCAL_SUPPORTED)
if (NOT THREAD_LOCAL_SUPPORTED)
  message(FATAL_ERROR
  "FATAL ERROR: The required feature thread_local storage is not supported by your compiler. \
  Known compilers that support this feature: GCC 4.8+, Visual Studio 2015+, Clang (community \
  version 3.3+), Clang (version for Xcode 8+ and iOS 9+).")
endif (NOT THREAD_LOCAL_SUPPORTED)

# Check if PROTOC library was compiled with the compatible compiler by trying
# to compile some dummy code
unset (PROTOC_IS_COMPATIBLE CACHE)
set (CMAKE_REQUIRED_INCLUDES ${PROTOBUF_INCLUDE_DIRS})
set (CMAKE_REQUIRED_LIBRARIES ${PROTOBUF_LIBRARY} ${PROTOBUF_PROTOC_LIBRARY})
check_cxx_source_compiles(
    "#include <google/protobuf/io/printer.h>
    #include <string>
    int main(void) {
      ::google::protobuf::io::ZeroCopyOutputStream *out = NULL;
      ::google::protobuf::io::Printer printer(out, '$');
      printer.PrintRaw(std::string(\"test\"));
      return 0;
    }"
    PROTOC_IS_COMPATIBLE)
if (NOT PROTOC_IS_COMPATIBLE)
  message(WARNING
  "WARNING: the Protocol Buffers Library and the Libhdfs++ Library must both be compiled \
  with the same (or compatible) compiler. Normally only the same major versions of the same \
  compiler are compatible with each other.")
endif (NOT PROTOC_IS_COMPATIBLE)

find_program(MEMORYCHECK_COMMAND valgrind HINTS ${VALGRIND_DIR} )
set(MEMORYCHECK_COMMAND_OPTIONS "--trace-children=no --leak-check=full --error-exitcode=1 --suppressions=${PROJECT_SOURCE_DIR}/tests/memcheck.supp")
message(STATUS "valgrind location: ${MEMORYCHECK_COMMAND}")

if (REQUIRE_VALGRIND AND MEMORYCHECK_COMMAND MATCHES "MEMORYCHECK_COMMAND-NOTFOUND" )
  message(FATAL_ERROR "valgrind was required but not found.  "
                      "The path can be included via a -DVALGRIND_DIR=... flag passed to CMake.")
endif (REQUIRE_VALGRIND AND MEMORYCHECK_COMMAND MATCHES "MEMORYCHECK_COMMAND-NOTFOUND" )

# Find the SASL library to use.  If you don't want to require a sasl library,
#    define -DNO_SASL=1 in your cmake call
# Prefer Cyrus SASL, but use GSASL if it is found
# Note that the packages can be disabled by setting CMAKE_DISABLE_FIND_PACKAGE_GSasl or
#    CMAKE_DISABLE_FIND_PACKAGE_CyrusSASL, respectively (case sensitive)
set (SASL_LIBRARIES)
set (SASL_INCLUDE_DIR)
if (NOT NO_SASL)
    if (CYRUS_SASL_FOUND)
        message(STATUS "Using Cyrus SASL; link with ${CYRUS_SASL_SHARED_LIB}")
        set (SASL_INCLUDE_DIR ${CYRUS_SASL_INCLUDE_DIR})
        set (SASL_LIBRARIES ${CYRUS_SASL_SHARED_LIB})
        set (CMAKE_USING_CYRUS_SASL 1)
        add_definitions(-DUSE_SASL -DUSE_CYRUS_SASL)
    else (CYRUS_SASL_FOUND)
        if (REQUIRE_CYRUS_SASL)
          message(FATAL_ERROR "Cyrus SASL was required but not found.  "
                                "The path can be included via a -DCYRUS_SASL_DIR=... flag passed to CMake.")
        endif (REQUIRE_CYRUS_SASL)

        # If we didn't pick Cyrus, use GSASL instead
        if (GSASL_FOUND)
          message(STATUS "Using GSASL; link with ${GSASL_LIBRARIES}")
          set (SASL_INCLUDE_DIR ${GSASL_INCLUDE_DIR})
          set (SASL_LIBRARIES ${GSASL_LIBRARIES})
          set (CMAKE_USING_GSASL 1)
          add_definitions(-DUSE_SASL -DUSE_GSASL)
        else (GSASL_FOUND)
          if (REQUIRE_GSASL)
            message(FATAL_ERROR "GSASL was required but not found.  "
                                "The path can be included via a -DGSASL_DIR=... flag passed to CMake.")
          endif (REQUIRE_GSASL)

          # No SASL was found, but NO_SASL was not defined
          message(FATAL_ERROR "Cound not find a SASL library (GSASL (gsasl) or Cyrus SASL (libsasl2).  "
                            "Install/configure one of them or define NO_SASL=1 in your cmake call")
        endif (GSASL_FOUND)
    endif (CYRUS_SASL_FOUND)
else (NOT NO_SASL)
    message(STATUS "Compiling with NO SASL SUPPORT")
endif (NOT NO_SASL)

check_symbol_exists(explicit_bzero "string.h" HAVE_EXPLICIT_BZERO)
if(HAVE_EXPLICIT_BZERO)
    add_definitions(-DHAVE_EXPLICIT_BZERO)
endif()

add_definitions(-DASIO_STANDALONE -DASIO_CPP11_DATE_TIME)

# Disable optimizations if compiling debug
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")

if(UNIX)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -g -fPIC -fno-strict-aliasing")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -fPIC -fno-strict-aliasing")
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_definitions(-DASIO_HAS_STD_ADDRESSOF -DASIO_HAS_STD_ARRAY -DASIO_HAS_STD_ATOMIC -DASIO_HAS_CSTDINT -DASIO_HAS_STD_SHARED_PTR -DASIO_HAS_STD_TYPE_TRAITS -DASIO_HAS_VARIADIC_TEMPLATES -DASIO_HAS_STD_FUNCTION -DASIO_HAS_STD_CHRONO -DASIO_HAS_STD_SYSTEM_ERROR)
endif ()

# Mac OS 10.7 and later deprecates most of the methods in OpenSSL.
# Add -Wno-deprecated-declarations to avoid the warnings.
if(APPLE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -Wno-deprecated-declarations -Wno-unused-local-typedef")
endif()

if(DOXYGEN_FOUND)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/doc/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile @ONLY)
add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile
                  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                  COMMENT "Generating API documentation with Doxygen" VERBATIM)
endif(DOXYGEN_FOUND)


# Copy files from the hadoop tree into the output/extern directory if
#    they've changed
function (copy_on_demand input_src_glob input_dest_dir)
  get_filename_component(src_glob ${input_src_glob} REALPATH)
  get_filename_component(dest_dir ${input_dest_dir} REALPATH)
  get_filename_component(src_dir ${src_glob} PATH)
  message(STATUS "Syncing ${src_glob} to ${dest_dir}")

  file(GLOB_RECURSE src_files ${src_glob})
  foreach(src_path ${src_files})
    file(RELATIVE_PATH relative_src ${src_dir} ${src_path})
    set(dest_path "${dest_dir}/${relative_src}")
    add_custom_command(TARGET copy_hadoop_files
     COMMAND ${CMAKE_COMMAND} -E copy_if_different "${src_path}" "${dest_path}"
    )
  endforeach()
endfunction()

# If we're building in the hadoop tree, pull the Hadoop files that
#     libhdfspp depends on.  This allows us to ensure that
#     the distribution will have a consistent set of headers and
#     .proto files
if(HADOOP_BUILD)
    set(HADOOP_IMPORT_DIR ${PROJECT_BINARY_DIR}/extern)
    get_filename_component(HADOOP_IMPORT_DIR ${HADOOP_IMPORT_DIR} REALPATH)

  add_custom_target(copy_hadoop_files ALL)

  # Gather the Hadoop files and resources that libhdfs++ needs to build
  copy_on_demand(../libhdfs/include/*.h* ${HADOOP_IMPORT_DIR}/include)
  copy_on_demand(${CMAKE_CURRENT_LIST_DIR}/../../../../../hadoop-hdfs-client/src/main/proto/*.proto ${HADOOP_IMPORT_DIR}/proto/hdfs)
  copy_on_demand(${CMAKE_CURRENT_LIST_DIR}/../../../../../../hadoop-common-project/hadoop-common/src/main/proto/*.proto  ${HADOOP_IMPORT_DIR}/proto/hadoop)
  copy_on_demand(${CMAKE_CURRENT_LIST_DIR}/../../../../../../hadoop-common-project/hadoop-common/src/test/proto/*.proto  ${HADOOP_IMPORT_DIR}/proto/hadoop_test)
else(HADOOP_BUILD)
  set(HADOOP_IMPORT_DIR ${CMAKE_CURRENT_LIST_DIR}/extern)
endif(HADOOP_BUILD)

# Paths to find the imported files
set(PROTO_HDFS_DIR         ${HADOOP_IMPORT_DIR}/proto/hdfs)
set(PROTO_HADOOP_DIR       ${HADOOP_IMPORT_DIR}/proto/hadoop)
set(PROTO_HADOOP_TEST_DIR  ${HADOOP_IMPORT_DIR}/proto/hadoop_test)

include_directories(
  include
  lib
  ${HADOOP_IMPORT_DIR}/include
)

include_directories( SYSTEM
  ${PROJECT_BINARY_DIR}/lib/proto
  ${Boost_INCLUDE_DIRS}
  third_party/rapidxml-1.13
  ${gtest_SOURCE_DIR}/include
  ${gmock_SOURCE_DIR}/include
  third_party/tr2
  third_party/protobuf
  third_party/uriparser2
  ${OPENSSL_INCLUDE_DIR}
  ${SASL_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
)

add_subdirectory(third_party/uriparser2)
add_subdirectory(lib)
if(NOT HDFSPP_LIBRARY_ONLY)
    add_subdirectory(tests)
    add_subdirectory(examples)
    add_subdirectory(tools)
endif()

# create an empty file; hadoop_add_dual_library wraps add_library which
# requires at least one file as an argument
set(EMPTY_FILE_CC ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/empty.cc)
file(WRITE ${EMPTY_FILE_CC} "")

# Build the output libraries
if(NEED_LINK_DL)
   set(LIB_DL dl)
endif()

set(LIBHDFSPP_VERSION "0.1.0")
set(LIBHDFSPP_ALL_OBJECTS $<TARGET_OBJECTS:x_platform_obj> $<TARGET_OBJECTS:bindings_c_obj> $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:rpc_obj> $<TARGET_OBJECTS:reader_obj> $<TARGET_OBJECTS:proto_obj> $<TARGET_OBJECTS:connection_obj> $<TARGET_OBJECTS:common_obj> $<TARGET_OBJECTS:uriparser2_obj>)
# HDFS-16464: We don't support building Hadoop DLL for Windows yet.
if (HADOOP_BUILD AND NOT MSVC)
  hadoop_add_dual_library(hdfspp ${EMPTY_FILE_CC} ${LIBHDFSPP_ALL_OBJECTS})
  hadoop_target_link_dual_libraries(hdfspp
    ${LIB_DL}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${SASL_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES})
  set_target_properties(hdfspp PROPERTIES SOVERSION ${LIBHDFSPP_VERSION})
  hadoop_dual_output_directory(hdfspp ${OUT_DIR})
else (HADOOP_BUILD AND NOT MSVC)
  add_library(hdfspp_static STATIC ${EMPTY_FILE_CC} ${LIBHDFSPP_ALL_OBJECTS})
  target_link_libraries(hdfspp_static PUBLIC
    ${LIB_DL}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${SASL_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES})
  if(BUILD_SHARED_HDFSPP)
    add_library(hdfspp SHARED ${EMPTY_FILE_CC} ${LIBHDFSPP_ALL_OBJECTS})
    set_target_properties(hdfspp PROPERTIES SOVERSION ${LIBHDFSPP_VERSION})
  endif(BUILD_SHARED_HDFSPP)
endif (HADOOP_BUILD AND NOT MSVC)

# Set up make install targets
# Can be installed to a particular location via "make DESTDIR=... install"
file(GLOB_RECURSE LIBHDFSPP_HEADER_FILES "${CMAKE_CURRENT_LIST_DIR}/include/*.h*")
file(GLOB_RECURSE LIBHDFS_HEADER_FILES "${HADOOP_IMPORT_DIR}/include/*.h*")
install(FILES ${LIBHDFSPP_HEADER_FILES} DESTINATION include/hdfspp)
install(FILES ${LIBHDFS_HEADER_FILES} DESTINATION include/hdfs)

install(TARGETS hdfspp_static ARCHIVE DESTINATION lib)
if(BUILD_SHARED_HDFSPP)
  install(TARGETS hdfspp LIBRARY DESTINATION lib)
endif(BUILD_SHARED_HDFSPP)

add_custom_target(
    InstallToBuildDirectory
    COMMAND "${CMAKE_MAKE_PROGRAM}" install DESTDIR=${PROJECT_BINARY_DIR}/output
)
set(LIBHDFSPP_DIR ${PROJECT_BINARY_DIR}/output)
