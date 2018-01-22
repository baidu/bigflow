#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
#
# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
########################################################################
"""
A helper script used by generic.cmake's resource_library function to generate
the ${target_name}.h header file and ${resource_src_name}.c impl files.

The args passed to resource_library are:
resource_library(target_name SRCS src_name0 src_name1 ...).
The ${target_name}.h is mapped from target_name, generated in CMAKE_CURRENT_BINARY_DIR
and includes all the resource definitions.
All the src_name files will be read from PROJECT_SOURCE_DIR/src_name, then used by this
script to generate ${resource_src_name}.c(in the CMAKE_CURRENT_BINARY_DIR).
The ${resource_src_name}.c file consists of two parts:

extern const char RESOURCE_${resource_src_name}[] = {$binary_data};
unsigned int ${resource_src_name}_len= xx;

${resource_src_name} is mapped from src_name by replacing '/' or '.' to '_'.

"""
from __future__ import print_function
import os
import sys


# Assume 'reduce' fuction needs to be imported in this way in future versions.
version_info = int(sys.version.split('.')[0])
if version_info > 2:
    from functools import reduce


def generate_line(byte_list):
    line = reduce(lambda x, y: x + ', ' + "%s" % str(y), byte_list)
    return line + ','


def generate_resource_and_src_list(base_dir, src_list):
    resource_list = []
    for src in src_list:
        prefix = os.path.commonprefix([base_dir, src])
        component = src[len(prefix):].lstrip('/')
        resource_name = component.replace('/', '_').replace('.', '_')
        c_filename = resource_name + ".c"
        resource_name = resource_name.replace('___', '')
        resource_list.append((src, resource_name, c_filename, os.path.getsize(src)))

    return resource_list


if __name__ == "__main__":
    """
    Used by resource_library with following args:
    
    python resurce_library.py ${base_dir} ${header_file} ${src_name0} ${src_name1} ...
    """
    base_dir, header_file = sys.argv[1:3]
    srcs = sys.argv[3:]

    resource_list = generate_resource_and_src_list(base_dir, srcs)

    hfile = open(header_file, 'w')
    print('#ifdef __cplusplus', file=hfile)
    print('extern "C" {', file=hfile)
    print('#endif', file=hfile)

    for (source, resource_name, c_filename, size) in resource_list:
        f = open(source, 'rb')
        all = f.read()
        all = bytearray(all)
        line_list = []
        byte_list = []
        for b in all:
            if len(byte_list) > 0 and len(byte_list) % 12 == 0:
                line_list.append(generate_line(byte_list))
                byte_list = []
            byte_list.append(hex(b))

        if len(byte_list) > 0:
            line_list.append(generate_line(byte_list))

        print("extern const char RESOURCE_%s[%d];" % (resource_name, size), file=hfile)
        with open(c_filename, "w") as c_file:
            print("const char RESOURCE_%s[] = {" % resource_name, file=c_file)
            line_list[len(line_list) - 1] = line_list[len(line_list) - 1][:-1]
            for line in line_list:
                print('   ' + line, file=c_file)
            print('};', file=c_file)
            print("unsigned int %s_len = %d;" % (resource_name, size), file=c_file)

    print('#ifdef __cplusplus', file=hfile)
    print('}', file=hfile)
    print('#endif', file = hfile)
    hfile.close()
