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

import os
import sys


def print_line(file, string):
    # In python 2 and python 3, the way to call print functions is different. 
    if sys.version_info[0] > 2:
        exec("print(string, file=file)")
    else:
        exec("print >> file, string")


def generate_line(byte_list):
    # Assume 'reduce' fuction needs to be imported in this way in future versions.
    if sys.version_info[0] > 2:
        from functools import reduce
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
    print_line(hfile, '#ifdef __cplusplus')
    print_line(hfile, 'extern "C" {')
    print_line(hfile, '#endif')

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

        print_line(hfile, "extern const char RESOURCE_%s[%d];" % (resource_name, size))
        with open(c_filename, "w") as c_file:
            print_line(c_file, "const char RESOURCE_%s[] = {" % resource_name)
            line_list[len(line_list) - 1] = line_list[len(line_list) - 1][:-1]
            for line in line_list:
                print_line(c_file, '   ' + line)
            print_line(c_file, '};')
            print_line(c_file, "unsigned int %s_len = %d;" % (resource_name, size))

    print_line(hfile, '#ifdef __cplusplus')
    print_line(hfile, '}')
    print_line(hfile, '#endif')
    hfile.close()
