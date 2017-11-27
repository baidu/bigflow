#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
#
# Copyright (c) 2015 Baidu, Inc. All Rights Reserved.

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
A class to support Pipeline's add_file/add_directory interfaces

"""

import os

from bigflow.util.log import logger
from bigflow_python.proto import python_resource_pb2


class Resource(object):
    """
    Register/Package needed resource files. Registered files will be putted into working
    directory at runtime.

    """

    def __init__(self):
        self.__files = []
        self.__binary_files = []
        self.__libraries = []
        self.__egg_files = []
        self.__file_targets = set()
        self.__library_targets = set()
        self.__egg_file_targets = set()
        self.__cache_files = None
        self.__cache_archives = None

    def add_file(self, file_path, resource_path=None, executable=False):
        """
        Add a single file to be packed with the job at runtime.

        Args:
          file_path (str):  path of file to add
          resource_path (str):  path of file at runtime
          executable (bool): if True, the file can be executed

        Raises:
          ValueError:  If invalid arguments are given
        """
        if resource_path is None:
            resource_path = os.path.basename(file_path)

        if not isinstance(file_path, str) or not isinstance(resource_path, str):
            raise ValueError("Invalid input path: must be str")

        # TODO(wangcong09): warn users if they specified different file names
        if resource_path in self.__file_targets:
            logger.warn("add [%s] duplicated" % resource_path)
            return

        self.__files.append((file_path, resource_path, executable))
        self.__file_targets.add(resource_path)

    def add_file_from_bytes(self, source_bytes, resource_path=None):
        """
        Add a single file to be packed with the job at runtime.

        Args:
          source_bytes (str):  the source binaries
          resource_path (str):  path of file at runtime
          executable (bool): if True, the file can be executed

        Raises:
          ValueError:  If invalid arguments are given
        """
        if not isinstance(source_bytes, str) or not isinstance(resource_path, str):
            raise ValueError("Invalid source bytes: must be str")

        if resource_path in self.__file_targets:
            logger.warn("add [%s] duplicated" % resource_path)
            return

        self.__binary_files.append((resource_path, source_bytes))
        self.__file_targets.add(resource_path)

    def add_egg_file(self, file_path):
        """
        Add an .egg file to be packed with the job and set its path to PYTHONPATH at runtime

        Args:
          file_path (str):  path of .egg file

        Raises:
          ValueError:  If invalid arguments are given
        """
        if not isinstance(file_path, str):
            raise ValueError("Invalid input path: must be str")

        import sys
        import traceback
        path = os.path.abspath(file_path)
        file_name = os.path.basename(path)
        if file_name in self.__egg_file_targets:
            logger.warn("add [%s] duplicated" % file_name)
            return

        self.__egg_files.append((file_name, path))
        self.__egg_file_targets.add(file_name)

    def add_directory(self, dir_path, resource_path="", is_only_python_files_accepted=False):
        """
        Add a directory recursively to be packed with the job at runtime.

        Args:
          dir_path (str):  path of directory to add
          resource_path (str):  path to directory at runtime
          is_only_python_files_accepted (bool):  if True, only .py/.pyc files are accepted

        Raises:
          ValueError:  if invalid arguments are given
        """
        if not os.path.isdir(dir_path):
            raise ValueError('Path "%s" is not a valid directory' % dir_path)
        normed = os.path.normpath(os.path.abspath(dir_path)) + '/'
        for root, subdirs, files in os.walk(normed):
            if not root.startswith(normed):
                raise ValueError("Invalid path, there may be a bug.")

            if any(path.startswith(".") for path in root[len(normed):].split(os.sep)):
                continue

            relative_path = root[len(normed): len(root)]
            for filename in files:
                if filename.startswith("."):
                    continue
                if not is_only_python_files_accepted \
                        or os.path.splitext(filename)[1] == ".py" \
                        or os.path.splitext(filename)[1] == ".pyc":
                    file_path = os.path.join(root, filename)
                    path_in_resource = os.path.join(resource_path, relative_path, filename)
                    self.add_file(file_path, path_in_resource)

    def add_dynamic_library(self, file_path):
        """
        Add a dynamic library file(.so) to be packed with the job and set it to LD_LIBRARY_PATH
        at runtime.

        Args:
          file_path:  Path of the library file
        """
        path = os.path.abspath(file_path)
        file_name = os.path.basename(path)
        if file_name in self.__library_targets:
            logger.warn("add [%s] duplicated" % file_path)
            return

        self.__libraries.append((file_name, path))
        self.__library_targets.add(file_name)

    def files_list(self):
        """
        Returns a list containing all files added.

        Returns:
          list:  added files
        """
        return self.__files[:]

    def file_target_list(self):
        """
        Returns a list containing all target files

        Returns:
            list: target files
        """
        return list(self.__file_targets)

    def libraries_list(self):
        """
        Returns a list containing all libraries added.

        Returns:
          list:  added libraries
        """
        return self.__libraries[:]

    def library_target_list(self):
        """
        Returns a list containing all target libraries

        Returns:
            list: target libraries
        """
        return list(self.__library_targets)

    def egg_files_list(self):
        """
        Returns a list containing all egg_files added.

        Returns:
          list:  added egg_files
        """
        return self.__egg_files[:]

    def egg_file_target_list(self):
        """
        Returns a list containing all target egg_files

        Returns:
            list: target egg_files
        """
        return list(self.__egg_file_targets)

    @property
    def cache_file_list(self):
        return self.__cache_files

    @cache_file_list.setter
    def cache_file_list(self, file_list):
        self.__cache_files = file_list

    @property
    def cache_archive_list(self):
        return self.__cache_archives

    @cache_archive_list.setter
    def cache_archive_list(self, archive_list):
        self.__cache_archives = archive_list

    def to_proto_message(self):
        """
        Convert resource to a protobuf message

        Returns:
          PbPythonResource:  protobuf message
        """
        resource = python_resource_pb2.PbPythonResource()
        for file_path, path_in_resource, executable in self.__files:
            added_file = resource.normal_file.add()
            added_file.file_path = file_path
            added_file.path_in_resource = path_in_resource
            added_file.is_executable = executable

        for file_name, file_path in self.__libraries:
            added_lib_file = resource.lib_file.add()
            added_lib_file.file_name = file_name
            added_lib_file.file_path = file_path

        for file_name, file_path in self.__egg_files:
            added_lib = resource.egg_file.add()
            added_lib.file_name = file_name
            added_lib.file_path = file_path

        for file_name, binary in self.__binary_files:
            added_binary = resource.binary_file.add()
            added_binary.file_name = file_name
            added_binary.binary = binary

        if self.__cache_files:
            resource.cache_file_list = self.__cache_files

        if self.__cache_archives:
            resource.cache_archive_list = self.__cache_archives

        return resource


if __name__ == "__main__":
    pass
