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
Utility of path related functions

Author: Wang, Cong(bigflow-opensource@baidu.com)
"""

import os
import re
import shutil

def extract_namenode(path):
    """
    extract namenode from URI
    Args:
        path(str): HDFS path or AFS path

    Returns:
        str: namenode of path

    >>>extract_namenode("hdfs://host:port/tmp")
    "hdfs://host:port"
    >>>extract_namenode("hdfs://fs/tmp")
    "hdfs://fs"
    >>>extract_namenode("hdfs:///tmp")
    "hdfs://"
    """
    match = re.match(r'((hdfs|afs)://[^/]*)/(.*)', path, re.M | re.I)
    return match.group(1) if match else None

def is_hdfs_path(path):
    """
    Check if a given path is HDFS uri

    Args:
      path (str):  input path

    Returns:
      bool:  True if input is a HDFS path, False otherwise

    >>>is_hdfs_path("/tdk")
    False
    >>>is_hdfs_path("hdfs://aa:123/bb/cc")
    True
    """
    return path.startswith("hdfs://")

def can_be_archive_path(path):
    """ Check a given path ends with common archive extensions or not

    :param path: str, input path
    :return: boolean, True if path ends with common archive extensions, otherwise False
    """
    exts = {".tar.gz", ".tgz", ".tar", ".zip", "qp", ".jar"}
    return any(map(lambda _: path.endswith(_), exts))


def is_toft_style_hdfs_path(path):
    """
    Check if a given toft style path is HDFS path

    Args:
      path (str):  input path

    Returns:
      bool:  True if input is a HDFS path, False otherwise

    Raises:
      ValueError:  if input path is not a toft style one

    >>>is_toft_style_hdfs_path("/tdk")
    False
    >>>is_toft_style_hdfs_path("/hdfs/aa/bb/cc")
    True
    """
    return path.startswith("/hdfs/")

def is_toft_style_dfs_path(path):
    """
    Check if a given toft style path is DFS path
    """
    return is_toft_style_hdfs_path(path)


def to_abs_local_path(path):
    """
    Return the absolute path if input path is a local one, otherwise return input path

    Args:
      path (str):  input path

    Returns:
      str:  absolute path

    >>>to_abs_local_path("/tdk")
    "/tdk"
    >>>to_abs_local_path("./tdk")
    "/a/b/c/tdk"
    >>>to_abs_local_path("hdfs:///a/b/c")
    "hdfs:///a/b/c"
    """
    if not is_hdfs_path(path):
        import os.path
        return os.path.abspath(path)
    else:
        return path


def rm_rf(path):
    """  Unix's rm -rf like operation.

    :param path: path to delete
    :return: None
    """
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.exists(path):
        os.remove(path)
