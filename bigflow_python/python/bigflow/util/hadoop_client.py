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
A utility to wrap Hadoop console command

"""

from bigflow import error
from bigflow.rpc import requests


def extract_fs_name_from_path(path):
    """
    Get fs.defaultFS from path like "hdfs://abcde:22222/a/b/c"
    """
    if path.startswith("hdfs://"):
        schema_pos = path.find('://')+3
        dfs_prefix = path[:schema_pos]
        sub_path = path[schema_pos:]
        pos_first_slash = sub_path.find('/')
        if pos_first_slash == 0:
            return None
        return dfs_prefix + sub_path[:pos_first_slash]
    else:
        return None


class HadoopClient:
    """
    A wrapper class of Hadoop console command

    内部类，请用户不要使用该类，未来不保证接口兼容，请直接使用hadoop-client。

    Args:
      client_path (str):  path of Hadoop client(usually 'hadoop' executable file)
      config_path (str):  path of Hadoop configuration
    """
    def __init__(self, client_path, config_path):
        self.__client_path = client_path
        self.__config_path = config_path

    def __build_args(self, path, args):
        commit_args = []

        fs_name_from_path = extract_fs_name_from_path(path)
        replace_explicit_fs_name = False

        if args is not None:
            for key, value in args.iteritems():
                if key == "fs.defaultFS" and fs_name_from_path is not None:
                    replace_explicit_fs_name = True
                    commit_args.extend(["-D", key + "=" + fs_name_from_path])
                else:
                    commit_args.extend(["-D", key + "=" + str(value)])
        if not replace_explicit_fs_name and fs_name_from_path is not None:
            commit_args.extend(["-D fs.defaultFS=" + fs_name_from_path])

        return commit_args

    def fs_mkdir(self, path, args=None):
        """
        Wraps console command 'hadoop fs -mkdir -p <path>'

        Args:
          path (str):  path to be created
        """
        if not self.fs_test(path, args):
            commit_args = ["fs"]
            commit_args.extend(self.__build_args(path, args))
            commit_args.extend(["-mkdir", "-p", path])
            if not self.__commit(commit_args):
                raise error.BigflowHDFSException("Error create HDFS path %s" % path)
            return self.fs_test(path, args)

    def fs_put(self, source, target, args=None, need_mkdir=True):
        """
        Wraps console command 'hadoop fs -put <source> <target>'

        Args:
          source (str):  path of source
          target (str):  path of target
        """
        if need_mkdir:
            import os
            mk_path = os.path.dirname(target)
            self.fs_mkdir(mk_path)

        commit_args = ["fs"]
        commit_args.extend(self.__build_args(target, args))
        commit_args.extend(["-put", source, target])
        if not self.__commit(commit_args):
            msg = "Error uploading temp file from [%s] to [%s],"\
                  " please make sure source file exists on local filesystem" \
                  " and you have the write permission to the target hdfs directory," \
                  " or may be you can change your 'tmp_data_path'" % (source, target)
            raise error.BigflowHDFSException(msg)

    def fs_get(self, source, target, args=None):
        """
        Wraps console command 'hadoop fs -get <source> <target>'

        Args:
          source (str):  path of source
          target (str):  path of target
        """
        commit_args = ["fs"]
        commit_args.extend(self.__build_args(source, args))
        commit_args.extend(["-get", source, target])
        if not self.__commit(commit_args):
            msg = "Error downloading HDFS path ['%s'] to local ['%s']" % (source, target)
            raise error.BigflowHDFSException(msg)

    def fs_rmr(self, path, args=None):
        """
        Wraps console command 'hadoop fs -rmr <path>'

        Args:
          path (str):  path to be removed
        """
        if self.fs_test(path, args):
            commit_args = ["fs"]
            commit_args.extend(self.__build_args(path, args))
            commit_args.extend(["-rmr", path])
            if not self.__commit(commit_args):
                raise error.BigflowHDFSException("Error removing HDFS path %s" % path)
            return not self.fs_test(path, args)

    def fs_mv(self, source, target, args=None):
        """
        Wraps console command 'hadoop fs -mv <source> <target>'

        Args:
          source (str):  path of source
          target (str):  path of target
        """
        commit_args = ["fs"]
        commit_args.extend(self.__build_args(source, args))
        commit_args.extend(["-mv", source, target])

        if not self.__commit(commit_args):
            msg = "Error moving HDFS path ['%s'] to ['%s']" % (source, target)
            raise error.BigflowHDFSException(msg)

    def fs_test(self, path, args=None):
        """
        Wraps console command 'hadoop fs -test -e <path>'

        Args:
          path (str):  path to test

        Returns:
          bool:  if path exist
        """
        commit_args = ["fs"]
        commit_args.extend(self.__build_args(path, args))
        commit_args.extend(["-test", "-e", path])

        return self.__commit(commit_args)

    def fs_dus(self, path, args=None):
        """
        Wraps console command 'hadoop fs -dus <path>'

        Args:
          path (str):  path to get size

        Returns:
          long:  path size
        """
        commit_args = ["fs"]
        commit_args.extend(self.__build_args(path, args))
        commit_args.extend(["-dus", path])

        return self.__commit(commit_args)

    def __commit(self, args):
        return requests.hadoop_commit(args, self.__client_path, self.__config_path)

