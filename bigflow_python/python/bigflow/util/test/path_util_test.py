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
Unit test for utils

Author: Zheng Gonglin(zhenggonglin@baidu.com)
"""

import unittest

from bigflow.util import path_util


class UtilsTestCase(unittest.TestCase):
    def test_extract_namenode(self):
        # accepte path
        accepte_paths = [
            "hdfs://host:port/a/b/c",
            "hdfs://fs/a/b/c",
            "hdfs:///a/b/c",
            "hdfs:////tmp",
        ]
        except_results = [
            "hdfs://host:port",
            "hdfs://fs",
            "hdfs://",
            "hdfs://",
        ]

        for idx, path in enumerate(accepte_paths):
            ret = path_util.extract_namenode(path)
            self.assertEqual(ret, except_results[idx])

        # wrong path
        wrong_paths = [
            "hdfs:/host:port/a/b/c",
            "hdfs:/fs/a/b/c",
            "xhdfs:///tmp",
        ]

        for path in wrong_paths:
            ret = path_util.extract_namenode(path)
            self.assertEqual(ret, None)

    def test_can_be_archive_path(self):
        input_paths = ["a.tgz", "a.tar", "a.tar.gz", "a.zip", "a.qp", "a.jar", "a.xx"]
        expected = [True, True, True, True, True, True, False]
        for path, expect in zip(input_paths, expected):
            self.assertEqual(path_util.can_be_archive_path(path), expect)

    def test_rm_r_path(self):
        import os
        import uuid
        # delete path not exists
        path_util.rm_rf("/path/not/exist")
        self.assertFalse(os.path.exists("/path/not/exist"))

        # delete single file
        tmp_path = os.path.join('.', str(uuid.uuid4()))
        with open(tmp_path, "w") as fp:
            pass
        self.assertTrue(os.path.exists(tmp_path))
        path_util.rm_rf(tmp_path)
        self.assertFalse(os.path.exists(tmp_path))

        # delete empty directory
        tmp_path = os.path.join('.', str(uuid.uuid4()))
        os.mkdir(tmp_path)
        path_util.rm_rf(tmp_path)
        self.assertFalse(os.path.exists(tmp_path))

        # delete link path
        tmp_path = os.path.join('.', str(uuid.uuid4()))
        os.mkdir(tmp_path)
        os.symlink(tmp_path, ".soft_link")
        path_util.rm_rf(".soft_link")
        self.assertFalse(os.path.exists(".soft_link"))
        self.assertTrue(os.path.exists(tmp_path))
        os.rmdir(tmp_path)

        tmp_path = os.path.join('.', str(uuid.uuid4()))
        with open(tmp_path, "w") as fp:
            pass

        os.link(tmp_path, ".hard_link")
        path_util.rm_rf(".hard_link")
        self.assertFalse(os.path.exists(".hard_link"))
        self.assertTrue(os.path.exists(tmp_path))
        path_util.rm_rf(tmp_path)
        self.assertFalse(os.path.exists(tmp_path))

        # delete dir contains file
        tmp_path = os.path.join('.', str(uuid.uuid4()))
        os.mkdir(tmp_path)
        another_path = os.path.join(tmp_path, str(uuid.uuid4()))
        with open(another_path, "w") as fp:
            pass

        self.assertTrue(os.path.exists(tmp_path))
        path_util.rm_rf(tmp_path)
        self.assertFalse(os.path.exists(tmp_path))


if __name__ == "__main__":
    unittest.main()
