#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""
Script definition

File: python_resource_test.py
Author: Wang, Cong(wangcong09@baidu.com)
Date: 3/26/15 7:46 PM
"""
import os
import unittest
import shutil
import uuid
from bigflow.runtime import python_resource


class PythonResourceTest(unittest.TestCase):
    TEST_DIR = "testdir" + str(uuid.uuid4()) + "/"

    def setUp(self):
        if not os.path.exists(PythonResourceTest.TEST_DIR):
            os.makedirs(PythonResourceTest.TEST_DIR)

        self.resource = python_resource.Resource()
        pass

    def test_simple(self):
        files = ["A1", "A2", "rtbsk/B1", "rtbsk/B2", "rtbsk/__init__"]
        for _file in files:
            self.__touch(_file)

        self.resource.add_directory(PythonResourceTest.TEST_DIR,
                                    PythonResourceTest.TEST_DIR,
                                    False)

        added_files_list = map(lambda x: x[1][len(PythonResourceTest.TEST_DIR): len(x[1])],
                               self.resource.files_list())

        self.assertItemsEqual(files, added_files_list)

    def test_only_pys(self):
        files = ["A1", "A2", "A3.py", "rtbsk/B1", "rtbsk/B2", "rtbsk/__init__.pyc"]
        for _file in files:
            self.__touch(_file)

        self.resource.add_directory(PythonResourceTest.TEST_DIR,
                                    PythonResourceTest.TEST_DIR,
                                    True)

        added_files_list = map(lambda x: x[1][len(PythonResourceTest.TEST_DIR): len(x[1])],
                               self.resource.files_list())

        filtered_list = filter(lambda name: name.endswith(".py") or name.endswith(".pyc"),
                               files)
        self.assertItemsEqual(filtered_list, added_files_list)

    def test_ignore_hiddens(self):
        files = ["A1.py", ".A3.py", "rtbsk/A2.pyc", "rtbsk/.A4.py", ".rd/ab", ".rd/.rs", ".rd/B.py"]
        for _file in files:
            self.__touch(_file)

        self.resource.add_directory(PythonResourceTest.TEST_DIR,
                                    PythonResourceTest.TEST_DIR,
                                    True)
        added_files_list = map(lambda x: x[1][len(PythonResourceTest.TEST_DIR): len(x[1])],
                               self.resource.files_list())

        result_list = ["A1.py", "rtbsk/A2.pyc"]

        self.assertItemsEqual(result_list, added_files_list)

    def test_add_duplicated_file(self):
        """
        test add duplicated file
        """
        add_file = "dir/file1"
        target_file = "resource_dir/file1"

        self.resource.add_file(add_file, target_file)
        self.resource.add_file(add_file, target_file)
        expect_list = [(add_file, target_file, False)]
        self.assertListEqual(self.resource.files_list(), expect_list)

    def test_add_multiples(self):
        """
        unit-test for add same file and muliple different resource path.
        """
        add_file1 = "dir1/file1"
        target_file1 = "resource_dir1/file1"
        target_file2 = "resource_dir2/file2"
        target_file3 = "resource_dir3/file3"

        self.resource.add_file(add_file1, target_file1)
        self.resource.add_file(add_file1, target_file2)
        expect_list = [
                (add_file1, target_file1, False),
                (add_file1, target_file2, False),
                ]
        self.assertListEqual(self.resource.files_list(), expect_list)

        self.resource.add_file(add_file1, target_file3)
        expect_list.append((add_file1, target_file3, False))
        self.assertListEqual(self.resource.files_list(), expect_list)

    def test_duplicated_eggfile_name(self):
        """
        unit-test for add duplicated egg file name
        """
        egg_files = ["dir1/test1.egg", "dir2/test1.egg", "dir3/test1.egg"]
        for _ef in egg_files:
            self.__touch(_ef)
        self.resource.add_egg_file(egg_files[0])
        self.resource.add_egg_file(egg_files[1])

        expect_list = [os.path.basename(egg_files[0])]
        self.assertListEqual(self.resource.egg_file_target_list(), expect_list)

    def test_add_file_for_non_python_file(self):
        """
        unit-test for add non-python file
        """
        files = ["dir1/file1", "dir2/file2", "dir3/f.a", "dir4/f.so"]
        basename_files = [
                os.path.basename(files[0]), os.path.basename(files[1]),
                os.path.basename(files[2]), os.path.basename(files[3])]

        for _file in files:
            self.__touch(_file)

        resource_dir = "resource_dir/"
        self.resource.add_file(files[0], resource_dir + basename_files[0])
        self.resource.add_file(files[1], resource_dir + basename_files[1])
        self.resource.add_file(files[2], resource_dir + basename_files[2])
        self.resource.add_file(files[3], resource_dir + basename_files[3])

        expect_list = [
            (files[0], resource_dir + basename_files[0], False),
            (files[1], resource_dir + basename_files[1], False),
            (files[2], resource_dir + basename_files[2], False),
            (files[3], resource_dir + basename_files[3], False)]
        self.assertListEqual(self.resource.files_list(), expect_list)

    def tearDown(self):
        shutil.rmtree(PythonResourceTest.TEST_DIR, ignore_errors=True)
        pass

    def __touch(self, filepath):
        fullpath = PythonResourceTest.TEST_DIR + "/" + filepath
        base, file_name = os.path.split(fullpath)
        if len(base) > 0 and not os.path.exists(base):
            os.makedirs(base)
        open(fullpath, 'a').close()


if __name__ == "__main__":
    unittest.main()
