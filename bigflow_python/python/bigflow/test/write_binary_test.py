#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
@Author: zhenggonglin
@Date:   2016-02-27
"""
import os
import unittest
import subprocess
import uuid

from bigflow import error
from bigflow import input
from bigflow import output
from bigflow import serde
from bigflow import transforms
from bigflow.test import test_base
from bigflow.util.log import logger


class TestCase(test_base.PipelineBasedTest):

    def _compare_expect_data_and_output(self, expect_data, output_path):
        data = self._pipeline.read(input.TextFile(output_path)).get()
        self.assertItemsEqual(expect_data, data)

    @test_base.run_mode(fs = ["local", "hdfs"])
    def test_write_binary_set_by_user(self):
        # Set record delimiter by user
        raw_data = ["aaa", "bbb", "ccc"]
        special_record_delimiter = chr(2)+chr(3)+chr(4)
        record_delimiters = [
                "\t",
                "\r\n",
                special_record_delimiter,
        ]
        expect_data = [
            ["aaa\tbbb\tccc\t"],
            ["aaa", "bbb", "ccc"],
            [special_record_delimiter.join(raw_data) + special_record_delimiter],
        ]

        self.tmp_output_dirs = []
        for record_delimiter in record_delimiters:
            data = self._pipeline.parallelize(raw_data)

            output_dir = self.generate_tmp_path()
            self.tmp_output_dirs.append(output_dir)

            self._pipeline.write(data,
                    output.TextFile(output_dir, record_delimiter=record_delimiter))

        self._pipeline.run()

        for idx, output_dir in enumerate(self.tmp_output_dirs):
            self._compare_expect_data_and_output(expect_data[idx], output_dir)

    @test_base.run_mode(fs = ["local", "hdfs"])
    def test_write_binary_use_default(self):
        # Use default record delimiter
        raw_data = ["aaa", "bbb", "ccc"]
        data = self._pipeline.parallelize(raw_data)
        output_dir = self.generate_tmp_path()
        self._pipeline.write(data, output.TextFile(output_dir))
        self._pipeline.run()
        self._compare_expect_data_and_output(raw_data, output_dir)

    @test_base.run_mode(fs = ["local", "hdfs"])
    def test_write_binary_none(self):
        # Don't set record_delimiter, write binary.
        chars = [chr(i) for i in xrange(1, 10)]
        from random import shuffle
        from random import randint
        shuffle(chars)

        raw_data = []
        for cnt in xrange(100):
            index1 = randint(0, len(chars)-1)
            index2 = randint(0, len(chars)-1)
            raw_data.append("".join(chars[min(index1, index2): max(index1, index2)]))
        raw_data

        data = self._pipeline.parallelize(raw_data)
        output_dir = self.generate_tmp_path()
        self._pipeline.write(data, output.TextFile(output_dir, record_delimiter=None))
        self._pipeline.run()
        expect_data = ["".join(raw_data)]
        self._compare_expect_data_and_output(expect_data, output_dir)

if __name__ == "__main__":
    unittest.main()
