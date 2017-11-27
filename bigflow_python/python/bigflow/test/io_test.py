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
@Author: zhangyuncong
@Date:   2016-01-13 20:50:20
@Last Modified by:   zhangyuncong
@Last Modified time: 2016-01-15 11:07:04
"""
import unittest

from bigflow import error
from bigflow import input
from bigflow import output
from bigflow import serde
from bigflow import transforms
from bigflow.test import test_base


class TestCase(test_base.PipelineBasedTest):
    """ test """

    @test_base.run_mode(mode=["local", "hadoop"])
    def test_user_define_input_format(self):
        """ test """
        class _FakeInputFormat(input.UserInputBase):

            def split(self):
                """ inner """
                return [1, 2, 3]

            def load(self, split):
                """ inner """
                return xrange(split)

            def post_process(self, pval):
                """ inner """
                return pval

        class _FakeOutputFormat(output.UserOutputBase):

            def open(self, part):
                """ inner """
                assert part == 0
                self.outputs = []

            def partition_number(self):
                """ inner """
                return 1

            def partition_fn(self):
                """ inner """
                return lambda record, n: 0

            def sink(self, data):
                """ inner """
                return self.outputs.append(data)

            def close(self):
                """ inner """
                assert sorted(self.outputs) == sorted([0, 1, 3])

        datas = self._pipeline.read(_FakeInputFormat())
        datas = datas.apply_values(transforms.sum).flatten_values()
        self._pipeline.write(datas, _FakeOutputFormat())
        self._pipeline.run()

    def test_output_sort(self):
        self.setConfig(spark_conf={
            "spark.default.parallelism": "1",
        })
        """ test """

        lines = self._pipeline.parallelize([5, 1, 2, 0, 3, 4])\
                .map(lambda x: str(x), serde=serde.of(str))

        out1_path = self.generate_tmp_path() + '/output-1/'
        out2_path = self.generate_tmp_path() + '/output-2/'
        self._pipeline.write(lines,
            output.TextFile(out1_path)
                .sort()
                .partition(n = 2, partition_fn = lambda x, n: int(x) % n)
        )
        self._pipeline.write(lines,
            output.TextFile(out2_path)
                .sort(reverse=True)
                .partition(n = 2, partition_fn = lambda x, n: int(x) % n)
        )
        self._pipeline.run()
        l11 = self._pipeline.read(input.TextFile(out1_path + '/part-00000'))\
                 .accumulate('', lambda x, y: x + y)
        l12 = self._pipeline.read(input.TextFile(out1_path + '/part-00001'))\
                 .accumulate('', lambda x, y: x + y)

        l21 = self._pipeline.read(input.TextFile(out2_path + '/part-00000'))\
                 .accumulate('', lambda x, y: x + y)
        l22 = self._pipeline.read(input.TextFile(out2_path + '/part-00001'))\
                 .accumulate('', lambda x, y: x + y)
        l11.cache()
        l12.cache()
        l21.cache()
        l22.cache()
        self.assertEqual('024', l11.get())
        self.assertEqual('135', l12.get())
        self.assertEqual('420', l21.get())
        self.assertEqual('531', l22.get())


if __name__ == "__main__":
    unittest.main()
