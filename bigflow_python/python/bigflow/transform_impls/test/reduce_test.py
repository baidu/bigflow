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
Author: Wang, Cong(wangcong09@baidu.com)
"""
import unittest

from bigflow import transforms
from bigflow.test import test_base


class TestCase(test_base.PipelineBasedTest):

    def test_normal(self):
        inp = [1, 9, 6, 2]
        fn = lambda x, y: x + y
        expect = reduce(fn, inp)
        result = self._pipeline.parallelize(inp).reduce(fn).get()
        self.assertEqual(expect, result)

    def test_side_input(self):
        inp = [1, 9, 6, 2]
        fn = lambda x, y: x + y
        expect = reduce(fn, inp)
        self._pipeline.parallelize(1)
        result = self._pipeline.parallelize(inp).reduce(fn).get()
        self.assertEqual(expect, result)

    def test_reduce_with_side_input_ptable(self):

        si = self._pipeline.parallelize({1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6})
        data = self._pipeline.parallelize([1, 2, 3])
        result = data.reduce(lambda x, y, d: d[x] + d[y], si)
        result_data = result.get()

        self.assertEqual(6, result_data)

    def test_modify_left_param(self):
        """ inner function"""
        inp = self._pipeline.parallelize([[1, 2, 3], [6, 5, 4]])

        def _sum(x, y):
            x[0] += y[0]
            x[1] += y[1]
            x[2] += y[2]
            return x

        result = transforms.union(inp.reduce(_sum), inp.reduce(_sum)).get()
        self.assertEqual([[7, 7, 7], [7, 7, 7]], result)

if __name__ == "__main__":
    unittest.main()
