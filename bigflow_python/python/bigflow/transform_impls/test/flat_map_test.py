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
File: flat_map_integration_test.py
Author: Wang, Cong(wangcong09@baidu.com)
Date: 2015/03/20 08:23:22
"""
import unittest

from bigflow.test import test_base


class TestCase(test_base.PipelineBasedTest):

    def test_normal(self):
        p = self._pipeline.parallelize([1, 9, 6, 2])
        mapped = p.flat_map(lambda x: [x * 2])

        p_result = self._pipeline.get(mapped)

        self.assertItemsEqual([2, 18, 12, 4], p_result)

    def test_with_side_input(self):
        si = self._pipeline.parallelize([2, 3])
        p = self._pipeline.parallelize([1, 9, 6, 2])
        mapped = p.flat_map(lambda x, y: [x * 2 + reduce(lambda a, b: a + b, y)], si)

        p_result = self._pipeline.get(mapped)

        self.assertItemsEqual([7, 23, 17, 9], p_result)

    def test_with_side_input_ptable(self):
        si = self._pipeline.parallelize({1: 'a', 2: 'b'})
        p = self._pipeline.parallelize([1, 2])
        mapped = p.flat_map(lambda x, y: y[x], si)

        p_result = self._pipeline.get(mapped)

        self.assertItemsEqual(['a','b'], p_result)

    def test_yield_fn(self):

        def yield_fn(x):
            for elem in x:
                yield elem

        p = self._pipeline.parallelize([[1, 2], [8, 9], [6, 7], [2, 3]])

        p_result = p.flat_map(yield_fn)

        self.assertItemsEqual([1, 2, 2, 3, 8, 9, 6, 7], p_result.get())

    def test_yield_fn_fail(self):
        """ test """

        def yield_fn(x):
            """ test """
            for elem in x:
                yield elem

            yield 1 / 0

        p = self._pipeline.parallelize([[1, 2], [8, 9], [6, 7], [2, 3]])

        p_result = p.flat_map(yield_fn)

        with self.assertRaises(Exception):
            p_result.get()

    def test_error_return(self):
        """ test """

        def fn(x):
            pass

        p = self._pipeline.parallelize([[1, 2], [8, 9], [6, 7], [2, 3]])

        p_result = p.flat_map(fn)

        with self.assertRaises(Exception):
            p_result.get()

if __name__ == "__main__":
    unittest.main()
