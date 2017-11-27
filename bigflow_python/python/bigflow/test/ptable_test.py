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
PTable unit test

Author: Wang, Cong(wangcong09@baidu.com)
"""

import unittest

from bigflow.test import test_base
from bigflow import transforms
from bigflow import output
from bigflow import error

class TestCase(test_base.PipelineBasedTest):

    def test_from_pcollection(self):
        data = self._pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
        grouped = data.group_by_key()
        self.assertEqual(0, grouped.nested_level())

        grouped_count = grouped.apply_values(transforms.count)
        self.assertEqual({"A": 3, "B": 1}, grouped_count.get())

    def test_from_parallelize(self):
        grouped_object = self._pipeline.parallelize({"A": 3, "B": 1})
        self.assertEqual(0, grouped_object.nested_level())

        self.assertEqual({"A": 3, "B": 1}, grouped_object.get())

        grouped_collection = self._pipeline.parallelize({"A": [4, 3, 1], "B": [1]})
        self.assertEqual(0, grouped_collection.nested_level())
        self.assertEqual({"A": [4, 3, 1], "B": [1]}, grouped_collection.get())

    def test_from_parallelize_and_group_by(self):
        grouped_collection = self._pipeline.parallelize({"A": [4, 3, 1], "B": [1]})
        grouped = grouped_collection.apply_values(transforms.count)
        self.assertEqual({"A": 3, "B": 1}, grouped.get())

    def test_get_nested_cache(self):
        data = self._pipeline.parallelize({"A": [4, 3, 1], "B": [1]})
        flat = data.flatten()
        data.cache()
        flat.cache()
        self.assertEqual({"A": [4, 3, 1], "B": [1]}, data.get())
        self.assertItemsEqual([("A", 4), ("A", 3), ("A", 1), ("B", 1)], flat.get())

    def test_apply_values(self):
        """
        Case: test apply values
        """
        data = self._pipeline.parallelize({'A': [1, 2, 3, 1, 2], 'B': [4, 5, 3, 4, 5]})
        side_input = self._pipeline.parallelize([1, 3, 4])
        def _apply_value_func(data, *args, **kargs):
            si = args[0]
            if kargs['action'] == 'inter':
                result = data.filter(lambda x, y: x in y, si)
            return result
        result = data.apply_values(_apply_value_func, side_input, action='inter')
        self._assertDictItemsEqual({'A': [1, 3, 1], 'B': [4, 3, 4]}, result.get())

    def test_apply_values_tuple(self):
        """
        Case: test apply values tuple
        """
        ptable = self._pipeline.parallelize({'A': [1, 2], 'B': [4, 5]})
        def _apply_value_func(data):
            data1 = data.map(lambda x: x + 1)
            data2 = data.map(lambda x: x + 2)
            data3 = data.map(lambda x: x + 3)
            return (data1, data2, data3)
        result = ptable.apply_values(_apply_value_func)
        self._assertDictItemsEqual(
                {'A': [2, 3], 'B': [5, 6]}, result.apply_values(lambda x, y, z: x).get())
        self._assertDictItemsEqual(
                {'A': [3, 4], 'B': [6, 7]}, result.apply_values(lambda x, y, z: y).get())
        self._assertDictItemsEqual(
                {'A': [4, 5], 'B': [8, 7]}, result.apply_values(lambda x, y, z: z).get())

    @test_base.run_mode(mode='local')
    def test_cogroup_ptable(self):
        """
        Case: test cogroup ptable
        """

        data1 = self._pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
        data2 = self._pipeline.parallelize([("A", 8), ("A", 6), ("B", 4), ("A", 2)])
        ptable = transforms.cogroup(data1, data2)
        with self.assertRaises(error.BigflowPlanningException):
            ptable.flatten()

        def _test_value_fn(a, b, c, d, e):
            self.assertEqual(200, e)
            return transforms.union(a, b, c, d)

        si = self._pipeline.parallelize([100])
        result = ptable.apply_key_values(_test_value_fn, si, 200).flatten()

        expect = [('A', 'A'),
                   ('A', 4),
                   ('A', 3),
                   ('A', 1),
                   ('A', 8),
                   ('A', 6),
                   ('A', 2),
                   ('A', 100),
                   ('B', 'B'),
                   ('B', 2),
                   ('B', 4),
                   ('B', 100)]
        result = result.get()
        self.assertItemsEqual(expect, result)

if __name__ == "__main__":
    unittest.main()
