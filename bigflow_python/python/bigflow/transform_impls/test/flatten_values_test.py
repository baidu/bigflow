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
File: flatten_values_test.py
Author: Wang Cong(bigflow-opensource@baidu.com)
Date: 2015/03/25 15:41:43
"""

import unittest

from bigflow.test import test_base
from bigflow import transforms


class FlattenValuesTestCase(test_base.PipelineBasedTest):

    def test_flatten(self):
        data = self._pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
        grouped = data.group_by_key()
        values = grouped.flatten_values()

        self.assertItemsEqual([2, 4, 3, 1], self._pipeline.get(values))

    def test_nested_flatten(self):

        def to_tuple_list(elem):
            tuple2 = (chr(elem + ord('a') - 1), elem)
            return [tuple2, tuple2]

        data = self._pipeline.parallelize([("A", 1), ("B", 2), ("C", 3), ("D", 4)])
        grouped = data.group_by_key().apply_values(transforms.flat_map, to_tuple_list)
        nested = grouped.apply_values(transforms.group_by_key)
        self.assertEqual(1, nested.nested_level())

        values = nested.flatten_values()
        values2 = nested.apply_values(transforms.flatten_values)

        values.cache()
        values2.cache()

        self.assertItemsEqual([2, 2, 4, 4, 3, 3, 1, 1], self._pipeline.get(values))
        self.assertEqual(0, values2.nested_level())
        self.assertEqual({"A": [1, 1], "C": [3, 3], "B": [2, 2], "D": [4, 4]},
                         self._pipeline.get(values2))


if __name__ == "__main__":
    unittest.main()
