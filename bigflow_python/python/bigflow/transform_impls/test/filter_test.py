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
        p = self._pipeline.parallelize([1, 9, 6, 2])
        filtered = p.filter(lambda x: x % 2 == 0)

        p_result = self._pipeline.get(filtered)

        self.assertItemsEqual([6, 2], p_result)

    def test_side_input(self):
        p = self._pipeline.parallelize([1, 9, 6, 2])
        two = self._pipeline.parallelize(2)
        filtered = p.filter(lambda x, y: x % y == 0, two)

        p_result = self._pipeline.get(filtered)

        self.assertItemsEqual([6, 2], p_result)

    def test_with_side_input_ptable(self):
        p = self._pipeline.parallelize([1, 9, 6, 2])
        si = self._pipeline.parallelize({6: 'a', 2: 'b'})
        filtered = p.filter(lambda x, y: y.has_key(x), si)

        p_result = self._pipeline.get(filtered)

        self.assertItemsEqual([6, 2], p_result)

    def test_in_apply_values(self):
        p = self._pipeline.parallelize({"A": [1, 9, 6, 2], "B": [2, 3, 1, 5]})
        dict = self._pipeline.parallelize([2, 6])
        filtered = p.apply_values(transforms.filter, lambda x, y: x in y, dict)

        p_result = self._pipeline.get(filtered)

        self.assertEqual({"A": [6, 2], "B": [2]}, p_result)


if __name__ == "__main__":
    unittest.main()
