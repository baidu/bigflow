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
File: accumulate_test.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/03/10 15:41:43
"""

import unittest

from bigflow import transforms
from bigflow.test import test_base


class AccumulateTestCase(test_base.PipelineBasedTest):
    def test_accumulate_sum(self):
        data = self._pipeline.parallelize([1, 2, 3, 4])
        result = data.accumulate(0, lambda x, y: x + y)

        self.assertEqual(10, self._pipeline.get(result))

    def test_accumulate_sum_with_side_inputs(self):

        si = self._pipeline.parallelize(5)
        data = self._pipeline.parallelize([1, 2, 3, 4])
        result = data.accumulate(0, lambda x, y, side_input: x + y + side_input, si)

        self.assertEqual(30, self._pipeline.get(result))

    def test_accumulate_sum_with_side_inputs_ptable(self):

        si = self._pipeline.parallelize({1: 2, 2: 4, 3: 6, 4: 8})
        data = self._pipeline.parallelize([1, 2, 3, 4])
        result = data.accumulate(0, lambda o, i, side_input: o + side_input[i], si)

        self.assertEqual(20, self._pipeline.get(result))

    def test_accumulate_sum_with_side_inputs_ptable_apply_values(self):

        data = self._pipeline.parallelize({"A": [2, 3, 4], "B": [2, 6, 7]})

        si = self._pipeline.parallelize({2: 4, 3: 6, 4: 8, 6: 12, 7: 14})

        def f(p, si):
            si = si.group_by_key()
            return transforms.accumulate(p, 0, lambda o, i, side_input: o + side_input[i][0], si)

        result = data.apply_values(f, si.flatten())

        self.assertEqual({"A": 18, "B": 30}, self._pipeline.get(result))

    def test_accumulate_sum_with_callable_zero(self):
        si = self._pipeline.parallelize([2, 3])
        data = self._pipeline.parallelize([1, 2, 3, 4])
        result = data.accumulate(lambda side_input: reduce(lambda x, y: x + y, list(side_input)),
                                lambda x, y, side_input: x + y,
                                si)

        self.assertEqual(15, self._pipeline.get(result))

    def test_accumulate_sum_in_ptable(self):

        def add_to_set(x, y):
            x.add(y)
            return x

        data = self._pipeline.parallelize({"A": [2, 3, 4], "B": [2, 6, 7]})

        result = data.apply_values(transforms.accumulate,
                                   set(),
                                   add_to_set)
        result_data = result.get()

        self.assertEqual({"A": {2, 3, 4}, "B": {2, 6, 7}},
                         result_data)


if __name__ == "__main__":
    unittest.main()
