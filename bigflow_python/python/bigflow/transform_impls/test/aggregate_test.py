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
File: aggregate_test.py
Author: panyunhong(bigflow-opensource@baidu.com)
Date: 2015/03/10 15:41:43
"""

import unittest

from bigflow import transforms
from bigflow.test import test_base

def aggregate_fn(current, elements, side_input):
    return current + elements + side_input


def combine_fn(o1, o2, side_input):
    return o1 + o2


class AggregateTestCase(test_base.PipelineBasedTest):

    def test_aggregate_sum(self):
        data = self._pipeline.parallelize([2, 3, 4])
        side_input = self._pipeline.parallelize(1000)
        result = data.aggregate(0, aggregate_fn, combine_fn, side_input)

        self.assertEqual(3009, self._pipeline.get(result))

    def test_aggregate_in_ptable(self):

        def add_to_set(x, y):
            x.add(y)
            return x

        data = self._pipeline.parallelize({"A": [2, 3, 4], "B": [2, 6, 7]})

        result = data.apply_values(transforms.aggregate,
                                   set(),
                                   add_to_set,
                                   lambda x, y: x.union(y))
        result_data = result.get()

        self.assertEqual({"A": {2, 3, 4}, "B": {2, 6, 7}},
                         result_data)

    def test_aggregate_with_side_input_ptable(self):

        si = self._pipeline.parallelize({2: 4, 3: 6, 4: 8, 6: 12, 7: 14})
        data = self._pipeline.parallelize([2, 3, 4])
        result = data.aggregate(0, lambda o, i, d: o + d[i], lambda x, y, d: x + y, si)
        result_data = result.get()

        self.assertEqual(18, result_data)

if __name__ == "__main__":
    unittest.main()

