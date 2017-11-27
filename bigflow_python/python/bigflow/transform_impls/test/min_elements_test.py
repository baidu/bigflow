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
File: min_elements_test.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/05/08 13:08:12
"""

import unittest

from bigflow.test import test_base
from bigflow import transforms


class MinElementsTestCase(test_base.PipelineBasedTest):

    def test_string(self):
        data = self._pipeline.parallelize(["a", "bb", "ccc", "ffee", "acde", "bggg"])
        min_str = transforms.min_elements(data, 3)

        self.assertItemsEqual(["a", "bb", "acde"], min_str.get())

    def test_simple(self):
        data = self._pipeline.parallelize([4, 3, 1, 2, 5, 8, 4, 0, 9, 6, 9, 7])
        mins = transforms.min_elements(data, 8)

        data2 = self._pipeline.parallelize([2, 2])
        trunc = transforms.min_elements(data2, 3)

        mins.cache()
        trunc.cache()

        self.assertItemsEqual([0, 1, 2, 3, 4, 4, 6, 5], mins.get())
        self.assertItemsEqual([2, 2], trunc.get())

    def test_after_grouping(self):
        data = self._pipeline.parallelize({"A": [4, 3, 1, 5, 9, 9],
                                           "B": [2, 4, 3]})
        expect_sort = sorted({"A": [1, 3, 4, 5], "B": [2, 3, 4]}.iteritems())
        actual_sort = sorted(data.apply_values(transforms.min_elements, 4).get().iteritems())

        final_expect = []
        final_actual = []
        for k in expect_sort:
            final_expect.append((k[0], sorted(k[1])))
        for k in actual_sort:
            final_actual.append((k[0], sorted(k[1])))
        self.assertItemsEqual(final_expect, final_actual)

if __name__ == "__main__":
    unittest.main()
