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
Author: Wang Cong(bigflow-opensource@baidu.com)
"""

import unittest

from bigflow.test import test_base
from bigflow import transforms


class MaxElementsTestCase(test_base.PipelineBasedTest):

    def test_string(self):
        data = self._pipeline.parallelize(["a", "bb", "ccc", "ffee", "acde", "bggg"])
        max_str = transforms.max_elements(data, 3)

        self.assertItemsEqual(["ffee", "ccc", "bggg"], max_str.get())

    def test_simple(self):
        data = self._pipeline.parallelize([4, 3, 1, 2, 5, 8, 4, 0, 9, 6, 9, 7])
        maxs = transforms.max_elements(data, 8)

        data2 = self._pipeline.parallelize([2, 2])
        trunc = transforms.max_elements(data2, 3)

        maxs.cache()
        trunc.cache()

        self.assertItemsEqual([4, 4, 5, 6, 7, 8, 9, 9], maxs.get())
        self.assertItemsEqual([2, 2], trunc.get())

    def test_after_grouping(self):
        data = self._pipeline.parallelize({"A": [4, 3, 1, 5, 9, 9],
                                           "B": [2, 4, 3]})
        expect_sort = sorted({"A": [9, 9, 4, 5], "B": [2, 3, 4]}.iteritems())
        actual_sort = sorted(data.apply_values(transforms.max_elements, 4).get().iteritems())

        final_expect = []
        final_actual = []
        for k in expect_sort:
            final_expect.append((k[0], sorted(k[1])))
        for k in actual_sort:
            final_actual.append((k[0], sorted(k[1])))
        self.assertItemsEqual(final_expect, final_actual)

if __name__ == "__main__":
    unittest.main()
