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
File: distinct_test.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/03/27 15:08:29
"""

import unittest
from bigflow.test import test_base
from bigflow.transform_impls import sum
from bigflow.transform_impls import count


class DistinctTestCase(test_base.PipelineBasedTest):
    def setUp(self):
        super(DistinctTestCase, self).setUp()

        self._test_data1 = self._pipeline.parallelize([1, 1, 2, 2, 3, 3, 4, 4, 4, 5])
        self._test_data2 = self._pipeline.parallelize(["key0", "key1", "key2", "key1", "key0", "key10"])

    def test_distinct_sum(self):
        sum_of_nums = self._test_data1.distinct().apply(sum.sum)
        count_of_key = self._test_data2.distinct().apply(count.count)
        sum_of_nums.cache()
        count_of_key.cache()

        self.assertEqual(1 + 2 + 3 + 4 + 5, self._pipeline.get(sum_of_nums))
        self.assertEqual(4, self._pipeline.get(count_of_key))


if __name__ == "__main__":
    unittest.main()


