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
File: take_test.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/03/14 13:40:55
"""

import unittest

from bigflow.test import test_base

class TakeTestCase(test_base.PipelineBasedTest):

    def test_take_pobject(self):
        data = self._pipeline.parallelize([3, 7, 1, 9, 2, 8])
        uplimit_data = self._pipeline.parallelize(3)

        result = data.take(uplimit_data)

        data2 = self._pipeline.parallelize({'A': [3, 7, 1, 9, 2, 8], 'B': [2]})

        result2 = data2.apply_values(lambda x: x.take(x.count()))

        result.cache()
        result2.cache()

        self.assertTrue(set([3, 7, 1, 9, 2, 8]).issuperset(set(result.get())))
        self.assertEqual(3, len(result.get()))
        self._assertDictItemsEqual({'A': [3, 7, 1, 9, 2, 8], 'B': [2]}, result2.get())

    def test_take_int(self):
        data = self._pipeline.parallelize([3, 7, 1, 9, 2, 8])

        result = data.take(3)

        self.assertTrue(set([3, 7, 1, 9, 2, 8]).issuperset(set(result.get())))
        self.assertEqual(3, len(result.get()))

if __name__ == "__main__":
    unittest.main()
