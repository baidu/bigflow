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
File: union_test.py
Author: Wang, Cong(bigflow-opensource@baidu.com)
Date: 2015/03/20 08:23:22
"""
import unittest

from bigflow.test import test_base


class UnionTest(test_base.PipelineBasedTest):

    def test_union_two(self):
        p1 = self._pipeline.parallelize([1, 9, 6, 2])
        p2 = self._pipeline.parallelize([3, 4, 8])

        p_result = p1.union(p2)

        self.assertItemsEqual([1, 9, 6, 2, 3, 4, 8], self._pipeline.get(p_result))

    def test_union_many(self):
        p1 = self._pipeline.parallelize([1])
        p2 = self._pipeline.parallelize([2, 3])
        p3 = self._pipeline.parallelize([4])

        p_result = p1.union(p2, p3)

        self.assertItemsEqual([1, 2, 3, 4], self._pipeline.get(p_result))

if __name__ == "__main__":
    unittest.main()
