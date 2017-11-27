#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @Author: zhangyuncong
# @Date:   2015-06-30 20:27:08
# @Last Modified by:   zhangyuncong
# @Last Modified time: 2015-06-30 21:30:55

import unittest
from bigflow.test import test_base


class SubtractTestCase(test_base.PipelineBasedTest):

    def test_subtract(self):
        a = self._pipeline.parallelize([1, 2, 3])
        b = self._pipeline.parallelize([1, 2, 4, 5])

        s1 = a.subtract(b)
        s2 = b.subtract(a)

        s1.cache()
        s2.cache()

        self.assertEqual([3], s1.get())
        self.assertItemsEqual([4, 5], s2.get())

    def test_substract_with_duplicated(self):
        a = self._pipeline.parallelize([1, 2, 3, 1, 3])
        b = self._pipeline.parallelize([1, 2, 4, 5, 4])

        s1 = a.subtract(b)
        s2 = b.subtract(a)

        s1.cache()
        s2.cache()

        self.assertEqual([3, 3], s1.get())
        self.assertItemsEqual([4, 4, 5], s2.get())


if __name__ == "__main__":
    unittest.main()
