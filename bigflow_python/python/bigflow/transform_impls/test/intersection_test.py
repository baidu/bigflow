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
# @Last Modified time: 2015-06-30 22:21:02

import unittest
from bigflow.test import test_base


class IntersectionTestCase(test_base.PipelineBasedTest):

    def test_intersection(self):
        a = self._pipeline.parallelize([1, 2, 3, 1, 3, 5])
        b = self._pipeline.parallelize([1, 2, 1, 4, 2, 3])

        without_distinct = a.intersection(b)
        with_distinct = a.intersection(b, True)

        self.assertItemsEqual([1, 2, 3], without_distinct.get())
        self.assertItemsEqual([1, 1, 2, 3], with_distinct.get())

if __name__ == "__main__":
    unittest.main()
