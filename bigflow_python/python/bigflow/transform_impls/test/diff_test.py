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
# @Last Modified time: 2015-08-03 19:01:12

import unittest
from bigflow.test import test_base
from bigflow import serde


class DiffTestCase(test_base.PipelineBasedTest):

    def test_intersection(self):
        a = self._pipeline.parallelize([1, 2, 3, 1, 4]).map(lambda x: x, serde = serde.of(int))
        b = self._pipeline.parallelize([1, 2, 1, 2, 3]).map(lambda x: x, serde = serde.of(int))
        diff = a.diff(b)
        diff_serde_str = str(diff.serde())
        expect_serde_str = str(serde.of((int, (int, int))))
        self.assertEqual(expect_serde_str, diff_serde_str)
        self.assertItemsEqual([(2, (1, 2)), (4, (1, 0))], diff.get())

if __name__ == "__main__":
    unittest.main()
