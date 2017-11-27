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
# @Last Modified time: 2015-06-30 22:56:51

import unittest
from bigflow.test import test_base


class IsEmptyTestCase(test_base.PipelineBasedTest):

    def test_is_empty(self):
        a = self._pipeline.parallelize([1, 2, 3])
        b = self._pipeline.parallelize([])

        a_is_empty = a.is_empty()
        b_is_empty = b.is_empty()

        a_is_empty.cache()
        b_is_empty.cache()

        self.assertEqual(False, a_is_empty.get())
        self.assertEqual(True, b_is_empty.get())

if __name__ == "__main__":
    unittest.main()
