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
File: map_integration_test.py
Author: Wang, Cong(bigflow-opensource@baidu.com)
Date: 2015/03/20 08:23:22
"""
import unittest

from bigflow.test import test_base


class TestCase(test_base.PipelineBasedTest):

    def test_normal(self):
        p = self._pipeline.parallelize([1, 9, 6, 2]).map(lambda x: x * 2)

        p_result = self._pipeline.get(p)

        self.assertItemsEqual([2, 18, 12, 4], p_result)

    def test_map_with_side_input_ptable(self):

        si = self._pipeline.parallelize({2: 4, 3: 6, 4: 8})
        data = self._pipeline.parallelize([2, 3, 4])

        result = data.map(lambda i, d: d[i], si)
        result_data = result

        self.passertEqual([4, 6, 8], result_data)

if __name__ == "__main__":
    unittest.main()
