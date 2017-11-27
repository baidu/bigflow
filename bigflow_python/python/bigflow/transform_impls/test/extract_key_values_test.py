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
File: flatten_test.py
Author: Wang Cong(wangcong09@baidu.com)
Date: 2015/03/25 15:41:43
"""

import unittest

from bigflow.test import test_base


class ExtractKeyValuesTestCase(test_base.PipelineBasedTest):

    def test_extract_key(self):
        data = self._pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
        grouped = data.group_by_key()

        grouped_keys = grouped.extract_keys()

        self.assertItemsEqual(["A", "B"], grouped_keys.get())

    def test_extract_value(self):
        data = self._pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
        grouped = data.group_by_key()

        grouped_values = grouped.extract_values()

        self.assertItemsEqual([2, 4, 3, 1], grouped_values.get())

if __name__ == "__main__":
    unittest.main()
