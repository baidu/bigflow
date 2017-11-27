#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""
GroupedTop3 example unit test

Author: Wang, Cong(bigflow-opensource@baidu.com)
"""

import unittest

from bigflow.example import grouped_top_3
from bigflow.test import test_base


class TestCase(test_base.PipelineBasedTest):
    def test_result(self):
        input_data = self._pipeline.parallelize([1, 2, 3, 9, 8, 7, 4, 5, 6, 10, 11, 12, 15, 14, 13])

        result = input_data.apply(grouped_top_3.grouped_top_3).get()

        self.assertItemsEqual(
            [(0, 4), (0, 8), (0, 12), (3, 7), (3, 11), (3, 15),
             (2, 6), (2, 10), (2, 14), (1, 5), (1, 9), (1, 13)],
            result)


if __name__ == "__main__":
    unittest.main()
