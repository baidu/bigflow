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
File: group_by_test.py
Author: Wang Cong(wangcong09@baidu.com)
Date: 2015/03/25 15:41:43
"""

import unittest

from bigflow.test import test_base
from bigflow import transforms
from bigflow import error


class GroupByTestCase(test_base.PipelineBasedTest):

    def test_value_extractor(self):
        """
        """
        data = self._pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
        grouped = transforms.group_by(data, lambda x: x, lambda x: x[1])

        self.assertEqual({("A", 1): [1],
                          ("B", 2): [2],
                          ("A", 3): [3],
                          ("A", 4): [4]},
                         self._pipeline.get(grouped))

    def test_default_value_extractor(self):
        """
        unit test for default value extractor of group by function
        """
        p = self._pipeline.parallelize([[1, 2], [3, 4]])
        self.assertEqual({2: [[1, 2]], 4: [[3, 4]]}, p.group_by(lambda x: x[1]).get())

    def test_group_by_key_fail(self):
        p = self._pipeline.parallelize([[1, 2], [3, 4, 5]]).group_by_key()
        self.assertRaises(error.BigflowRuntimeException, p.get)

if __name__ == "__main__":
    unittest.main()
