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
WordCount example unit test

Author: Wang, Cong(bigflow-opensource@baidu.com)
"""

import unittest

from bigflow.example import word_cnt
from bigflow.test import test_base


class TestCase(test_base.PipelineBasedTest):

    def test_count_result(self):
        data = self._pipeline.parallelize(["To", "be", "or", "not", "to", "be"])
        result = word_cnt.count_words(data).get()
        self.assertItemsEqual(
            [("to", 1), ("not", 1), ("or", 1), ("be", 2), ("To", 1)],
            result)

if __name__ == "__main__":
    unittest.main()
