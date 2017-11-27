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
Pagerank example unit test

Author: Wang, Cong(bigflow-opensource@baidu.com)
"""

import unittest

from bigflow.example import pagerank
from bigflow.test import test_base


class TestCase(test_base.PipelineBasedTest):
    def test_result(self):
        input_data = [(0, 0), (0, 1), (0, 3), (0, 4),
                      (1, 0), (1, 1),
                      (2, 0), (2, 2),
                      (3, 3),
                      (4, 0), (4, 1), (4, 4)]

        edges = self._pipeline.parallelize(input_data)

        result = edges.apply(pagerank.pagerank_algo).get()

        self.assertItemsEqual([(2, 1), (3, 110), (4, 110), (1, 177), (0, 178)], result)


if __name__ == "__main__":
    unittest.main()
