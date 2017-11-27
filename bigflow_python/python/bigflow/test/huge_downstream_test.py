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
"""
@Author: zhangyuncong
"""
import unittest

from bigflow.test import test_base
from bigflow import transforms
from bigflow import error
from bigflow import input
from bigflow import output


class TestCase(test_base.PipelineBasedTest):
    """ test """
    def test_100_downstream(self):
        """ test """
        nums = self._pipeline.parallelize([1, 2])

        downstreams = []
        for i in xrange(100):
            downstreams.append(nums.map(lambda x: x))

        self.assertEqual(100 * 3, transforms.union(*downstreams).sum().get())

if __name__ == "__main__":
    unittest.main()
