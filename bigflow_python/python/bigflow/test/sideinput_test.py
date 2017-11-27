#!/bin/env python
#-*- coding:utf-8 -*-
#
# Copyright (c) 2016 Baidu, Inc. All Rights Reserved.

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
PCollection as sideinput unit test

"""

import unittest
import re
from bigflow.test import test_base
from bigflow.util.log import logger

class TestCase(test_base.PipelineBasedTest):
    """
    PObject unit test
    """

    def setUp(self):
        """set up function"""
        super(TestCase, self).setUp()
        self.p1 = self._pipeline.parallelize([1,2,3])
        self.p2 = self._pipeline.parallelize([1,2])
        self.p3 = self._pipeline.parallelize(2)
        self.tests = {}

    def test_sideinput(self):
        p1_ = self.p1.map(lambda x, y: y, self.p2)
        p2_ = self.p1.map(lambda x, y: y, self.p3)
        p1_.cache()
        p2_.cache()

        expect_p1_ = [[1,2], [1,2], [1,2]]
        expect_p2_ = [2,2,2]

        p1 = sorted(p1_.get())
        p2 = sorted(p2_.get())
        self.assertEqual(len(p1), len(expect_p1_))
        self.assertEqual(len(p2), len(expect_p2_))

        for i in xrange(len(p1)):
            self.assertItemsEqual(p1[i], expect_p1_[i])

        self.assertItemsEqual(p2, expect_p2_)


if __name__ == "__main__":
    unittest.main()
