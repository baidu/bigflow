#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
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
########################################################################
"""
"""
import unittest

from bigflow import transforms
from bigflow import base
from bigflow import udf
from bigflow.test import test_base

class TestUDF(test_base.PipelineBasedTest):
    """
    the test of user_defined_functions.py
    """
    def test_udf(self):
        p1 = self._pipeline.parallelize([1, 2, 3])
        p2 = self._pipeline.parallelize([4, 5, 6])

        mp1 = p1.apply(udf.map(lambda x: x+10))
        mp2 = p2.apply(udf.map(lambda x: x+20))

        expect_mp1 = [11, 12, 13]
        expect_mp2 = [24, 25, 26]
        mp1 = mp1.get()
        mp2 = mp2.get()
        self.assertEqual(mp1, expect_mp1)
        self.assertEqual(mp2, expect_mp2)


        p3 = self._pipeline.parallelize([(1,1), (1,2), (1,3), (2,4), (2,5), (2,6)])
        p4 = self._pipeline.parallelize([(10,1), (10,2), (10,3), (20,4), (20,5), (20,6)])

        p3 = p3.group_by_key()
        p4 = p4.group_by_key()

        mp3 = p3.apply_values(udf.map(lambda x: x + 100)) \
                .flatten().get()
        mp4 = p4.apply_values(udf.map(lambda x: x + 200)) \
                .flatten().get()

        expect_mp3 = [(1,101), (1, 102), (1, 103), (2, 104), (2, 105), (2, 106)]
        expect_mp4 = [(10,201), (10, 202), (10, 203), (20, 204), (20, 205), (20, 206)]

        self.assertEqual(sorted(mp3), sorted(expect_mp3))
        self.assertEqual(sorted(mp4), sorted(expect_mp4))

        p = self._pipeline.parallelize([(1,1), (1,2), (2,3), (2,4)])
        p5 = p.group_by_key() \
                .apply_values(udf.sum()) \
                .flatten()
        p6 = p.group_by_key() \
                .apply_values(transforms.sum) \
                .flatten()
        sp5 = p5.get()
        sp6 = p6.get()

        expect_mp5 = [(1,3), (2,7)]
        expect_mp6 = [(1,3), (2,7)]
        self.assertEqual(sorted(sp5), sorted(expect_mp5))
        self.assertEqual(sorted(sp6), sorted(expect_mp6))


if __name__ == "__main__":
    unittest.main()
