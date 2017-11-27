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
Author: Zhang Yuncong <zhangyuncong@baidu.com>
"""
import unittest

from bigflow.test import test_base
from bigflow import serde
from bigflow import transforms

class TestCase(test_base.PipelineBasedTest):

    def test_serde(self):
        p = self._pipeline.parallelize([1, 2])

        p = p.map(lambda x: x, serde = serde.of(int))
        q = p.map(lambda x: x + 1, serde = serde.of(int))
        o = q.map(lambda x: str(x + 1), serde= serde.of(str))

        result = p.cartesian(q)
        self.assertEqual(str(serde.tuple_of(int, int)), str(result.serde()))

        result = p.cartesian(q, o)
        self.assertEqual(str(serde.sample((1, 2, '3'))), str(result.serde()))

    def check_cartesian(self, expect, *pvalues):
        self.passertEqual(expect, pvalues[0].cartesian(*pvalues[1:]))
        self.passertEqual(expect, transforms.cartesian(*pvalues))

    def test_cartesian(self):
        p1 = self._pipeline.parallelize(1)
        p2 = self._pipeline.parallelize([2, 3])
        p3 = self._pipeline.parallelize([4, 5, 6])

        self.check_cartesian([(1, 1)], p1, p1)
        self.check_cartesian([(1, 2), (1, 3)], p1, p2)
        self.check_cartesian([(1, 2, 1), (1, 3, 1)], p1, p2, p1)
        self.check_cartesian([(2, 1), (3, 1)], p2, p1)
        self.check_cartesian([(2, 2), (2, 3), (3, 2), (3, 3)], p2, p2)
        self.check_cartesian([(2, 1, 2), (2, 1, 3), (3, 1, 2), (3, 1, 3)], p2, p1, p2)
        self.check_cartesian([(2, 4), (2, 5), (2, 6), (3, 4), (3, 5), (3, 6)], p2, p3)


if __name__ == "__main__":
    unittest.main()
