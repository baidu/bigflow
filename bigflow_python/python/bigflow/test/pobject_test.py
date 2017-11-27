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
PObject unit test

Author: Wen, Chunyang(wenchunyang@baidu.com)
"""

import unittest
import re
from bigflow.test import test_base

class TestCase(test_base.PipelineBasedTest):
    """
    PObject unit test
    """

    def setUp(self):
        """set up function"""
        super(TestCase, self).setUp()
        self.p1 = self._pipeline.parallelize(1)
        self.p2 = self._pipeline.parallelize(2)
        self.p3 = self._pipeline.parallelize(16)
        self.p4 = self._pipeline.parallelize(1.356)
        self.p5 = self._pipeline.parallelize(-1)
        self.p6 = self._pipeline.parallelize(0)

    def operator_combinations(self):
        """
        Case: test combinations of operators
        """
        self._test_this("33 == (p1 + p2 * p3)")
        self._test_this("-31 == (p1 - p2 * p3)")
        self._test_this("3 == (2 * p1 + 1)")
        self._test_this("1 == ((2 * p1 + 1)/2)")

    def _test_this(self, expr):
        """
        tests _test_this: construct test dict
        """

        from operator import pow
        from operator import mod
        from operator import abs
        from operator import neg
        from operator import invert
        from operator import pos
        from math import ceil
        from math import floor
        import re

        if "tests" not in dir(self):
            self.tests = {}
        exprs = re.split(" *== *", expr, 1)
        lhs = re.sub(r"(p\d+)", r"self.\1", exprs[0])
        rhs = re.sub(r"(p\d+)", r"self.\1", exprs[1])
        exec("lhs=" + lhs)
        exec("rhs=" + rhs)
        self.tests[lhs] = (rhs, expr)
        rhs.cache()

    def run_tests(self):
        """
        run all tests
        """
        for lhs, rhs in self.tests.iteritems():
            self.assertEqual(lhs, rhs[0].get(), rhs[1])

    def binary_op(self):
        """
        Case: test binary operations
        """
        # mathametical
        self._test_this("4 == (1 + p1 + p2)")
        self._test_this("3 == (6 - p1 - p2)")
        self._test_this("6 == (3 * p1 * p2)")
        self._test_this("1.5 == (3.0 / p1 / p2)")
        self._test_this("8 == pow(p2, 3)")
        self._test_this("1 == mod(p3, 3)")

        # bit
        self._test_this("8 == (p2 << 2)")
        self._test_this("8 == (2 << p2)")
        self._test_this("4 == (p3 >> 2)")
        self._test_this("0 == (p3 & 2)")
        self._test_this("18 == (2 | p3)")
        self._test_this("18 == (p3 ^ 2)")

        # logical
        self._test_this("0 == (p1 == p2)")
        self._test_this("1 == (p1 != p2)")
        self._test_this("1 == (p1 < p2)")
        self._test_this("0 == (p1 > p2)")
        self._test_this("0 == (1 == p2)")
        self._test_this("1 == (p1 == 1)")

    def unary_op(self):
        """
        Case: test unary operations
        """
        self._test_this("1 == abs(p5)")
        self._test_this("1 == neg(p5)")
        self._test_this("-1.356 == (-p4)")
        self._test_this("-1 == (+p5)")
        self._test_this("-3 == (~p2)")
        self._test_this("True == p6.not_()")
        self._test_this("False == p5.not_()")
        self._test_this("ceil(1.356) == p4.ceil()")
        self._test_this("floor(1.356) ==  p4.floor()")
        self._test_this("round(1.356, 2) == p4.round(2)")

    def test_all(self):
        """
        Unit tests entrance
        """
        self.unary_op()
        self.binary_op()
        self.operator_combinations()
        self.run_tests()

    def test_as_pcollection(self):
        from bigflow import transforms
        # test normal
        o = self._pipeline.parallelize(2)
        self.passertEqual([2], o.as_pcollection())
        self.passertEqual(1, o.as_pcollection().count())

        # in apply_values
        p = self._pipeline.parallelize(range(3))
        r = p.group_by(lambda n: n % 2) \
            .apply_values(transforms.sum) \
            .apply_values(lambda po: po.as_pcollection()) \
            .apply_values(transforms.filter, lambda n: n == 1) \
            .apply_values(transforms.sum) \
            .apply_values(lambda po: po.as_pcollection().count())
        self.passertEqual({0: 0, 1: 1}, r)


if __name__ == "__main__":
    unittest.main()
