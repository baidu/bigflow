#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
#
# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.

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

import unittest

from bigflow.test import test_base
from bigflow import transforms

class TestCase(test_base.PipelineBasedTest):
    """
    TestCase for transforms.make_tuple
    """

    def test_make_tuple(self):
        """ test ok """
        p1 = self._pipeline.parallelize(1)
        p2 = self._pipeline.parallelize(2)
        p3 = self._pipeline.parallelize([3, 4, 5])

        self.passertEqual((1,), transforms.make_tuple(p1))

        self.passertEqual((1, 2), transforms.make_tuple(p1, p2))
        self.passertEqual((1, 1, 2), transforms.make_tuple(p1, p1, p2))

        p5 = p3.group_by(lambda x: x % 2) \
            .apply_values(transforms.sum) \
            .apply_values(lambda v: transforms.make_tuple(v, v))
        self.passertEqual({0: [(4, 4)], 1: [(8, 8)]}, p5)

        p6 = p3.group_by(lambda x: x % 2) \
            .apply_values(transforms.sum) \
            .apply_values(transforms.make_tuple)
        self.passertEqual({0: [(4,)], 1: [(8,)]}, p6)

    def test_make_tuple_fail(self):
        """ test fail """

        p1 = self._pipeline.parallelize(1)
        p3 = self._pipeline.parallelize([3, 4, 5])

        with self.assertRaises(AssertionError):
            transforms.make_tuple(p1, p3).get()

        with self.assertRaises(AssertionError):
            transforms.make_tuple(p1, p3).get()

        with self.assertRaises(AssertionError):
            p3.group_by(lambda x: x % 2) \
            .apply_values(lambda v: transforms.make_tuple(v, v)).get()

if __name__ == "__main__":
    unittest.main()
