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
Unit test for utils

Author: Wang, Cong(bigflow-opensource@baidu.com)
"""

import unittest

from bigflow import pobject
from bigflow import pcollection
from bigflow import ptable
from bigflow import base

from bigflow.util import utils


class UtilsTestCase(unittest.TestCase):

    def test_flatten_runtime_value(self):
        d1 = "A"
        d2 = (1, 3)

        self.assertEqual([d1], utils.flatten_runtime_value(d1))
        self.assertEqual([d2], utils.flatten_runtime_value(d2))

        d3 = ["A", "B"]
        d4 = [(1, 2), (3, 4)]

        self.assertEqual(d3, utils.flatten_runtime_value(d3))
        self.assertEqual(d4, utils.flatten_runtime_value(d4))

        d5 = {"A": {"b": [1, 5]}, "D": {"c": [2], "d": [1]}, "C": {"c": [0]}}
        expected = [("A", "b", 1), ("A", "b", 5), ("D", "c", 2), ("D", "d", 1), ("C", "c", 0)]

        self.assertItemsEqual(expected, utils.flatten_runtime_value(d5))

    def test_detect_ptype(self):
        d1 = "A"
        d2 = (1, 3)

        self.assertEqual((-1, pobject.PObject), utils.detect_ptype(d1))
        self.assertEqual((-1, pobject.PObject), utils.detect_ptype(d2))

        d3 = ["A", "B"]
        d4 = [(1, 2), (3, 4)]

        self.assertEqual((-1, pcollection.PCollection), utils.detect_ptype(d3))
        self.assertEqual((-1, pcollection.PCollection), utils.detect_ptype(d4))

        d5 = {"A": {"b": [1, 5]}, "D": {"c": [2, 2], "d": [1, 2]}, "C": {"c": [0, 3, 3]}}
        d6 = {"A": 1, "B": 2}

        self.assertEqual((1, pcollection.PCollection), utils.detect_ptype(d5))
        self.assertEqual((0, pobject.PObject), utils.detect_ptype(d6))

    def test_construct(self):
        fake_pipeline = base.Pipeline.create('local')
        fake_node = "Fake Node"

        _pobject = utils.construct(fake_pipeline, fake_node, pobject.PObject)
        self.assertTrue(isinstance(_pobject, pobject.PObject))

        _pcollection = utils.construct(fake_pipeline, fake_node, pcollection.PCollection)
        self.assertTrue(isinstance(_pcollection, pcollection.PCollection))

        _ptable1 = utils.construct(fake_pipeline, fake_node, ptable.PTable, 0, pcollection.PCollection)
        self.assertEqual(0, _ptable1.nested_level())
        self.assertEqual(pcollection.PCollection, _ptable1.inner_most_type())

        _ptable1 = utils.construct(fake_pipeline, fake_node, ptable.PTable, 2, pobject.PObject)
        self.assertEqual(2, _ptable1.nested_level())
        self.assertEqual(pobject.PObject, _ptable1.inner_most_type())

if __name__ == "__main__":
    unittest.main()
