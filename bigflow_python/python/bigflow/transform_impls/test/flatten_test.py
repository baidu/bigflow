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
File: flatten_test.py
Author: Wang Cong(bigflow-opensource@baidu.com)
Date: 2015/03/25 15:41:43
"""

import unittest

from bigflow import transforms
from bigflow import serde
from bigflow.test import test_base


class FlattenValuesTestCase(test_base.PipelineBasedTest):
    def serde_equal(self, expect, real):
        self.assertEqual(str(serde.of(expect)), str(serde.of(real)))

    def test_flatten(self):
        data = self._pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
        grouped = data.group_by_key()
        self.assertEqual(0, grouped.nested_level())

        flatten = grouped.flatten()

        self.assertItemsEqual([("B", 2), ("A", 4), ("A", 3), ("A", 1)], self._pipeline.get(flatten))

    def test_nested_flatten(self):
        def to_tuple_list(elem):
            tuple2 = (chr(elem + ord('a') - 1), elem)
            return [tuple2, tuple2]

        data = self._pipeline.parallelize([("A", 1), ("B", 2), ("C", 3), ("D", 4)])
        data = data.map(lambda x: x, serde = serde.of((str, int)))

        self.serde_equal(data.serde(), serde.of((str, int)))

        grouped = data.group_by_key().apply_values(transforms.flat_map, to_tuple_list,
            serde = serde.of((str, int)))

        self.serde_equal(str, grouped.key_serdes()[0])
        self.serde_equal((str, int), grouped.serde())

        self.assertEqual(0, grouped.nested_level())

        nested = grouped.apply_values(transforms.group_by_key)

        self.serde_equal(str, nested.key_serdes()[0])
        self.serde_equal(str, nested.key_serdes()[1])
        self.serde_equal(int, nested.serde())

        self.assertEqual(1, nested.nested_level())

        flatten = nested.flatten()
        self.serde_equal((str, (str, int)), flatten.serde())

        expected = [("D", ("d", 4)),
                    ("D", ("d", 4)),
                    ("C", ("c", 3)),
                    ("C", ("c", 3)),
                    ("B", ("b", 2)),
                    ("B", ("b", 2)),
                    ("A", ("a", 1)),
                    ("A", ("a", 1))]

        self.assertItemsEqual(expected, self._pipeline.get(flatten))

if __name__ == "__main__":
    unittest.main()
