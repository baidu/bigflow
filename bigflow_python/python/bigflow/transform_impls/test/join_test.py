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
Author: panyunhong(panyunhong@baidu.com), Wang Cong(wangcong09@baidu.com), Miaodongdong(miaodongdong@baidu.com)
"""

import unittest

from bigflow.test import test_base


class JoinTestCase(test_base.PipelineBasedTest):
    def setUp(self):
        super(JoinTestCase, self).setUp()
        pass

    def generate_data(self):

        t1 = self._pipeline.parallelize([("key1", 1),
                                              ("key2", 2),
                                              ("key2", 3),
                                              ("key3", 4)])

        t2 = self._pipeline.parallelize([("key0", 5.0),
                                              ("key1", 6.0),
                                              ("key2", 7.0),
                                              ("key2", 8.0)])

        t3 = self._pipeline.parallelize([("key0", "A"),
                                              ("key2", "C"),
                                              ("key3", "D"),
                                              ("key4", "E")])
        return t1, t2, t3

    def test_join(self):
        t1, t2, t3 = self.generate_data()
        result = t1.join(t2)
        expected = [("key2", (2, 7.0)),
                    ("key2", (2, 8.0)),
                    ("key2", (3, 7.0)),
                    ("key2", (3, 8.0)),
                    ("key1", (1, 6.0))]

        result_multi = t1.join(t2, t3)
        expected_multi = [('key2', (2, 7.0, 'C')),
                          ('key2', (2, 8.0, 'C')),
                          ('key2', (3, 7.0, 'C')),
                          ('key2', (3, 8.0, 'C'))]

        result.cache()
        result_multi.cache()

        self.assertItemsEqual(expected, self._pipeline.get(result))
        self.assertItemsEqual(expected_multi, result_multi.get())

    def test_left_join(self):
        t1, t2, t3 = self.generate_data()
        result = t1.left_join(t2)
        expected = [("key3", (4, None)),
                    ("key2", (2, 7.0)),
                    ("key2", (2, 8.0)),
                    ("key2", (3, 7.0)),
                    ("key2", (3, 8.0)),
                    ("key1", (1, 6.0))]

        result_multi = t1.left_join(t2, t3)

        expected_multi = [('key3', (4, None, 'D')),
                          ('key2', (2, 7.0, 'C')),
                          ('key2', (2, 8.0, 'C')),
                          ('key2', (3, 7.0, 'C')),
                          ('key2', (3, 8.0, 'C')),
                          ('key1', (1, 6.0, None))]

        result.cache()
        result_multi.cache()

        self.assertItemsEqual(expected, result.get())
        self.assertItemsEqual(expected_multi, result_multi.get())

    def test_right_join(self):
        t1, t2, t3 = self.generate_data()
        result = t1.right_join(t2)

        expected = [("key2", (2, 7.0)),
                    ("key2", (3, 7.0)),
                    ("key2", (2, 8.0)),
                    ("key2", (3, 8.0)),
                    ("key1", (1, 6.0)),
                    ("key0", (None, 5.0))]

        result_multi = t1.right_join(t2, t3)

        expected_multi = [('key4', (None, None, 'E')),
                          ('key3', (4, None, 'D')),
                          ('key2', (2, 7.0, 'C')),
                          ('key2', (3, 7.0, 'C')),
                          ('key2', (2, 8.0, 'C')),
                          ('key2', (3, 8.0, 'C')),
                          ('key0', (None, 5.0, 'A'))]

        result.cache()
        result_multi.cache()

        self.assertItemsEqual(expected, result.get())
        self.assertItemsEqual(expected_multi, result_multi.get())

    def test_full_join(self):
        t1, t2, t3 = self.generate_data()
        result = t1.full_join(t2)
        expected = [("key3", (4, None)),
                    ("key2", (2, 7.0)),
                    ("key2", (2, 8.0)),
                    ("key2", (3, 7.0)),
                    ("key2", (3, 8.0)),
                    ("key1", (1, 6.0)),
                    ("key0", (None, 5.0))]

        result_multi = t1.full_join(t2, t3)
        expected_multi = [('key1', (1, 6.0, None)),
                          ('key4', (None, None, 'E')),
                          ('key3', (4, None, 'D')),
                          ('key2', (2, 7.0, 'C')),
                          ('key2', (2, 8.0, 'C')),
                          ('key2', (3, 7.0, 'C')),
                          ('key2', (3, 8.0, 'C')),
                          ('key0', (None, 5.0, 'A'))]

        result.cache()
        result_multi.cache()

        self.assertItemsEqual(expected, self._pipeline.get(result))
        self.assertItemsEqual(expected_multi, result_multi.get())


if __name__ == "__main__":
    unittest.main()

