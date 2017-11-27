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
File: cogroup_test.py
Author: panyunhong(bigflow-opensource@baidu.com)
Date: 2015/03/10 15:41:43
"""

import unittest

from bigflow.test import test_base
from bigflow import transforms

class CogroupTestCase(test_base.PipelineBasedTest):
    def test_cogroup(self):
        input1 = self._pipeline.parallelize([("A", 1),
                                             ("B", 1),
                                             ("A", 2)])

        input2 = self._pipeline.parallelize([("A", 3),
                                             ("C", 4)])

        t1 = input1.group_by_key()
        t2 = input2.group_by_key()

        result_t1 = input1.cogroup(input2) \
            .apply_values(lambda _t1, _t2: _t1)
        result_t2 = input1.cogroup(input2) \
            .apply_values(lambda _t1, _t2: _t2)

        t1.cache()
        t2.cache()
        result_t1.cache()
        result_t2.cache()

        self.assertItemsEqual(self._pipeline.get(t1), self._pipeline.get(result_t1))
        self.assertItemsEqual(self._pipeline.get(t2), self._pipeline.get(result_t2))

    def test_cogroup_options(self):
        input1 = self._pipeline.parallelize([("A", 1),
                                             ("B", 1),
                                             ("A", 2)])

        input2 = self._pipeline.parallelize([("A", 3),
                                             ("C", 4)])

        t1 = input1.group_by_key()
        t2 = input2.group_by_key()

        result1 = transforms.cogroup(input1, input2, concurrency=1)\
                            .apply_values(lambda _t1, _t2: _t1)
        result2 = transforms.cogroup(input1, input2, concurrency=1) \
            .apply_values(lambda _t1, _t2: _t2)
        t1.cache(), t2.cache(), result1.cache(), result2.cache()
        self.assertEqual(t1.get(), result1.get())
        self.assertEqual(t2.get(), result2.get())

    # def test_cogroup_twoside_not_equal(self):
    #     self.setConfig(must_keep_empty_group=True, tmp_data_path='/app/dc/yarn/dataflow/acmol')
    #     input1 = self._pipeline.parallelize([(9, 1),
    #                                          (8, 0),
    #                                          (4, 10),
    #                                          (4, 20)])

    #     input2 = self._pipeline.parallelize([(9, 10),
    #                                          (9, 15),
    #                                          (7, 3),
    #                                          (17, 3),
    #                                          (4, 45)])
    #     result = input1.cogroup(input2)

    #     r1 = result.apply_values(lambda t1, t2: t1.count())
    #     self.assertEqual({9:1, 8:1, 4:2, 7:0, 17:0}, r1.get())

if __name__ == "__main__":
    unittest.main()
