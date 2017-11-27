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
File: combine_test.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/03/10 15:41:43
"""

import unittest

from bigflow.test import test_base


def combine_fun(numbs):
    accumulate = 0
    for i in numbs:
        accumulate += i
    return accumulate


class CombineTestCase(test_base.PipelineBasedTest):

    def test_combine_add_int(self):
        number_list = self._pipeline.parallelize([1, 2, 3, 4])

        result = number_list.combine(combine_fun)

        self.assertEqual(10, self._pipeline.get(result))

    def test_combine_without_map_combine(self):
        number_list = self._pipeline.parallelize(['2', '3', '4'])
        to_lookup = {'2': 2, '3': 3, '4': 4}

        def lookup_dict_and_sum(numbers):
            return sum(map(lambda s: to_lookup[s], numbers))

        result = number_list.combine(lookup_dict_and_sum, pre_combine=False)

        self.passertEqual(9, result)


if __name__ == "__main__":
    unittest.main()
