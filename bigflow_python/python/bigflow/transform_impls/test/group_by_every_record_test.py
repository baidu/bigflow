# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
"""
@Author: zhangyuncong
@Date:   2016-03-07 19:58:12
@Last Modified by:   zhangyuncong
"""
import unittest

from bigflow.test import test_base
from bigflow import transforms
from bigflow.transform_impls import group_by_every_record


class GroupByEveryRecordTestCase(test_base.PipelineBasedTest):
    """ test """

    def test_normal(self):
        """
        test
        """
        data = self._pipeline.parallelize([1, 2, 2, 3])
        result = data.apply(group_by_every_record.group_by_every_record) \
                      .apply_values(lambda record, all: record.union(all), data) \
                      .apply_values(transforms.sum) \
                      .flatten_values()

        self.assertItemsEqual([9, 10, 10, 11], self._pipeline.get(result))

    def test_with_side_input(self):
        """ test """
        data = self._pipeline.parallelize([1, 2, 2, 3])
        result1 = data.apply(group_by_every_record.group_by_every_record) \
                      .apply_values(lambda record, all: all.cartesian(record), data) \
                      .flatten_values()

        result2 = data.apply(group_by_every_record.group_by_every_record) \
                      .apply_values(lambda record, all: record.cartesian(all), data) \
                      .flatten_values()

        result2.cache()

        expect = [(1, 1), (2, 1), (2, 1), (3, 1),
                  (1, 2), (2, 2), (2, 2), (3, 2),
                  (1, 2), (2, 2), (2, 2), (3, 2),
                  (1, 3), (2, 3), (2, 3), (3, 3)]

        self.assertItemsEqual(expect, result1.get())
        self.assertItemsEqual(expect, result2.get())


if __name__ == "__main__":
    unittest.main()
