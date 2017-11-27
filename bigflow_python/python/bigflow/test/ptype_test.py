#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""
PType unit test

Author: Xu, Yao(xuyao02@baidu.com)
"""

import unittest

from bigflow import error
from bigflow.test import test_base
from bigflow import transforms

class TestCase(test_base.PipelineBasedTest):
    """
    PType unit test
    """
    def test_apply(self):
        """
        Case: test apply
        """
        data = self._pipeline.parallelize([1, 2, 3, 4, 5])
        side_input = self._pipeline.parallelize([4, 5, 6])
        def _apply_func(data, *args, **kargs):
            si = args[0]
            if kargs['operation'] == 'sum':
                result = data.map(lambda x, y: x + y, si.sum())
            elif kargs['operation'] == 'product':
                result = data.map(lambda x, y: x * y, si.reduce(lambda x, y: x * y))
            return result
        result = data.apply(_apply_func, side_input, operation='sum')
        self.assertItemsEqual([16, 17, 18, 19, 20], result.get())
        result2 = data.apply(_apply_func, side_input, operation='product')
        self.assertItemsEqual([120, 240, 360, 480, 600], result2.get())

    def test_apply_nonptype(self):
        """
        Case: test apply nontype
        """
        data = self._pipeline.parallelize([1, 2, 3, 4, 5])
        def _apply_func(data):
            return True
        result = data.apply(_apply_func)
        self.assertEqual(True, result)

    def test_side_input(self):
        """
        Case: test side input
        """
        data = self._pipeline.parallelize([1, 2, 3, 4, 5])
        data1 = data.map(lambda x: x)
        def _map_func(record, *args):
            si = list(args[0])
            record = record + 1 if record in si else record
            return record
        result = data.map(_map_func, data1)
        self.assertItemsEqual([2, 3, 4, 5, 6], result.get())

    def test_unicode_string(self):
        """
        Case: test unicode string process
        """
        unicode_str = self._pipeline.parallelize(1).map(lambda x: u'a').get()
        self.assertEqual('a', unicode_str)

    def test_lambda_ser(self):
        """
        Case : Ptype will not be pickled in lambda.
        """
        data = self._pipeline.parallelize([1, 2, 3, 4, 5])
        s = data.first()
        success = False
        try:
            p = data.map(lambda x: x + s)
            print p.get()
        except error.InvalidLogicalPlanException:
            success = True
        self.assertTrue(success)

if __name__ == "__main__":
    unittest.main()
