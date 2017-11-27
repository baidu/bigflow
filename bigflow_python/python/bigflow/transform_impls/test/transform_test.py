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
File: transform_test.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/03/10 15:41:43
"""

import unittest

from bigflow.test import test_base
from bigflow import transforms

def ps_initialize(emitter):
    status = 0
    return status


def ps_transform(status, emitter, record):
    status += int(record)
    emitter.emit(status)
    return status


def ps_finalize(status, emitter):
    pass

def es_initialize(emitter, side_input):
    return side_input


def es_transform(status, emitter, record, side_input):
    status += int(record)
    emitter.emit(status)
    return status


def es_finalize(status, emitter, side_input):
    pass


def ets_initialize(emitter, side_input):
    return side_input[5]


def ets_transform(status, emitter, record, side_input):
    status += int(record)
    emitter.emit(status)
    return status


def ets_finalize(status, emitter, side_input):
    pass

from bigflow.transform_impls import sort

class TransformTestCase(test_base.PipelineBasedTest):

    def test_transform(self):
        data = self._pipeline.parallelize([4, 3, 2, 1]).sort(sort.DESC)
        result = data.transform(ps_initialize, ps_transform, ps_finalize)

        self.assertEqual([4, 7, 9, 10], self._pipeline.get(result))

    def test_transform_with_side_input(self):
        side_inputs = self._pipeline.parallelize(5)
        data = self._pipeline.parallelize([4, 3, 2, 1]).sort(sort.DESC)
        result = data.transform(es_initialize, es_transform, es_finalize, side_inputs)

        self.assertEqual([9, 12, 14, 15], self._pipeline.get(result))

    def test_transform_with_side_input_ptable(self):
        side_inputs = self._pipeline.parallelize({5: 5})

        data = self._pipeline.parallelize([4, 3, 2, 1]).sort(sort.DESC)
        result = data.transform(ets_initialize, ets_transform, ets_finalize, side_inputs)

        self.passertEqual([9, 12, 14, 15], result)

    def test_with_transformer(self):
        self._pipeline.add_file("transformers.py", "transformers.py")
        import transformers
        data = self._pipeline.parallelize([4, 3, 2, 1]).sort(sort.DESC)

        result1 = data.transform(transformers.PartialSumTransformer())
        self.passertEqual([4, 7, 9, 10], result1)

        result2 = data.transform(transformers.FakeOutputTransformer([1, 2, 3])).sort()
        self.passertEqual([1, 2, 3], result2)

        result3 = data.transform(transformers.ZipTransformer(), result1, result2)
        self.passertEqual([(4, 4, 1), (3, 7, 2), (2, 9, 3)], result3)

        result4 = data.transform(transformers.SumTransformer())
        self.passertEqual([10], result4)

if __name__ == "__main__":
    unittest.main()
