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
entity unit test

"""

import unittest
import re
from bigflow.test import test_base
from bigflow.core import entity
from bigflow import input
from bigflow import output
from bigflow import base

class TestCase(test_base.PipelineBasedTest):
    """
    entity unit test
    """

    def test_entity_from_file(self):
        """test eneity from file"""
        try:
            size_old = entity.ENTITY_PROTO_SIZE_LIMIT
            entity.ENTITY_PROTO_SIZE_LIMIT = -1
            l1 = [1, 2, 4, 6, 7]
            p1 = self._pipeline.parallelize([1, 2, 3])
            p2 = p1.filter(lambda x: x in l1)
            p3 = p2.sum()
            self.assertEqual(3, p3.get())
        finally:
            entity.ENTITY_PROTO_SIZE_LIMIT = size_old


if __name__ == "__main__":
    unittest.main()
