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
File: to_pobject_test.py
Author: Miao, Dongdong(bigflow-opensource@baidu.com)
"""
import unittest

from bigflow.test import test_base
from bigflow import transforms
from bigflow import pobject

class TestCase(test_base.PipelineBasedTest):

    def test_to_pobject(self):

        data = self._pipeline.parallelize({2: 4, 3: 6, 4: 8})

        result = transforms.to_pobject(data)
        result_data = result.get()

        self.assertEqual({2: 4, 3: 6, 4: 8}, result_data)
        self.assertEqual(isinstance(result,pobject.PObject), True)

if __name__ == "__main__":
    unittest.main()
