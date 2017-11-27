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
File: first_test.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/03/14 13:40:55
"""

import unittest

from bigflow.test import test_base


class FirstTestCase(test_base.PipelineBasedTest):

    def test_first_pobject(self):
        data = self._pipeline.parallelize([2, 3, 1, 4])

        result = data.first()

        self.assertEqual(2, self._pipeline.get(result))

if __name__ == "__main__":
    unittest.main()
