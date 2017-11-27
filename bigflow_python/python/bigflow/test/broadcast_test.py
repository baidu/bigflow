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
Broadcast related unit test

Author: Wen, Chunyang(bigflow-opensource@baidu.com)
"""

import unittest
import os
from bigflow.test import test_base
from bigflow import input, output

class TestCase(test_base.PipelineBasedTest):
    """
    Broadcast related unit test
    """

    def test_broadcast(self):
        """
        Unit tests entrance
        """
        output_path = self.generate_tmp_path()
        pc  = self._pipeline.parallelize([1,2,3])
        pc1 = self._pipeline.parallelize([1,2,3])
        pc2 = self._pipeline.parallelize([4,2,6])
        pc3 = pc.map(lambda x,y,z:(x, (x in y) and (x in z)), pc1, pc2)
        pc4 = pc3.map(lambda x:"\t".join(map(str, x)))
        self._pipeline.write(pc4, output.TextFile(output_path).partition(n=2))
        self._pipeline.run()

        parts = ['part-00000', 'part-00001']
        input_path = map(lambda path:os.path.join(output_path, path), parts)
        result = self._pipeline.read(input.TextFile(*input_path))
        target = ['1\tFalse', '2\tTrue', '3\tFalse']
        self.assertItemsEqual(result.get(), target)

if __name__ == "__main__":
    unittest.main()
