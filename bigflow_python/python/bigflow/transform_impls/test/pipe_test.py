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
File: pipe_test.py
Author: Xu Yao(xuyao02@baidu.com)
Date: 2015/12/20 08:23:22
"""
import unittest

from bigflow import transforms
from bigflow.test import test_base


class TestCase(test_base.PipelineBasedTest):
    """
    Pipe transform test
    """

    def test_streaming_pcollection(self):
        """
        Case: test streaming pcollection
        """
        p = self._pipeline.parallelize(['2', '18', '12', '4'])
        piped = p.apply(transforms.pipe, "cat")

        self.assertItemsEqual(['2', '18', '12', '4'], piped.get())

        p = self._pipeline.parallelize(['\t\tabc', '\ta\tb\tc', 'a\tb\tc'])
        piped = p.apply(transforms.pipe, "cat", output_fields_num=2)

        self.assertItemsEqual([
            ('', '\tabc'),
            ('', 'a\tb\tc'),
            ('a', 'b\tc')], piped.get())

        p = self._pipeline.parallelize(['a\t\t', 'b\t\t', '\t\t', '\t\t'])
        piped = p.apply(transforms.pipe, "cat", output_fields_num=3)

        self.assertItemsEqual([
            ('a', '', ''),
            ('b', '', ''),
            ('', '', ''),
            ('', '', '')], piped.get())

    def test_bistreaming_pcollection(self):
        """
        Case: test bistreaming pcollection
        """
        p = self._pipeline.parallelize(['2', '18', '12', '4'])
        piped = p.apply(transforms.pipe, "cat", type='bistreaming')

        self.assertItemsEqual(['2', '18', '12', '4'], piped.get())

    def test_bistreaming_pcollection_with_multi_fields(self):
        """
        Case: test bistreaming pcollection with multi-fields
        """
        self._pipeline.add_file('ut_bistreaming_util.py', 'ut_bistreaming_util.py')
        p = self._pipeline.parallelize([2, 18, 12, 4])
        piped = p.map(lambda x: ('', 'x', x)) \
                 .apply(transforms.pipe, "PATH=__bigflow_on_spark__/python_runtime/bin:$PATH python ut_bistreaming_util.py",
                    type='bistreaming',
                    input_fields_num=3,
                    output_fields_num=3)

        self.assertItemsEqual([
            ('', 'x', '2'),
            ('', 'x', '18'),
            ('', 'x', '12'),
            ('', 'x', '4')], piped.get())

    def test_streaming_ptable(self):
        """
        Case: test streaming ptable
        """
        p = self._pipeline.parallelize({
            "A": {
                "AA": [1],
                "AB": [2, 3],
                },
            "B": {
                "BA": [3],
                "BB": [4, 5],
                }
            })
        piped = p.apply(transforms.pipe, "cat", output_fields_num=3)
        self.assertItemsEqual([
            ('A', 'AA', '1'),
            ('A', 'AB', '2'),
            ('A', 'AB', '3'),
            ('B', 'BA', '3'),
            ('B', 'BB', '4'),
            ('B', 'BB', '5')], piped.get())

    def test_bistreaming_ptable(self):
        """
        Case: test bistreaming ptable
        """
        p = self._pipeline.parallelize({
            "A": {
                "AA": [1],
                "AB": [2, 3],
                },
            "B": {
                "BA": [3],
                "BB": [4, 5],
                }
            })
        piped = p.apply(transforms.pipe, "cat", output_fields_num=3, type='bistreaming')
        self.assertItemsEqual([
            ('A', 'AA', '1'),
            ('A', 'AB', '2'),
            ('A', 'AB', '3'),
            ('B', 'BA', '3'),
            ('B', 'BB', '4'),
            ('B', 'BB', '5')], piped.get())

    def test_streaming_ptable_with_value_pobject(self):
        """
        Case: test streaming ptable with value pobject
        """
        p = self._pipeline.parallelize({
            "AA": [1],
            "AB": [2, 3],
            "BA": [3],
            "BB": [4, 5],
            })
        p = p.apply_values(lambda x: x.reduce(lambda x, y: x + y))
        piped = p.apply(transforms.pipe, "cat", output_fields_num=2)
        self.assertItemsEqual([
            ('AA', '1'),
            ('AB', '5'),
            ('BA', '3'),
            ('BB', '9')], piped.get())

if __name__ == "__main__":
    unittest.main()
