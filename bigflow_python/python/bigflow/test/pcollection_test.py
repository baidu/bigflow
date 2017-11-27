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
PCollection unit test

Author: Wen, Chunyang(wenchunyang@baidu.com)
"""

import unittest
import re
import sys
from bigflow import error
from bigflow.test import test_base
from bigflow.util.log import logger

class TestCase(test_base.PipelineBasedTest):
    """
    PCollection unit test
    """

    def setUp(self):
        """set up function"""
        super(TestCase, self).setUp()
        self.p1_list = [1, 3, 5, 7]
        self.p2_list = [2, 2, 4, 6, 8]
        self.p1 = self._pipeline.parallelize(self.p1_list)
        self.p2 = self._pipeline.parallelize(self.p2_list)
        self.p3 = self._pipeline.parallelize([('huo', 1), ('shi', 2)])
        self.p4 = self._pipeline.parallelize([('wei', 4), ('yuan', 2), ('huo', 5)])
        self.p5 = self._pipeline.parallelize(2)
        self.p6 = self._pipeline.parallelize([])
        self.p7 = self._pipeline.parallelize([1, 3])
        self.p8 = self._pipeline.parallelize([1, 4])
        self.tests = {}

    def _test_this(self, expr, need_sorted=True):
        """
        tests _test_this: construct test dict
        Args:
            need_sorted: for list assert, default: we sort before assert
        """
        import re
        from operator import add
        from operator import mul
        from bigflow.transforms import union

        exprs = re.split(" *== *", expr, 1)
        lhs = re.sub(r"(p\d+)", r"self.\1", exprs[0])
        rhs = re.sub(r"(p\d+)", r"self.\1", exprs[1])
        exec("lhs=" + lhs)
        exec("rhs=" + rhs)
        self.tests[expr] = (lhs, rhs, expr, need_sorted)
        rhs.cache()

    def run_tests(self):
        """execute tests"""
        for _, value in self.tests.iteritems():
            lhs, rhs, expr, need_sorted = value
            logger.info("I am testing: %s" % str(expr))
            if isinstance(lhs, list):
                if need_sorted:
                    self.assertItemsEqual(lhs, rhs.get(), expr)
                else:
                    self.assertListEqual(lhs, rhs.get(), expr)
            elif isinstance(lhs, dict):
                self.assertDictEqual(lhs, rhs.get(), expr)
            else:
                self.assertEqual(lhs, rhs.get(), expr)
        # clear all the tests
        self.tests.clear()

    def _test_accumulate(self):
        """test accumulate"""
        self._test_this("16 == p1.accumulate(0, add)")

    def _test_aggregate(self):
        """test aggregate"""
        self._test_this("16 == p1.aggregate(0, add, add)")

    def _test_as_object(self):
        """test as_object """
        self._test_this("1 == p1.as_pobject()")

    def _test_cogroup(self):
        """fail to run in local"""
        self._test_this("{'huo':[1], 'shi':[2]} == p3.cogroup(p6).apply_values(union)")

    def _test_combine(self):
        """test combine"""
        self._test_this("22 == p2.combine(sum)")

    def _test_count(self):
        """test count"""
        self._test_this("4 == p1.count()")

    def _test_distinct(self):
        """test distinct"""
        self._test_this("[2, 4, 6, 8] == p2.distinct()")

    def _test_filter(self):
        """test filter"""
        self._test_this("[4, 6, 8] == p2.filter(lambda x: x > 2)")

    def _test_map(self):
        """test map"""
        self._test_this("[2, 4, 6, 8] == p1.map(lambda x: x+1)")

    def _test_foreach(self):
        """pass
        has no good way to test it
        """
        pass

    def _test_flat_map(self):
        """test flat_map"""
        self._test_this("[1, 3, 5, 7] * 2 == p1.flat_map(lambda x: [x, x])")

    def _test_group_by(self):
        """test group_by"""
        self._test_this("{1:[('huo', 1)], 2:[('shi',2)]} == p3.group_by(lambda x:x[1])")

    def _test_group_by_key(self):
        """test group_by_key"""
        self._test_this("{'huo':[1], 'shi':[2]} == p3.group_by_key()")

    def _test_max(self):
        """test max"""
        self._test_this("8 == p2.max()")

    def _test_max_elements(self):
        """test max_elements"""
        self._test_this("[5, 7] == p1.max_elements(2)")
        self._test_this("[8] == p2.max_elements(1)")
        self._test_this("[6, 8] == p2.max_elements(p5)")
        self._test_this("[] == p2.max_elements(0)")
        self._test_this("[] == p2.max_elements(-p5)")

    def _test_transform(self):
        """test transform"""
        pass

    def _test_reduce(self):
        """test reduce"""
        self._test_this("16 == p1.reduce(add)")
        self._test_this("768 == p2.reduce(mul)")

    def test_take(self):
        """test take"""
        # last case, takes more elements than existed
        sizes = [1, 2, len(self.p1_list) + 1]
        takes = map(lambda n: self.p1.take(n), sizes)
        for take in takes:
            take.cache()
        p1_set = set(self.p1_list)
        self.assertTrue(all(set(takes[n].get()).issubset(p1_set) \
                for n in range(len(sizes))))
        self.assertTrue(all(len(takes[n].get()) == sizes[n] \
                for n in range(len(sizes) - 1)))
        self.assertTrue(len(takes[-1].get()) == len(self.p1_list))

    def test_lambda_ser(self):
        """
        Case : Ptype will not be pickled in lambda.
        """
        s = self.p2
        success = False
        try:
            p = self.p1.map(lambda x: x + s)
            print p.get()
        except error.InvalidLogicalPlanException:
            success = True
        self.assertTrue(success)

    def _test_first(self):
        """test this"""
        self._test_this("1 == p1.first()")
        self._test_this("2 == p2.first()")

    def _test_sort(self):
        """test sort"""
        import copy
        p1 = copy.deepcopy(self.p1_list)
        p1.sort(reverse=True)
        self._test_this(str(p1) + " == p1.sort(reverse=True)", need_sorted=False)

    def _test_sort_by(self):
        """test sort by"""
        self._test_this("[('shi', 2), ('huo', 1)] == \
                p3.sort_by(lambda x:x[1], reverse=True)", need_sorted=False)

    def _test_sum(self):
        """test sum"""
        self._test_this("16 == p1.sum()")
        self._test_this("4 == p7.sum()")

    def _test_join(self):
        """test join"""
        self._test_this("[('huo', (1,5))] == p3.join(p4)")
        self._test_this("[('huo', (5,1))] == p4.join(p3)")

    def _test_left_join(self):
        """test left_join"""
        self._test_this("[('huo', (1,5)),('shi', (2, None))] == p3.left_join(p4)")

    def _test_right_join(self):
        """test right_join"""
        result = "[('huo', (1,5)), ('wei', (None, 4)), ('yuan', (None, 2))]"
        self._test_this(result + " == p3.right_join(p4)")

    def _test_full_join(self):
        """test full join"""
        result = "[('huo', (1,5)),('shi', (2, None)),\
                ('wei', (None, 4)), ('yuan', (None, 2))]"
        self._test_this(result + " == p3.full_join(p4)")

    def _test_union(self):
        """test union"""
        self._test_this("[1,1,3,4] == p7.union(p8)")
        self._test_this("[1,3] == p7.union(p6)")
        self._test_this("[1,3,4,5,7] == p1.union(p1.count())")

    def _test_cartesian(self):
        """test cartesian"""
        self._test_this("[(1,1),(1,4),(3,1),(3,4)] == p7.cartesian(p8)")
        # curious why it is empty?
        self._test_this("[] == p7.cartesian(p6)")

    def _test_substract(self):
        """test substract"""
        self._test_this("[5, 7] == p1.substract(p7)")
        self._test_this("[1,3,5,7] == p1.substract(p6)")

    def _test_is_empty(self):
        """test is empty"""
        self._test_this("False == p1.is_empty()")
        self._test_this("True == p6.is_empty()")

    def _test_intersection(self):
        """test intersection"""
        self._test_this("[1,3] == p1.intersection(p7)")

    def _test_diff(self):
        """test diff"""
        self._test_this("[(3, (1, 0)), (4, (0, 1))] == p7.diff(p8)")

    def _test_min(self):
        """test min"""
        self._test_this("2 == p2.min()")

    def _test_min_elements(self):
        """test min elements"""
        self._test_this("[1] == p1.min_elements(1)")
        self._test_this("[2, 2] == p2.min_elements(2)")
        self._test_this("[2, 2] == p2.min_elements(p5)")
        self._test_this("[] == p2.min_elements(0)")
        self._test_this("[] == p2.min_elements(-p5)")

    def test_as_schema(self):
        from bigflow import serde
        from bigflow import schema

        raw_dict = {"name": "xiaoming", "school": "PKU", "age": 20}

        data = self._pipeline.parallelize([("xiaoming", "PKU", 20)])
        d1 = data.as_schema((str, str, int))
        d2 = data.as_schema([str, str, int])

        d1_serde = d1.serde().origin_serde()
        d2_serde = d2.serde().origin_serde()
        self.assertEqual(type(d1_serde), serde.TupleSerde)
        self.assertEqual(type(d2_serde), serde.TupleSerde)

        self.assertEqual(d1.get(), [("xiaoming", "PKU", 20)])
        self.assertEqual(d2.get(), [("xiaoming", "PKU", 20)])

        data = self._pipeline.parallelize([raw_dict])
        d3 = data.as_schema({"name": str, "school": str, "age": int})

        output = d3.get()
        expect_keys = sorted(["name", "school", "age"])
        expect_values = [raw_dict.get(k) for k in expect_keys]
        for d in output:
            keys = sorted(d.keys())
            values = [d.get(k) for k in keys]
            self.assertEqual(keys, expect_keys)
            self.assertEqual(values, expect_values)

        d3_serde = d3.serde()
        self.assertEqual(type(d3_serde), schema.FieldsDictSerde)

        data = self._pipeline.parallelize([raw_dict])
        d6 = data.as_schema(["name", "school", "age"]).get()
        expect_data = [{"name": "xiaoming", "school": "PKU", "age": 20}]
        self.assertEqual(expect_data, d6)

    def test_all(self):
        """test all"""
        self._test_accumulate()
        self._test_aggregate()
        self._test_as_object()
        self._test_cogroup()
        self.run_tests()

        self._test_combine()
        self._test_count()
        self._test_distinct()
        self._test_filter()
        self._test_map()
        self.run_tests()

        self._test_foreach()
        self._test_flat_map()
        self._test_group_by()
        self._test_group_by_key()
        self._test_max()
        self.run_tests()

        self._test_max_elements()
        self._test_transform()
        self._test_reduce()
        #self._test_take()
        self._test_first()
        self.run_tests()

        self._test_sort()
        self._test_sort_by()
        self._test_sum()
        self._test_join()
        self._test_left_join()
        self._test_right_join()
        self._test_full_join()
        self._test_union()
        self._test_cartesian()
        self.run_tests()

        self._test_substract()
        self._test_is_empty()
        self._test_intersection()
        self._test_diff()
        self._test_min()
        self.run_tests()

        self._test_min_elements()
        self._test_count()
        self._test_distinct()
        self.run_tests()


if __name__ == "__main__":
    unittest.main()
