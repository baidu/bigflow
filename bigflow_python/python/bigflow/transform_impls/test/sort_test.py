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
File: sort_test.py
Author: panyunhong(bigflow-opensource@baidu.com)
Date: 2015/03/14 13:40:55
"""
import unittest

from bigflow.test import test_base
from bigflow.transform_impls import sort
from bigflow import serde

def key_read_fn(key):
    return key[0]

class SortTestCase(test_base.PipelineBasedTest):

    def test_multiple_field_default_sort(self):
        """
        test_multiple_field_default_sort
        """
        raw = [(12, 23), (15, 6), (10, 21), (10, 11), (15, 2), (15, 9), (34, 1)]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort()
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if r1[0] == r2[0]:
                return r1[1] - r2[1]
            else:
                return r1[0] - r2[0]
        nresult = sorted(raw, key_comparer)
        self.assertEqual(self._pipeline.get(presult), nresult)

    def test_multiple_various_type_field_default_sort(self):
        """
        test_multiple_various_type_field_default_sort
        """
        raw = [(12, "123"), (15, "321"), (10, "210"), (10, "11"), (15, "2"), \
            (15, "9"), (34, "0123"), (34, "1")]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort()
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if r1[0] == r2[0]:
                if r1[1] < r2[1]:
                    return -1
                elif r1[1] == r2[1]:
                    return 0
                else:
                    return 1
            else:
                return r1[0] - r2[0]
        nresult = sorted(raw, key_comparer)
        self.assertEqual(self._pipeline.get(presult), nresult)


    def test_sortby_tuple_list_double_int(self):
        """
        test_sortby_tuple_list_double_int
        """
        raw = [(12, 23), (15, 6), (10, 21), (10, 11), (15, 2), (15, 9), (34, 1)]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda record: [sort.ASC(record[0]), sort.DESC(record[1])])
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if (r1[0] != r2[0]):
                return r1[0] - r2[0]
            return r2[1] - r1[1]
        nresult = sorted(raw, cmp=key_comparer)
        self.assertEqual(self._pipeline.get(presult), nresult)

    def test_sortby_single_key_int(self):
        """
        test_sortby_single_key_int
        """
        raw = [(12, -23), (15, -6), (10, 21), (8, 0), (44555, -2000000001), (23, 50), (15, 13)]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda record: sort.ASC(record[1]))
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            return r1[1] - r2[1]
        nresult = sorted(raw, cmp=key_comparer)
        self.assertEqual(nresult, self._pipeline.get(presult))

    def test_sortby_single_key_long(self):
        """
        test_sortby_single_key_long
        """
        raw = [(12L, -23L), (15L, -6L), (10L, 21L), (8L, 0L), (44555, -2147483644L),\
         (34L, -222222223333333333L), (23, 50L), (15, 13L), (1L, 2222222222222222L), \
         (1L, 2222222222222221L), (1L, 1073741823L), (2L, 1073741824L), \
         (2L, 1073741825L), (2L, 1073741826L), (1L, -1073741823L), (2L, -1073741824L)]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda record: sort.ASC(record[1]))
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if r1[1] < r2[1]:
                return -1
            elif r1[1] == r2[1]:
                return 0
            else:
                return 1
        nresult = sorted(raw, cmp=key_comparer)
        self.assertEqual(nresult, self._pipeline.get(presult))

    def test_sortby_multiple_key_long(self):
        """
        test_sortby_multiple_key_long
        """
        raw = [(15L, 2L), (2L, 4L), (2L, 40L), (10L, 20L), (15L, 10L), \
        (-1073741824L, 14L), (-1073741824L, 12L), (1073741824L, 3L), (1073741824L, 1L)]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda record: [sort.ASC(record[0]), sort.DESC(record[1])])
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if r1[0] == r2[0]:
                if r1[1] < r2[1]:
                    return 1
                elif r1[1] > r2[1]:
                    return -1
                else:
                    return 0
            elif r1[0] < r2[0]:
                return -1
            else:
                return 1
        nresult = sorted(raw, cmp=key_comparer)
        self.assertEqual(nresult, self._pipeline.get(presult))

    def test_sort_single_float(self):
        """
        test_sort_single_float
        """
        import math
        raw = [1.2, 2.3, 1.80001, 0.001, 0.0, 0.0003, \
        -73786976294838206464.2, 1.8, 1.8000001, 2.334]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda record: sort.ASC(record))
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if r1 < r2:
                return -1
            elif math.fabs(r1 - r2) < 1e-7:
                return 0
            return 1
        nresult = sorted(raw, cmp=key_comparer)
        self.assertEqual(nresult, self._pipeline.get(presult))

    def test_sort_multiple_float(self):
        """
        test_sort_multiple_float
        """
        import math
        raw = [(1.0001, 2.03), (0.003, 1.0), (4.5, 0.223), (-0.223, 1.222), \
        (1.22, -3.12)]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda record: [sort.ASC(record[0]), sort.DESC(record[1])])
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if r1[0] < r2[0]:
                return -1
            elif math.fabs(r1[0]-r2[0]) < 1e-7:
                if r1[1] < r2[1]:
                    return 1
                elif math.fabs(r1[1]-r2[1]) < 1e-7:
                    return 0
                return -1
            return 1
        nresult = sorted(raw, cmp=key_comparer)
        self.assertEqual(nresult, self._pipeline.get(presult))

    def test_sort_multiple_float_long(self):
        """
        test_sort_multiple_float_long
        """
        import math
        raw = [(1.0001, 2030L), (0.003, 10000L), (4.5, 2230L), (-0.223, 1222L), \
        (1.22, -3120L, "aa"), (-0.002, -9000000000000000L)]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda record: [sort.ASC(record[0]), sort.DESC(record[1])])
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if r1[0] < r2[0]:
                return -1
            elif math.fabs(r1[0]-r2[0]) < 1e-7:
                if r1[1] < r2[1]:
                    return 1
                elif r1[1] > r2[1]:
                    return -1
                return 0
            return 1
        nresult = sorted(raw, cmp=key_comparer)
        self.assertEqual(nresult, self._pipeline.get(presult))

    def test_sort_multiple_string(self):
        """
        test_sort_multiple_string
        """
        raw = [("a", "56"), ("e", "123"), ("c", "b231"), ("zzzz", "34555")]
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda record: [sort.DESC(record[0]), sort.ASC(record[1])])
        def key_comparer(r1, r2):
            """
            key_comparer
            """
            if r1[0] < r2[0]:
                return 1
            elif r1[0] == r2[0]:
                if r1[1] < r2[1]:
                    return 1
                elif r1[1] > r2[1]:
                    return -1
                return 0
            return -1
        nresult = sorted(raw, cmp=key_comparer)
        self.assertEqual(nresult, self._pipeline.get(presult))

    def test_unicode_string_sort(self):
        """
        test_unicode_string_sort
        """
        raw = [u'abc', u'你好', u'gcc', u'', u'abcdefg', u'hello世界']
        nresult = sorted(raw)
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda rec: sort.ASC(rec))
        self.assertEqual(nresult, presult.get())
        nresult_desc = sorted(raw, reverse=True)
        presult_desc = pdata.sort_by(lambda rec: sort.DESC(rec))
        self.assertEqual(nresult_desc, presult_desc.get())

    def test_unicode_string_sort_total_chinese(self):
        """
        test_unicode_string_sort_desc
        """
        raw = [u'尽头', u'你好', u'深渊', u'', u'宝马bmw', u'hello世界']
        nresult = sorted(raw)
        pdata = self._pipeline.parallelize(raw)
        presult = pdata.sort_by(lambda rec: sort.ASC(rec))
        self.assertEqual(nresult, presult.get())
        nresult_desc = sorted(raw, reverse=True)
        presult_desc = pdata.sort_by(lambda rec: sort.DESC(rec))
        self.assertEqual(nresult_desc, presult_desc.get())

    def test_sort(self):
        """
        test_sort
        """
        raw = ["3", "7", "1", "3", "2", "8"]
        data = self._pipeline.parallelize(raw)
        sorted_raw = raw[:]
        sorted_raw.sort()

        result = data.sort()
        result_reverse = data.sort(reverse=True)

        result.cache()
        result_reverse.cache()

        self.assertEqual(sorted_raw, self._pipeline.get(result))
        self.assertEqual(sorted_raw[::-1], self._pipeline.get(result_reverse))

    def test_table_sort(self):
        """
        test_table_sort
        """
        raw = [("b", 2),
               ("a", 1),
               ("c", 3)]
        data = self._pipeline.parallelize(raw)
        sorted_raw = raw[:]
        sorted_raw.sort()

        result = data.sort_by(key_read_fn)
        result_reverse = data.sort_by(key_read_fn, reverse=True)

        result.cache()
        result_reverse.cache()

        self.assertEqual(sorted_raw, self._pipeline.get(result))
        self.assertEqual(sorted_raw[::-1], self._pipeline.get(result_reverse))

    def test_sort_str(self):
        """ test """
        data = self._pipeline.parallelize([4, 5, 1, 2, 3, 0])
        lines = data.map(lambda x: str(x), serde=serde.of(str))
        self.assertEqual('012345', lines.sort().accumulate('', lambda x, y: x + y).get())


if __name__ == "__main__":
    unittest.main()
