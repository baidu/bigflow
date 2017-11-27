#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
#
"""
Author: zhangyuncong
Date:   2015-09-23 16:18:06
Last Modified by:   zhangyuncong
Last Modified time: 2015-09-23 16:18:06
"""

from bigflow import serde
from bigflow.future import fields
import unittest

class TestFieldsCase(unittest.TestCase):
    """ test """

    def test_fields_serde(self):
        """ test """
        sd = fields.FieldsDictSerde({'id': int, 'name': str, 'age': int})
        self.assertEqual({'id': 1, 'name': '123', 'age': 15},
            sd.deserialize(sd.serialize({'id': 1, 'name': '123', 'age': 15, 'foo': 'nothing'})))

    def test_get_serde_of_fields(self):
        """ test """
        sd = fields.FieldsDictSerde({'id': int, 'name': str, 'age': int})
        self.assertEqual(str(serde.of(str)), str(fields.get_serde_of_field(sd, 'name')))
        self.assertEqual(str(fields.of({'id':int, 'name': str})),
                str(fields.get_serde_of_fields(sd, ['id', 'name'])))


    def test_fields(self):
        """ test """
        from bigflow import base

        p = base.Pipeline.create('local')
        website_click = p.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
            .map(lambda record: dict(zip(['websites', 'clicknum'], record)),
                 serde = fields.of({'websites': str, 'clicknum': int}))\
            .select('websites, clicknum => website, clicknum',
                    lambda websites, clicknum:
                        (websites.flat_map(lambda websites: websites.split(',')), clicknum))
        analytics = website_click.apply(fields.group_by, ['website'])\
            .apply_values(fields.agg,
                "website, clicknum => website, max_click_num, sum_click_num, avg_click_num",
                lambda website, clicknum:
            (website.first(), clicknum.max(), clicknum.sum(), clicknum.sum() / clicknum.count()))\
            .apply(fields.flatten)


        expect =  \
        [{'website': 'a', 'sum_click_num': 7, 'website': 'a', 'avg_click_num': 1, 'max_click_num': 3},
        {'website': 'c', 'sum_click_num': 6, 'website': 'c', 'avg_click_num': 2, 'max_click_num': 3},
        {'website': 'b', 'sum_click_num': 5, 'website': 'b', 'avg_click_num': 1, 'max_click_num': 2},
        {'website': 'd', 'sum_click_num': 1, 'website': 'd', 'avg_click_num': 1, 'max_click_num': 1}]

        self.assertItemsEqual(expect, analytics.get())

if __name__ == "__main__":
    unittest.main()

