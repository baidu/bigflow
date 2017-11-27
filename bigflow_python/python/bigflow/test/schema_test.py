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
Last Modified by:   songzhan
Last Modified time: 2016-03-09 15:28:00
"""

from bigflow.test import test_base
from bigflow import serde
from bigflow import transforms
from bigflow import schema
from bigflow import udf
import unittest

class TestSchemaCase(test_base.PipelineBasedTest):
    """ test """

    def test_schema_serde(self):
        """ test """
        sd = schema.FieldsDictSerde({'id': int, 'name': str, 'age': int})
        self.assertEqual({'id': 1, 'name': '123', 'age': 15},
            sd.deserialize(sd.serialize({'id': 1, 'name': '123', 'age': 15, 'foo': 'nothing'})))

    def test_get_serde_of_fields(self):
        """ test """
        sd = schema.FieldsDictSerde({'id': int, 'name': str, 'age': int})
        self.assertEqual(str(serde.of(str)), str(schema._get_serde_of_field(sd, 'name')))
        self.assertEqual(str(schema.of({'id':int, 'name': str})),
                str(schema._get_serde_of_fields(sd, ['id', 'name'])))

    def test_get_tuple_serde_of_fields(self):
        """ test """
        sd = schema.FieldsDictSerde({'id': int, 'name': str, 'age': int})
        self.assertEqual(str(serde.of((int, str))),
                str(schema._get_tuple_serde_of_fields(sd, ['id', 'name'])))

    def test_transform_schema_pcollection(self):
        """ test """
        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2),
            ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
			.apply(transforms.map, lambda x:x, serde=serde.TupleSerde(str, int))
        sp = schema._transform_schema_pcollection(pc)
        from bigflow import schema_pcollection
        self.assertTrue(isinstance(sp, schema_pcollection.SchemaPCollection))

    def test_tuple_to_dict(self):
        """ test """
        sp1 = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        sp2 = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, [('websites', str), ('clicknum', int)])
        sp3 = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, 'websites,clicknum')

        expect = \
        [{'clicknum': 1, 'websites': 'a,b,c'}, {'clicknum': 2, 'websites': 'b,c'},
         {'clicknum': 3, 'websites': 'a,c'}, {'clicknum': 2, 'websites': 'a,b'},
         {'clicknum': 1, 'websites': 'a,d'}]

        self.passertEqual(expect, sp1)
        self.passertEqual(expect, sp2)
        self.passertEqual(expect, sp3)

    def test_dict_to_tuple(self):
        """ test """
        sp = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, ['websites', 'clicknum']) \
            .apply(schema.dict_to_tuple, ['websites', 'clicknum'])

        expect = [('a,b,c', 1), ('b,c', 2), ('a,c', 3), ('a,b', 2), ('a,d', 1)]

        self.passertEqual(expect, sp)

    def test_describe(self):
        """ test """
        sp = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, [('websites', str), ('clicknum', int)])
        expect = {'clicknum': int, 'websites': str}
        self.assertItemsEqual(expect, schema.describe(sp))

    def test_select(self):
        """ test """
        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2),
            ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
			.apply(transforms.map, lambda x:x, serde=serde.TupleSerde(str, int))\
		 	.apply(schema.select, lambda websties, clicknum: (
		        websties.flat_map(lambda line: line.split(',')),
		        clicknum))

        expect = \
		[('b', 2L), ('c', 2L), ('a', 3L), ('c', 3L), ('a', 1L), ('b', 1L), ('c', 1L),
		 ('a', 2L), ('b', 2L), ('a', 1L), ('d', 1L)]

        self.passertEqual(expect, pc)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2),
            ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
			.apply(transforms.map, lambda x:x, serde=serde.TupleSerde(str, int))\
		 	.apply(schema.select, lambda websties, clicknum: (
		        websties.flat_map(lambda line: line.split(',')),
		        1))
        expect = \
        [('a', 1), ('b', 1), ('c', 1), ('b', 1), ('c', 1), ('a', 1),
         ('c', 1), ('a', 1), ('b', 1), ('a', 1), ('d', 1)]

        self.passertEqual(expect, pc)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2),
            ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
			.apply(transforms.map, lambda x:x, serde=serde.TupleSerde(str, int))\
		 	.apply(schema.select, lambda websties, clicknum: (
		        'bigflow',
		        1))
        expect = \
        [('bigflow', 1), ('bigflow', 1), ('bigflow', 1), ('bigflow', 1), ('bigflow', 1)]

        self.passertEqual(expect, pc)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum']) \
		    .apply(schema.select, lambda cols: {
		        'website': cols['websites'].flat_map(lambda line: line.split(',')),
		        'clicknum': cols['clicknum']})

        expect = \
		[{'clicknum': 1, 'website': 'a'}, {'clicknum': 1, 'website': 'd'},
         {'clicknum': 3, 'website': 'a'}, {'clicknum': 3, 'website': 'c'},
         {'clicknum': 1, 'website': 'a'}, {'clicknum': 1, 'website': 'b'},
		 {'clicknum': 1, 'website': 'c'}, {'clicknum': 2, 'website': 'b'},
         {'clicknum': 2, 'website': 'c'}, {'clicknum': 2, 'website': 'a'},
         {'clicknum': 2, 'website': 'b'}]

        self.passertEqual(expect, sp)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum']) \
		    .apply(schema.select, ['websites'])

        expect = \
        [{'websites': 'a,b,c'}, {'websites': 'b,c'}, {'websites': 'a,c'},
         {'websites': 'a,b'}, {'websites': 'a,d'}]

        self.passertEqual(expect, sp)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum']) \
            .map(lambda x: x, serde = schema.of({'websites': str, 'clicknum': int}))\
		    .apply(schema.select, ['websites'])

        expect = \
        [{'websites': 'a,b,c'}, {'websites': 'b,c'}, {'websites': 'a,c'},
         {'websites': 'a,b'}, {'websites': 'a,d'}]

        self.passertEqual(expect, sp)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum'])\
            .apply(schema.select, lambda cols: {
                'website': cols['websites'].flat_map(lambda line: line.split(',')),
                'clicknum': 100})
        expect = \
        [{'clicknum': 100, 'website': 'a'}, {'clicknum': 100, 'website': 'b'},
         {'clicknum': 100, 'website': 'c'}, {'clicknum': 100, 'website': 'b'},
         {'clicknum': 100, 'website': 'c'}, {'clicknum': 100, 'website': 'a'},
         {'clicknum': 100, 'website': 'c'}, {'clicknum': 100, 'website': 'a'},
         {'clicknum': 100, 'website': 'b'}, {'clicknum': 100, 'website': 'a'},
         {'clicknum': 100, 'website': 'd'}]

        self.passertEqual(expect, sp)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum'])\
            .apply(schema.select, lambda cols: {
                'website': 'bigflow',
                'clicknum': 100})
        expect = \
        [{'clicknum': 100, 'website': 'bigflow'}, {'clicknum': 100, 'website': 'bigflow'},
         {'clicknum': 100, 'website': 'bigflow'}, {'clicknum': 100, 'website': 'bigflow'},
         {'clicknum': 100, 'website': 'bigflow'}]

        self.passertEqual(expect, sp)

        sideinput = self._pipeline.parallelize(2)
        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum'])\
            .apply(schema.select, lambda cols, sd: {
                'website': cols['websites'].flat_map(lambda line: line.split(',')),
                'clicknum':cols['clicknum'] > sd}, sideinput)
        expect = \
        [{'clicknum': False, 'website': 'a'}, {'clicknum': False, 'website': 'b'},
         {'clicknum': False, 'website': 'c'}, {'clicknum': False, 'website': 'b'},
         {'clicknum': False, 'website': 'c'}, {'clicknum': True, 'website': 'a'},
         {'clicknum': True, 'website': 'c'}, {'clicknum': False, 'website': 'a'},
         {'clicknum': False, 'website': 'b'}, {'clicknum': False, 'website': 'a'},
         {'clicknum': False, 'website': 'd'}]

        self.passertEqual(expect, sp)

        sideinput = self._pipeline.parallelize(2)
        sp = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2),
            ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
            .apply(transforms.map, lambda x:x, serde=serde.TupleSerde(str, int))\
            .apply(schema.select, lambda websties, clicknum, tp: (
                websties.flat_map(lambda line: line.split(',')),
                clicknum > tp), sideinput)
        expect = \
        [('a', False), ('b', False), ('c', False), ('b', False), ('c', False),
         ('a', True), ('c', True), ('a', False), ('b', False), ('a', False), ('d', False)]

        self.passertEqual(expect, sp)

    def test_agg(self):
        """ test """
        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)]) \
        	 .map(lambda x:x, serde=serde.TupleSerde(str, int))
        sp =pc.apply(schema.agg, lambda websties, clicknum:(
                websties.flat_map(lambda line: line.split(',')),
                clicknum.max(),
                clicknum.sum()))

        expect = \
        [('a', 3, 9), ('b', 3, 9), ('c', 3, 9), ('b', 3, 9),
         ('c', 3, 9), ('a', 3, 9), ('c', 3, 9), ('a', 3, 9),
         ('b', 3, 9), ('a', 3, 9), ('d', 3, 9)]

        self.passertEqual(expect, sp)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum']) \
            .apply(schema.agg, lambda cols: {
                'website': cols['websites'].flat_map(lambda line: line.split(',')),
                'max_click_num': cols['clicknum'].max(),
                'sum_click_num': cols['clicknum'].sum()})

        expect = \
		[{'sum_click_num': 9, 'website': 'a', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'b', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'c', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'b', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'c', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'a', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'c', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'a', 'max_click_num': 3},
         {'sum_click_num': 9, 'website': 'b', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'a', 'max_click_num': 3},
		 {'sum_click_num': 9, 'website': 'd', 'max_click_num': 3}]

        self.passertEqual(expect, sp)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum']) \
            .apply(schema.agg, lambda cols: {
                'website': cols['websites'].flat_map(lambda line: line.split(',')),
                'click_num': 100})
        expect = \
        [{'click_num': 100, 'website': 'a'}, {'click_num': 100, 'website': 'b'},
         {'click_num': 100, 'website': 'c'}, {'click_num': 100, 'website': 'b'},
         {'click_num': 100, 'website': 'c'}, {'click_num': 100, 'website': 'a'},
         {'click_num': 100, 'website': 'c'}, {'click_num': 100, 'website': 'a'},
         {'click_num': 100, 'website': 'b'}, {'click_num': 100, 'website': 'a'},
         {'click_num': 100, 'website': 'd'}]

        self.passertEqual(expect, sp)

        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum']) \
            .apply(schema.agg, lambda cols: {
                'website': 'bigflow',
                'click_num': 100})
        expect = \
        [{'click_num': 100, 'website': 'bigflow'}, {'click_num': 100, 'website': 'bigflow'},
         {'click_num': 100, 'website': 'bigflow'}, {'click_num': 100, 'website': 'bigflow'},
         {'click_num': 100, 'website': 'bigflow'}]

        self.passertEqual(expect, sp)

        sp = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2),
            ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
            .apply(transforms.map, lambda x:x, serde=serde.TupleSerde(str, int))\
            .apply(schema.agg, lambda websties, clicknum:(
                         websties.flat_map(lambda line: line.split(',')),
                         100))
        expect = \
        [('a', 100), ('b', 100), ('c', 100), ('b', 100), ('c', 100), ('a', 100),
         ('c', 100), ('a', 100), ('b', 100), ('a', 100), ('d', 100)]

        self.passertEqual(expect, sp)

        sp = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2),
            ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
            .apply(transforms.map, lambda x:x, serde=serde.TupleSerde(str, int))\
            .apply(schema.agg, lambda websties, clicknum:(
                         'bigflow',
                         100))
        expect = \
        [('bigflow', 100), ('bigflow', 100), ('bigflow', 100), ('bigflow', 100), ('bigflow', 100)]

        self.passertEqual(expect, sp)

        sideinput = self._pipeline.parallelize(6)
        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.apply(schema.tuple_to_dict, ['websites', 'clicknum'])\
            .apply(schema.select, lambda cols: {
                'website': cols['websites'].flat_map(lambda line: line.split(',')),
                'clicknum':cols['clicknum']})\
            .apply(schema.group_by, ['website'])\
            .apply_values(schema.agg, lambda cols, tp:{
                'max_click_num': cols['clicknum'].max(),
                'sum_click_num': cols['clicknum'].sum(),
                'flag': cols['clicknum'].sum() > tp}, sideinput)\
            .apply(schema.flatten)
        expect = \
        [{'sum_click_num': 5, 'website': 'b', 'flag': False, 'max_click_num': 2},
         {'sum_click_num': 1, 'website': 'd', 'flag': False, 'max_click_num': 1},
         {'sum_click_num': 7, 'website': 'a', 'flag': True, 'max_click_num': 3},
         {'sum_click_num': 6, 'website': 'c', 'flag': False, 'max_click_num': 3}]

        self.passertEqual(expect, sp)

        sideinput = self._pipeline.parallelize(6)
        sp = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2),
            ('a,c', 3), ('a,b', 2), ('a,d', 1)])\
            .apply(transforms.map, lambda x:x, serde=serde.TupleSerde(str, int))\
            .apply(schema.agg, lambda websties, clicknum, tp:(
                         websties.flat_map(lambda line: line.split(',')),
                         clicknum.max(),
                         clicknum.sum(),
                         clicknum.sum() > tp), sideinput)
        expect = \
        [('a', 3, 9, True), ('b', 3, 9, True), ('c', 3, 9, True), ('b', 3, 9, True),
         ('c', 3, 9, True), ('a', 3, 9, True), ('c', 3, 9, True), ('a', 3, 9, True),
         ('b', 3, 9, True), ('a', 3, 9, True), ('d', 3, 9, True)]

        self.passertEqual(expect, sp)

    def test_group_by(self):
        """ test """
        pc = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])
        sp = pc.map(
            lambda x:x, serde=serde.TupleSerde(str, int)) \
        	.apply(schema.select, lambda websties, clicknum: (
                websties.flat_map(lambda line: line.split(',')),
                clicknum)) \
            .apply(schema.group_by, [0])\
        	.apply_values(schema.agg, lambda webstie, clicknum:(
                clicknum.max(),
                clicknum.sum(),
                clicknum.sum()/clicknum.count()))\
        	.flatten_values()

        expect = [(2, 5, 1L), (1, 1, 1L), (3, 7, 1L), (3, 6, 2L)]

        self.passertEqual(expect, sp)

        sp = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
        	.apply(schema.tuple_to_dict, ['websites', 'clicknum'])\
        	.apply(schema.select, lambda cols: {
                         'website': cols['websites'].flat_map(lambda line: line.split(',')),
                         'clicknum': cols['clicknum']
                     })\
        	.apply(schema.group_by, ['website'])\
        	.apply_values(schema.agg, lambda cols:{
        		'max_click_num': cols['clicknum'].max(),
            	'sum_click_num': cols['clicknum'].sum(),
        		'avg_click_num': cols['clicknum'].sum() / cols['clicknum'].count()})\
        	.flatten_values()

        expect = \
        [{'sum_click_num': 5, 'avg_click_num': 1L, 'max_click_num': 2},
         {'sum_click_num': 1, 'avg_click_num': 1L, 'max_click_num': 1},
         {'sum_click_num': 7, 'avg_click_num': 1L, 'max_click_num': 3},
         {'sum_click_num': 6, 'avg_click_num': 2L, 'max_click_num': 3}]

        self.passertEqual(expect, sp)

    def test_join(self):
        """ test """
        sp1 = self._pipeline.parallelize([('a', 2), ('e', 4), ('c', 6)])\
        	.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        sp2 = self._pipeline.parallelize([('a', 9), ('b', 8), ('d', 7)])\
        	.apply(schema.tuple_to_dict, ['websites', 'click'])
        sp3 = self._pipeline.parallelize([{'websites': 'a', 'clicknum': 2},
                {'websites': 'b', 'clicknum': 3}])\
            .map(lambda x: x, serde = schema.of({'websites': str, 'clicknum': int}))
        jsp = sp1.apply(schema.join, sp2, fields=['websites'])

        expect = [({'clicknum': 2, 'websites': 'a'}, {'click': 9, 'websites': 'a'})]

        self.passertEqual(expect, jsp)

        jsp = sp1.apply(schema.left_join, sp2, fields=['websites'])

        expect = \
        [({'clicknum': 4, 'websites': 'e'}, {'click': None, 'websites': None}),
         ({'clicknum': 2, 'websites': 'a'}, {'click': 9, 'websites': 'a'}),
         ({'clicknum': 6, 'websites': 'c'}, {'click': None, 'websites': None})]

        self.passertEqual(expect, jsp)

        jsp = sp3.apply(schema.left_join, sp1, fields=['websites'])

        expect = \
        [({'clicknum': 2, 'websites': 'a'}, {'clicknum': 2, 'websites': 'a'}),
         ({'clicknum': 3, 'websites': 'b'}, {'clicknum': None, 'websites': None})]

        self.passertEqual(expect, jsp)

        jsp = sp1.apply(schema.right_join, sp2, fields=['websites'])

        expect = \
        [({'clicknum': None, 'websites': None}, {'click': 8, 'websites': 'b'}),
         ({'clicknum': None, 'websites': None}, {'click': 7, 'websites': 'd'}),
         ({'clicknum': 2, 'websites': 'a'}, {'click': 9, 'websites': 'a'})]

        self.passertEqual(expect, jsp)

        jsp = sp1.apply(schema.full_join, sp2, fields=['websites'])

        expect = \
        [({'clicknum': 4, 'websites': 'e'}, {'click': None, 'websites': None}),
         ({'clicknum': None, 'websites': None}, {'click': 8, 'websites': 'b'}),
         ({'clicknum': None, 'websites': None}, {'click': 7, 'websites': 'd'}),
         ({'clicknum': 2, 'websites': 'a'}, {'click': 9, 'websites': 'a'}),
         ({'clicknum': 6, 'websites': 'c'}, {'click': None, 'websites': None})]

        self.passertEqual(expect, jsp)

    def test_optional_join(self):
        """ test """
        sp1 = self._pipeline.parallelize([('a', 1), ('b', 2)]) \
            .apply(schema.tuple_to_dict, [('websites', serde.StrSerde()),
                                          ('clicknum', serde.IntSerde())])
        sp2 = self._pipeline.parallelize([('a', 2), ('c', 3)]) \
            .apply(schema.tuple_to_dict, ['websites', 'clicknum']) \
            .apply(schema.group_by, ['websites']) \
            .apply_values(schema.agg, lambda record: {'websites': record['websites'],
                                               'clicknum': record['clicknum'].count()}) \
            .apply(schema.flatten)

        jsp = sp2.apply(schema.join, sp1, fields=['websites'])

        expect = [({'clicknum': 1, 'websites': 'a'}, {'clicknum': 1, 'websites': 'a'})]

        self.passertEqual(expect, jsp)

        jsp = sp2.apply(schema.left_join, sp1, fields=['websites'])

        expect = \
        [({'clicknum': 1, 'websites': 'a'}, {'clicknum': 1, 'websites': 'a'}),
         ({'clicknum': 1, 'websites': 'c'}, {'clicknum': None, 'websites': None})]

        self.passertEqual(expect, jsp)

        jsp = sp2.apply(schema.right_join, sp1, fields=['websites'])

        expect = [({'clicknum': None, 'websites': None}, {'clicknum': 2, 'websites': 'b'}),
                  ({'clicknum': 1, 'websites': 'a'}, {'clicknum': 1, 'websites': 'a'})]

        self.passertEqual(expect, jsp)

        jsp = sp2.apply(schema.full_join, sp1, fields=['websites'])

        expect = [({'clicknum': None, 'websites': None}, {'clicknum': 2, 'websites': 'b'}),
                  ({'clicknum': 1, 'websites': 'a'}, {'clicknum': 1, 'websites': 'a'}),
                  ({'clicknum': 1, 'websites': 'c'}, {'clicknum': None, 'websites': None})]

        self.passertEqual(expect, jsp)

    def test_schema(self):
        """ test """
        website_click = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, ['websites', 'clicknum'])\
            .apply(schema.select, lambda cols: {
                      'website': cols['websites'].flat_map(lambda line: line.split(',')),
                      'clicknum': cols['clicknum']
                         })
        analytics = website_click.apply(schema.group_by, ['website'])\
            .apply_values(schema.agg, lambda cols: {'max_click_num': cols['clicknum'].max(),
                'sum_click_num': cols['clicknum'].sum(),
                'avg_click_num': cols['clicknum'].sum() / cols['clicknum'].count()})\
            .apply(schema.flatten)


        expect =  \
        [{'sum_click_num': 5, 'website': 'b', 'avg_click_num': 1L, 'max_click_num': 2},
         {'sum_click_num': 1, 'website': 'd', 'avg_click_num': 1L, 'max_click_num': 1},
         {'sum_click_num': 7, 'website': 'a', 'avg_click_num': 1L, 'max_click_num': 3},
         {'sum_click_num': 6, 'website': 'c', 'avg_click_num': 2L, 'max_click_num': 3}]

        self.passertEqual(expect, analytics)

        dict_to_tuple = analytics.apply(schema.dict_to_tuple, ['max_click_num', 'sum_click_num'])
        expect = [(2, 5), (1, 1), (3, 7), (3, 6)]
        self.passertEqual(expect, dict_to_tuple)

        p1 = self._pipeline.parallelize([('a', 1), ('c', 2), ('a', 3), ('b', 2), ('d', 1)])
        sp1 = p1.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        p2 = self._pipeline.parallelize([('a', 5), ('c', 6), ('a', 7), ('b', 8), ('d', 9)])
        sp2 = p2.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        csp = sp1.apply(schema.cogroup, sp2, fields=['websites'])
        gg = csp.apply_values(lambda x, y: transforms.union(x, y)).apply(schema.flatten)

        expect = [{'clicknum': 8, 'websites': 'b'}, {'clicknum': 2, 'websites': 'b'},
                  {'clicknum': 9, 'websites': 'd'}, {'clicknum': 1, 'websites': 'd'},
                  {'clicknum': 5, 'websites': 'a'}, {'clicknum': 7, 'websites': 'a'},
                  {'clicknum': 1, 'websites': 'a'}, {'clicknum': 3, 'websites': 'a'},
                  {'clicknum': 6, 'websites': 'c'}, {'clicknum': 2, 'websites': 'c'}]
        self.passertEqual(expect, gg)

    def test_distinct(self):
        """ inner """
        result = self._pipeline.parallelize([('a', 2), ('e', 4), ('a', 2)]) \
            .apply(schema.tuple_to_dict, ['website', 'clicknum']) \
            .apply(schema.distinct, ['website'])

        self.passertEqual([{'website': 'a', 'clicknum': 2}, {'website': 'e', 'clicknum': 4}],
                result)

    def test_schema_select_after_groupby(self):
        """ test """
        website_click = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, ['websites', 'clicknum'])\
            .apply(schema.select, lambda cols: {
                      'website': cols['websites'].flat_map(lambda line: line.split(',')),
                      'clicknum': cols['clicknum']
                         })
        analytics = website_click.apply(schema.group_by, ['website'])\
            .apply_values(schema.select, lambda cols: cols)\
            .apply_values(schema.agg, lambda cols: {'max_click_num': cols['clicknum'].max(),
                'sum_click_num': cols['clicknum'].sum(),
                'avg_click_num': cols['clicknum'].sum() / cols['clicknum'].count()})\
            .apply(schema.flatten)

        expect =  \
        [{'sum_click_num': 5, 'website': 'b', 'avg_click_num': 1L, 'max_click_num': 2},
         {'sum_click_num': 1, 'website': 'd', 'avg_click_num': 1L, 'max_click_num': 1},
         {'sum_click_num': 7, 'website': 'a', 'avg_click_num': 1L, 'max_click_num': 3},
         {'sum_click_num': 6, 'website': 'c', 'avg_click_num': 2L, 'max_click_num': 3}]

        self.passertEqual(expect, analytics)

    def test_schema_complex_case(self):
        """complex case of schema"""
        _split = udf.flat_map(lambda x: x.split(','))
        sp1 = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, ['websites', 'clicknum'])

        sp2 = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, ['websites', 'clicknum'])

        n = sp1.count()

        sp3 = sp1.union(sp2).apply(schema.select, lambda _: {
            'website': _split(_['websites']),
             'clicknum': _['clicknum']
            })\
            .apply(schema.group_by, ['website']) \
            .apply_values(schema.select, lambda _: _) \
            .apply(schema.flatten)
            #.apply_values(schema.select, lambda _: _) \
            #.apply_values(schema.update, lambda _, n: {'large_than_three': _['clicknum'] > n}, n) \

        result =\
                [{'website':'a', 'clicknum':1}, {'website':'b', 'clicknum':1},
                 {'website':'c', 'clicknum':1}, {'website':'b', 'clicknum':2},
                 {'website':'c', 'clicknum':2}, {'website':'a', 'clicknum':3},
                 {'website':'c', 'clicknum':3}, {'website':'a', 'clicknum':2},
                 {'website':'b', 'clicknum':2}, {'website':'a', 'clicknum':1},
                 {'website':'d', 'clicknum':1}, {'website':'d', 'clicknum':1},
                 {'website':'a', 'clicknum':1}, {'website':'b', 'clicknum':1},
                 {'website':'c', 'clicknum':1}, {'website':'b', 'clicknum':2},
                 {'website':'c', 'clicknum':2}, {'website':'a', 'clicknum':3},
                 {'website':'c', 'clicknum':3}, {'website':'a', 'clicknum':2},
                 {'website':'b', 'clicknum':2}, {'website':'a', 'clicknum':1},
                 ]
        self.passertEqual(result, sp3)

    def test_schema_complex_case1(self):
        """test"""
        def group_and_filter_then_count(p):
            return p.apply(schema.group_by, ['clicknum']) \
                    .apply_values(schema.select, lambda _: _) \
                    .apply_values(transforms.filter, lambda _: _['gt_5'] == _['gt_5_2']) \
                    .apply_values(schema.agg, lambda _: {'count': _['gt_5'].count()}) \
                    .apply(schema.flatten)

        _split = udf.flat_map(lambda x: x.split(','))

        sp1 = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, ['websites', 'clicknum'])

        sp2 = self._pipeline.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3),
            ('a,b', 2), ('a,d', 1)])\
            .apply(schema.tuple_to_dict, ['websites', 'clicknum'])

        n = sp1.count() # 3

        sp3 = sp1.union(sp2).apply(schema.select, lambda _: {
                'website': _split(_['websites']),
                'clicknum': _['clicknum']
            })\
            .apply(schema.group_by, ['website']) \
            .apply_values(schema.select, lambda _: _) \
            .apply_values(schema.select, lambda _: _) \
            .apply_values(schema.update, lambda _, n: {'gt_5': _['clicknum'] > n}, n) \
            .apply_values(schema.update, lambda _, n: {'gt_5_2': n < _['clicknum']}, n) \
            .apply_values(group_and_filter_then_count)\
            .apply(schema.flatten) \
            .apply(schema.select, lambda _: _) \
            .apply(schema.agg, lambda _:{'count':_['count'].sum()})

        c = sp3.get()
        self.assertEqual(c[0]['count'], 22)


if __name__ == "__main__":
    unittest.main()

