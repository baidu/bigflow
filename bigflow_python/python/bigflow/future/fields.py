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
Last Modified time: 2015-12-24 16:18:06
"""

import uuid

from bigflow import transforms
from bigflow import serde

from bigflow import pcollection
from bigflow import ptable
from bigflow import ptype
from bigflow.test import magic


class FieldsDictSerde(serde.CppSerde):
    """ Use for the dict with know fields """
    def __init__(self, fields_to_types):
        if not isinstance(fields_to_types, dict):
            fields_to_types = dict(zip(fields_to_types, [serde._] * len(fields_to_types)))
        self._fields_to_types = fields_to_types
        self._fields = sorted(fields_to_types.keys())
        self._tuple_serde = serde.of(tuple(fields_to_types[key] for key in self._fields))

    def serialize(self, obj):
        """ serialize """
        return self._tuple_serde.serialize(tuple(obj.get(key, None) for key in self._fields))

    def deserialize(self, buf):
        """ deserialize """
        return dict(zip(self._fields, self._tuple_serde.deserialize(buf)))

    def __str__(self):
        fields_str = ','.join(self._fields)
        fields_str = fields_str[:10] + '...' if fields_str[10:] else ''

        return "Fields(%s)" % fields_str
        #return "Fields<%s>" % (dict(zip(self._fields, map(str, serde.origin(self._tuple_serde)))))


def of(fields_dict):
    """
    创建FieldsDictSerde，用来序列化、反序列化有指定字段的字典。
    因为字段已知，则key无需序列化，序列化出来的数据会小于marshal序列化后的结果。

    Args:
      fields_to_types: 可以传入一个字段名组成的列表，
        也可以传入一个key是字段名，value是相应字段所用的serde的dict。
        如果只传入字段名，则表示所有类型都是可被marshal序列化的类型。

    Returns:
        相应的序列化器。

    """
    return FieldsDictSerde(fields_dict)


def get_serde_of_field(sd, field, default=None):
    """ get serde of field """
    if isinstance(sd, FieldsDictSerde):
        return serde.of(serde.origin(sd._tuple_serde)[sd._fields.index(field)])
    return default


def get_serde_of_fields(sd, fields, dft=None):
    """ 内部函数 """
    return of(dict((field, get_serde_of_field(sd, field, dft)) for field in fields))


def select_cols(val, select_fields):
    """ 内部函数 """
    def _get_field(field):
        return val.map(lambda v: v[field],
                       serde=get_serde_of_field(
                            val.serde(),
                            field,
                            val.pipeline().default_objector()))
    return tuple(_get_field(field) for field in select_fields)


def get_out_fields_serde(tpserde, out_fields):
    """ 内部函数 """
    tpserde = serde.origin(tpserde)
    tpserde = map(lambda sd: serde.Optional(sd), tpserde)
    assert len(tpserde) == len(out_fields)
    return of(dict(zip(out_fields, tpserde)))


def select(p, io_description, fn, *args, **kargs):
    """
    对每条数据选择一些字段进行变换。

    Args:
        p (pcollection): 输入数据集，需要是一个每个元素都是dict的pcollection
        io_description (str): 格式为：  a,b=>c,d,e  即，输入字段=>输出字段
        fn (callable) : 函数原型为 (*input_pobjects) => (*output_pcollection_or_pobjects)
                    即，该函数的输入参数为多个pobject，每个pobject表示数据的一个字段，
                    对这个pobject上进行的操作会执行在每行数据上；该函数的返回值是一些pobject或
                    pcollection所组成的tuple（如果只有一个元素可以不必返回tuple）。

    Returns:
        返回一个每个元素是一个dict的pcollection。
        这个pcollection中所有元素相当于对原数据每条数据进行一次fn处理，
        处理后返回的tuple中的所有数据集进行笛卡尔积，
        最终再把所有输入数据处理后得出的结果拼成一个数据集。

    例如：::

        >>> x = _pipeline.parallelize([{'a' : 1, 'b': 2.0}, {'a': 2, 'b': 3.0}])
        >>> print x.apply(fields.select,
        >>>      'a, b => c, d, e',
        >>>      lambda a, b: (
        >>>         a.map(lambda x: x + 1),
        >>>         b.map(lambda x, y: x / y, a),
        >>>         a.flat_map(lambda x: xrange(x))
        >>>     )
        >>> ).get()

        [{'c': 2, 'd': 2.0, 'e': 0}, {'c': 3, 'd': 1.5, 'e': 0}, {'c': 3, 'd': 1.5, 'e': 1}]

    """

    io_fields = ''.join(io_description.split()).split('=>')
    assert len(io_fields) >= 1
    assert len(io_fields) <= 2

    select_fields = io_fields[0].split(',')
    io_fields.append(io_fields[0])
    out_fields = io_fields[1].split(',')
    default_serde = p.pipeline().default_objector()

    tp = p.group_by(lambda o: str(uuid.uuid4()))\
        .apply_values(transforms.first)\
        .apply_values(select_cols, select_fields) \
        .apply_values(fn, *args, **kargs)\
        .apply_values(transforms.cartesian)
    return tp.apply_values(transforms.map,
                           lambda tp: dict(zip(out_fields, tp)),
                           serde=get_out_fields_serde(tp.serde(), out_fields))\
             .flatten_values()


def agg(p, io_description, fn, *args, **kargs):
    """
    选择一些字段去做一些聚合操作。

    Args:
        p (pcollection): 输入数据集，需要是一个每个元素都是dict的pcollection
        io_description (str): 格式为：  a,b=>c,d,e  即，输入字段=>输出字段
        fn (callable) : 函数原型为 (*input_pcollections) => (*output_pcollection_or_pobjects)
                    即，该函数的输入参数为多个pcollection，
                    每个pcollection表示数据的一个字段的全部行所拼成的一个pcollection。
                    该函数的返回值是一些pobject或
                    pcollection所组成的tuple（如果只有一个元素可以不必返回tuple）。

    Returns:
        返回一个每个元素是一个dict的pcollection。
        这个pcollection中所有元素输出的几个pcollection进行笛卡尔积并添加字段名后的结果。

    例如：::

        >>> x = _pipeline.parallelize([{'a' : 1, 'b': 2.0}, {'a': 2, 'b': 3.0}])
        >>> print x.apply(fields.agg,
        >>>      'a, b => c, d, e',
        >>>      lambda a, b: (
        >>>         a.count(),
        >>>         b.sum(),
        >>>         a.flat_map(lambda x: xrange(x))
        >>>     )
        >>> ).get()

        [{'c': 2, 'd': 5.0, 'e': 0}, {'c': 2, 'd': 5.0, 'e': 0}, {'c': 2, 'd': 5.0, 'e': 1}]

    """
    io_fields = ''.join(io_description.split()).split('=>')
    assert len(io_fields) >= 1
    assert len(io_fields) <= 2

    select_fields = io_fields[0].split(',')
    io_fields.append(io_fields[0])
    out_fields = io_fields[1].split(',')

    fields = list(p.apply(select_cols, select_fields))
    fields.extend(args)
    ret = fn(*fields, **kargs)
    if isinstance(ret, ptype.PType):
        ret = (ret,)
    tp = transforms.cartesian(*ret)
    return tp.apply(transforms.map, lambda tp: dict(zip(out_fields, tp)),
            serde=get_out_fields_serde(tp.serde(), out_fields))


def group_by(p, fields):
    """
    按fields分组。

    Args:
        p (pcollection): 输入数据集，需要是一个每个元素都是dict的pcollection
        fields(str/[str]): 如果fields为一个str，则会按“,”进行切割，然后按切割出的字段进行分组。
                           如果fields为一个list，则直接按list中的多个字段进行分组。

    Returns:
        返回一个key为一个包含指定字段的dict，value为原数据集的PTable。

    需要注意的是，由于python原生的dict的key不能为dict，所以，这个返回的PTable上不能调用get操作，
    如果需要get结果，可以先调用flatten。

    例如：::

        >>> x = _pipeline.parallelize([{'a': 1, 'b': 2.0, 'c': 1},
        >>>                            {'a': 1, 'b': 2.0, 'c': 2},
        >>>                            {'a': 2, 'b': 1.0, 'c': 3},
        >>>                            ])

        >>> print (x.apply(fields.group_by, 'a, b')
        >>>         .apply_values(fields.agg,
        >>>                       'a,b,c=>a,b,c',
        >>>                       lambda a, b, c: (a.sum(), b.sum(), c.sum()))
        >>>         .apply(fields.flatten)
        >>>         .get()
        >>> )

        [{'a': 2, 'c': 3, 'b': 1.0}, {'a': 2, 'c': 3, 'b': 4.0}]

    """
    if isinstance(fields, str):
        fields = ''.join(fields.split()).split(',')
    def _select_fields(dct, fields):
        return dict((field, dct[field]) for field in fields)
    return p.group_by(lambda record: _select_fields(record, fields),
            key_serde=get_serde_of_fields(p.serde(), fields, p.pipeline().default_objector()))


def flatten(p):
    """
    打平PTable为一个PCollection。如果K,V中有同样的字段，则以value中为准。

    Args:
        p (pcollection): 输入数据集，需要是一个PTable，key,value中都必须为字典。

    Returns:
        返回一个每个元素是一个dict的pcollection，表示PTable打平后的结果，
        如果key,value中有同样字段，以value为准。

    例如：::

        >>> x = _pipeline.parallelize([{'a': 1, 'b': 2.0, 'c': 1},
        >>>                            {'a': 1, 'b': 2.0, 'c': 2},
        >>>                            {'a': 2, 'b': 1.0, 'c': 3},
        >>>                            ])

        >>> print (x.apply(fields.group_by, 'a, b')
        >>>         .apply_values(fields.agg,
        >>>                       'a,b,c=>a,b,c',
        >>>                       lambda a, b, c: (a.sum(), b.sum(), c.sum()))
        >>>         .apply(fields.flatten)
        >>>         .get()
        >>> )

        [{'a': 2, 'c': 3, 'b': 1.0}, {'a': 2, 'c': 3, 'b': 4.0}]

    """
    def new_dict(*dicts):
        """ inner """
        ret = {}
        for dct in dicts:
            ret.update(dct)
        return ret

    def merge_kv(tp, level):
        """ inner """
        kvs=[]
        for i in xrange(level):
            kvs.append(tp[0])
            tp = tp[1]
        kvs.append(tp)
        return kvs

    if not isinstance(p, ptable.PTable):
        return p

    def merge_serde(serdes, dft=None):
        """ inner """
        ret = {}
        for sd in serdes:
            if not isinstance(sd, FieldsDictSerde):
                return dft
            for field, field_serde in sd._fields_to_types.iteritems():
                ret[field] = field_serde
        return of(ret)
    #print merge_serde(p.key_serdes() + [p.serde()], p.pipeline().default_objector())
    level = p.nested_level() + 1
    return p.flatten().map(lambda kv: new_dict(*merge_kv(kv, level)),
            serde=merge_serde(p.key_serdes() + [p.serde()], p.pipeline().default_objector()))


pcollection.PCollection.select = select
pcollection.PCollection.agg = agg
