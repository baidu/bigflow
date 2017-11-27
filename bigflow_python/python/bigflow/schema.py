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
Last Modified by:  songzhan
Last Modified time: 2016-03-15 09:00:00
"""


from bigflow import transforms
from bigflow import serde
from bigflow import ptable
from bigflow import ptype
from bigflow.transform_impls import group_by_every_record


class FieldsDictSerde(serde.CppSerde):
    """
    Use for the dict with know fields.

    Args:
        fields_to_types (dict): key字段名, vaule相应字段所用的serde。
                                如果只传入字段名, 则表示所有类型都是可被marshal序列化的类型
    """

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

    def get_fields(self):
        """
            Return all fields of FieldsDict
        """
        return self._fields

    def _get_tuple_serde(self, fields):
        """ 内部函数 """
        return serde.of(tuple(self._fields_to_types[key] for key in fields))

    def get_fields_to_types(self):
        """
            Return all fields and corresponding types of FieldsDict
        """
        return self._fields_to_types

    def __str__(self):
        fields_str = ','.join(self._fields)
        fields_str = fields_str[:60] + '...' if fields_str[0:] else ''
        return "Fields(%s)" % fields_str
        #return "Fields<%s>" % (dict(zip(self._fields, map(str, serde.origin(self._tuple_serde)))))


def of(fields_dict):
    """
    创建FieldsDictSerde，用来序列化、反序列化有指定字段的字典。
    因为字段已知，则key无需序列化，序列化出来的数据会小于marshal序列化后的结果。

    Args:
        fields_to_types (dict): 可以传入一个字段名组成的列表，
        也可以传入一个key是字段名，value是相应字段所用的serde的dict。
        如果只传入字段名，则表示所有类型都是可被marshal序列化的类型。

    Returns:
        FieldsDictSerde: 相应的序列化器。

    """
    return FieldsDictSerde(fields_dict)


def _get_serde_of_field(sd, field, dft=None):
    """ 内部函数 """
    if isinstance(sd, FieldsDictSerde):
        return serde.of(serde.origin(sd._tuple_serde)[sd._fields.index(field)])
    return dft


def _get_serde_of_fields(sd, fields, dft=None):
    """ 内部函数 """
    return of(dict((field, _get_serde_of_field(sd, field, dft)) for field in fields))


def _get_tuple_serde_of_fields(sd, fields, dft=None):
    """ 内部函数 """
    return serde.of(tuple(_get_serde_of_field(sd, field, dft) for field in fields))


def _select_cols(val, select_fields):
    """ 内部函数 """
    def _get_field(field):
        return val.map(lambda v: v[field],
                       serde=_get_serde_of_field(
                            val.serde(),
                            field,
                            val.pipeline().default_objector()))
    ret = list(_get_field(field) for field in select_fields)
    ret.append(val)
    return tuple(ret)


def _origin_serde(sd):
    """ 内部函数 """
    if isinstance(sd, serde.Optional):
        sd = sd.origin_serde()
    return sd.get_args()


def _select_cols_tp(val):
    """ 内部函数 """
    def _get_col(index):
        return val.map(lambda v: v[index], serde=serde.of(_origin_serde(val.serde())[index]))

    return tuple(_get_col(index) for index in xrange(len(_origin_serde(val.serde()))))


def _get_out_fields_serde(tpserde, out_fields):
    """ 内部函数 """
    tpserde = serde.origin(tpserde)
    tpserde = map(lambda sd: serde.Optional(sd), tpserde)
    assert len(tpserde) == len(out_fields)
    return of(dict(zip(out_fields, tpserde)))


def _transform_schema_pcollection(pcollection):
    """ 内部函数 """
    from bigflow import schema_pcollection
    return schema_pcollection.SchemaPCollection(pcollection)


def _is_tuple_serde(sd):
    """ 内部函数 """
    if isinstance(sd, serde.Optional):
        sd = sd.origin_serde()
    if isinstance(sd, serde.TupleSerde):
        return True
    return False


def _is_fieldsdict_serde(sd):
    """ 内部函数 """
    if isinstance(sd, serde.Optional):
        sd = sd.origin_serde()
    if isinstance(sd, FieldsDictSerde):
        return True
    return False


def _str_to_list(fields):
    """ 内部函数 """
    if isinstance(fields, str):
        fields = ''.join(fields.split()).split(',')
    return fields


def describe(pcollection):
    """
    获取SchemaPCollection schema, 类似sql操作describe, 查看特定表的详细设计信息

    .. note::  该方法不是一个transform

    Args:
        pcollection (SchemaPCollection): 输入一个SchemaPCollection(FieldsDict)

    Returns:
        dict: key是字段，value是字段类型

    """
    sd = pcollection.serde()
    if isinstance(sd, serde.Optional):
        sd = sd.origin_serde()
    if isinstance(sd, FieldsDictSerde):
        return sd.get_fields_to_types()
    else:
        raise ValueError("SchemaPCollection's serde should be FieldsDictSerde")


def tuple_to_dict(pcollection, fields):
    """
    从每个元素是tuple的PCollection转化成一个SchemaPCollection。

    Args:
        pcollection (PCollection): 输入PCollection，每个元素是一个tuple
        fields (list): fields中每个元素是一个tuple(key, value), key是字段名，value是相应字段所用的serde,
        若fields中每个元素是一个str表示字段名, 则表示所有类型都是可被marshal序列化的类型

    Returns:
        SchemaPCollection: 用来表示结构化的，带字段的PCollection(FieldsDict)，它拥有普通PCollection的所有操作

    """
    fields = _str_to_list(fields)
    from bigflow import schema_pcollection
    order_fields = []
    serde_fields = {}
    for field in fields:
        if isinstance(field, tuple):
            order_fields.append(field[0])
            serde_fields[field[0]] = field[1]
        else:
            order_fields.append(field)
            serde_fields[field] = serde._
    return schema_pcollection.SchemaPCollection(pcollection.map(
        lambda record: dict(zip(order_fields, record)), serde = of(serde_fields)))


def dict_to_tuple(pcollection, fields):
    """
    从一个SchemaPCollection转化成一个每个元素是tuple的PCollection

    Args:
        pcollection (SchemaPCollection): 输入一个SchemaPCollection(FieldsDict)
        fields (Iterable): 一个列表、元组、一个逗号分隔的字符串，表示要输出的字段

    Returns:
        PCollection: 每个元素是fields对应的值构成的tuple

    """
    fields = _str_to_list(fields)
    pcollection = _transform_schema_pcollection(pcollection)
    if _is_tuple_serde(pcollection.serde()):
        raise ValueError("SchemaPCollection(tuple) can't use this function")
    return pcollection.map(lambda record: tuple(record[field] for field in fields),
            serde = pcollection.serde()._get_tuple_serde(fields))


def select(pcollection, select_fn, *args):
    """
    对每条数据选择一些字段进行变换

    Args:
        pcollection (SchemaPCollection): 输入数据集SchemaPCollection(用来表示结构化的, 带字段的PCollection), 可以当作每个元素是一个dict的PCollection来用
        select_fn (callable): 一个函数，表示每条记录要进行什么样的变换，该函数将传入一个dict类型的参数，dict的key是所有的字段名，
                    每个value是一个PObject，表示本条记录中该字段的数据。 用户需要返回一个dict，dict的key是要输出的字段，
                    value是一个分布式数据集(PCollection或PObject)， 表示相关字段下的数据。 最终多个字段下的多个数据集进
                    行笛卡尔积，拼接成最终返回的数据集。
        *args (object):  变换所需要的参数列表

    Returns:
        SchemaPCollection: 每个元素是一个dict的pcollection,
            这个SchemaPCollection中所有元素相当于对原数据每条数据进行一次select_fn处理，
            处理后返回的tuple中的所有数据集进行笛卡尔积，
            最终再把所有输入数据处理后得出的结果拼成一个数据集。

    Examples:
        >>> from bigflow import base, schema
        >>> p = base.Pipeline.create('local')
        >>> analytics = p.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3), ('a,b', 2), ('a,d', 1)])
        >>>     .apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>>     .apply(schema.select, lambda cols: {
        >>>         'website': cols['websites'].flat_map(lambda line: line.split(',')),
        >>>         'clicknum': cols['clicknum']
        >>>     })
        >>> print analytics.get()
    	输出结果为
    	[{'website': 'a', 'clicknum': 1}, {'website': 'b', 'clicknum': 1}, {'website': 'c', 'clicknum': 1},
    	 {'website': 'b', 'clicknum': 2}, {'website': 'c', 'clicknum': 2}, {'website': 'a', 'clicknum': 3},
    	 {'website': 'c', 'clicknum': 3}, {'website': 'a', 'clicknum': 2}, {'website': 'b', 'clicknum': 2},
    	 {'website': 'a', 'clicknum': 1}, {'website': 'd', 'clicknum': 1}]

        >>> from bigflow import schema
        >>> from bigflow import transforms
        >>> from bigflow import base
        >>> pl = base.Pipeline.create("local")
        >>> raw_data = [["xiaoming", "school_1", 12, 150, 90], ]
        >>> data = pl.parallelize(raw_data) \
        >>>         .apply(schema.tuple_to_dict,
        >>>             [("name", str),
        >>>              ("school", str),
        >>>              ("age", int),
        >>>              ("height", int),
        >>>              ("weight", int)])
        >>> fields = {
        >>>     # 复用bigflow提供的transforms：传入一个tuple，提供transforms及自定义函数
        >>>     "name": (transforms.map, lambda name: "My name is " + name),
        >>>     # 提供变化函数：传入一个function
        >>>     "school": lambda school: "My school is " + school,
        >>> }
        >>> output = schema.select(data, fields)
        >>> print output.get()
        输出结果为
        [{'school': 'My school is school_1', 'name': 'My name is xiaoming'}]
    """

    if isinstance(select_fn, list) or isinstance(select_fn, tuple) or isinstance(select_fn, str):
        cols = _str_to_list(select_fn)
        return pcollection.map(lambda record: {col: record.get(col) for col in cols},
                serde = of({col: serde._ for col in cols}))

    pcollection = _transform_schema_pcollection(pcollection)

    if isinstance(select_fn, dict):
        def wrapper_of_udf(select_fields):
            fields_fn = select_fields
            def _apply_udf_in_cols(cols, *args):
                """ only return columns which user select """
                result = {}
                for field, tf in fields_fn.items():
                    if callable(tf):
                        result[field] = cols[field].apply(tf)
                    elif isinstance(tf, tuple):
                        result[field] = cols[field].apply(*tf)
                    else:
                        result[field] = tf
                return result
            return _apply_udf_in_cols

        return select(pcollection, wrapper_of_udf(select_fn))

    if _is_tuple_serde(pcollection.serde()):
        def _pack_udf(*val):
            """ 内部函数 """
            ret_tuple = select_fn(*val)
            return _ret_tuple_handler(ret_tuple, val[0])

        from bigflow import schema_pcollection
        return schema_pcollection.SchemaPCollection(
            pcollection.apply(group_by_every_record.group_by_every_record)
            .apply_values(transforms.first)
            .apply_values(_select_cols_tp)
            .apply_values(_pack_udf, *args)
            .flatten_values())


    def _pack_udf(*val):
        """ 内部函数 """
        sep_postion = len(select_fields) + 1
        record_val= val[sep_postion - 1]
        ret_dict = {}
        if len(val) > sep_postion:
            shard_pts = val[sep_postion:]
            ret_dict = select_fn(dict(zip(select_fields, val[0:sep_postion - 1])), *shard_pts)
        else:
            ret_dict = select_fn(dict(zip(select_fields, val[0:sep_postion - 1])))
        return _ret_dict_handler(ret_dict, record_val)

    select_fields = pcollection._get_fields()

    from bigflow import schema_pcollection
    return schema_pcollection.SchemaPCollection(
        pcollection.apply(group_by_every_record.group_by_every_record)
        .apply_values(transforms.first)
        .apply_values(_select_cols, select_fields)
        .apply_values(_pack_udf, *args)
        .flatten_values())


def update(pcollection, update_fn, *args):
    """
    用户可以传递一个字典，key表示要修改的字段，value是一个自定义函数用于修改值
    update将返回原本SchemaPCollection的所有字段，需要修改的字段更新为新值，不需要修改的
    字段保留原本的值
    也可以直接传递一个function表示修改逻辑

    Args:
        pcollection: PCollection(SchemaPCollection)
        update_fn: Python Dict or or Python Object or Function

    Returns:
        pcollection: PCollection(SchemaPCollection), 更新后的PCollection

    Examples:
        >>> data = self._pipeline.parallelize([("xiaoming", "school_1", 12, 150, 90)])
        >>> data = data.apply(schema.tuple_to_dict,
                [("name", str), ("school", str), ("age", int), ("height", int), ("weight", int)])
        >>>
        >>> fields = {
                # 复用bigflow提供的transforms：传入一个tuple，提供transforms及自定义函数
                "name": (transforms.map, lambda name: "My name is "+ name),
                # 提供变化函数：传入一个function
                "school": lambda school: "My school is " + school,
                # 新增一个字段，直接设置一个值
                "score": 100,
            }
        >>>
        >>> data = schema.update(data, fields)
        >>> print data.get()
        [{"name": "My name is xiaoming", "school": "My school is school_1", "age": 12,
            "height": 150, "weight": 90, "score": 100}]
        >>>
    """
    if callable(update_fn):
        return select(pcollection,
                      lambda cols, *args: cols.update(update_fn(cols, *args)) or cols,
                      *args)
    elif isinstance(update_fn, dict):
        def wrapper_of_udf(update_fields):
            fields_fn = update_fields
            def _apply_udf_in_cols(cols, *args):
                """ return all columns """
                for field, tf in fields_fn.items():
                    if callable(tf):
                        #cols[field] = cols[field].apply(tf)
                        cols[field] = tf(cols[field])
                    elif isinstance(tf, tuple):
                        #cols[field] = cols[field].apply(*tf)
                        cols[field] = tf[0](cols[field], *tf[1:])
                    else:
                        cols[field] = tf
                return cols
            return _apply_udf_in_cols

        if not _is_tuple_serde(pcollection.serde()) and \
                not _is_fieldsdict_serde(pcollection.serde()):
            return pcollection.map(wrapper_of_udf(update_fn), *args)
        return select(pcollection, wrapper_of_udf(update_fn), *args)
    else:
        raise ValueError("update_fn only accept callable object or python dict.")


def agg(pcollection, agg_fn, *args):
    """
    把PCollection的全部数据进行聚合

    Args:
       pcollection (SchemaPCollection): 输入数据集SchemaPCollection(用来表示结构化的，带字段的PCollection), 可以当作每个元素是一个dict的PCollection来用
       agg_fn (callable): 一个函数，表示对每个字段进行怎么样的聚合，该函数将传入一个dict类型的参数，dict的key是所有的字段名，每个value是一个
                PCollection，表示该字段的全部数据。 用户需要返回一个dict，dict的key是要输出的字段，value是一个分布式数据集(PCollection
                或PObject)， 表示相关字段下的数据。 最终多个字段下的多个数据集进行笛卡尔积，拼接成最终返回的数据集。
       *args (object):  变换所需要的参数列表

    Returns:
        SchemaPCollection: 返回一个每个元素是一个dict的pcollection, 其中所有元素输出的几个pcollection进行笛卡尔积并添加字段名后的结果

    Examples:
        >>> from bigflow import base, schema
        >>> p = base.Pipeline.create('local')
        >>> analytics = p.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3), ('a,b', 2), ('a,d', 1)])
        >>>     .apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>>     .apply(schema.select, lambda cols: {
        >>>         'website': cols['websites'].flat_map(lambda line: line.split(',')),
        >>>         'clicknum': cols['clicknum']
        >>>     }).apply(schema.group_by, ['website'])
        >>> .apply_values(schema.agg, lambda cols: {
    	>>> 'max_click_num': cols['clicknum'].max(),
    	>>> 'sum_click_num': cols['clicknum'].sum(),
    	>>> 'avg_click_num': cols['clicknum'].sum() / cols['clicknum'].count()
    	>>> }).apply(schema.flatten)
        >>> print analytics.get()
    	输出结果为:
        [{'sum_click_num': 7, 'website': 'a', 'avg_click_num': 1, 'max_click_num': 3},
    	{'sum_click_num': 6, 'website': 'c', 'avg_click_num': 2, 'max_click_num': 3},
    	{'sum_click_num': 5, 'website': 'b', 'avg_click_num': 1, 'max_click_num': 2},
    	{'sum_click_num': 1, 'website': 'd', 'avg_click_num': 1, 'max_click_num': 1}]

    """
    pcollection = _transform_schema_pcollection(pcollection)

    if _is_tuple_serde(pcollection.serde()):
        def _pack_udf():
            """ 内部函数 """
            tp = list(pcollection.apply(_select_cols_tp))
            tp.extend(args)
            ret_tuple = agg_fn(*tp)
            return _ret_tuple_handler(ret_tuple, tp[0])
        from bigflow import schema_pcollection
        return schema_pcollection.SchemaPCollection(_pack_udf())

    def _pack_udf(val, *args):
        """ 内部函数 """
        record_val = val[-1]
        ret_dict = agg_fn(dict(zip(agg_fields, val[:-1])), *args)
        return _ret_dict_handler(ret_dict, record_val)

    agg_fields = pcollection._get_fields()

    return _pack_udf(pcollection.apply(_select_cols, agg_fields), *args)


def group_by(pcollection, fields, **options):
    """
	对pcollection按字段group by

    Args:
        pcollection (SchemaPCollection): 输入数据集SchemaPCollection(用来表示结构化的，带字段的PCollection), 可以当作每个元素是一个dict的PCollection来用
        fields (Iterable): 如果fields为一个str，则会按“,”进行切割，然后按切割出的字段进行分组。
                           如果fields为一个list/tuple，则直接按list中的多个字段进行分组

    Returns:
        SchemaPCollection: 每个key为group的字段所组成的一个dict，每个value是一个PCollection，包含所有的列。

    Examples:
        >>> from bigflow import base, schema
        >>> p = base.Pipeline.create('local')
        >>> analytics = p.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3), ('a,b', 2), ('a,d', 1)])
        >>>     .apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>>     .apply(schema.select, lambda cols: {
        >>>         'website': cols['websites'].flat_map(lambda line: line.split(',')),
        >>>         'clicknum': cols['clicknum']
        >>>     }).apply(schema.group_by, ['website'])
        >>>       .apply_values(transforms.first)
        >>>     .apply(schema.flatten)
        >>> print analytics.get()
       输出结果为:
        [{'website': 'a', 'clicknum': 1}, {'website': 'b', 'clicknum': 1}, {'website': 'c', 'clicknum': 1},
         {'website': 'd', 'clicknum': 1}]

    """
    pcollection = _transform_schema_pcollection(pcollection)

    if _is_tuple_serde(pcollection.serde()):
        def _select_fields(tup, fields):
            """ 内部函数 """
            return tuple(tup[field] for field in fields)

        from bigflow import schema_pcollection
        return pcollection.group_by(lambda record: _select_fields(record, fields),
            key_serde=serde.of(tuple(_origin_serde(pcollection.serde())[field]
            for field in fields)), **options)\
            .apply_values(lambda record: schema_pcollection.SchemaPCollection(record))

    def _select_fields(dct, fields):
        """ 内部函数 """
        return dict((field, dct[field]) for field in fields)

    if isinstance(fields, str):
        fields = ''.join(fields.split()).split(',')

    from bigflow import schema_pcollection
    return pcollection.group_by(lambda record: _select_fields(record, fields),
        key_serde=_get_serde_of_fields(pcollection.serde(), fields,
        pcollection.pipeline().default_objector()), **options) \
        .apply_values(lambda record: schema_pcollection.SchemaPCollection(record))


def flatten(ptype):
    """
	把PTable的所有value拼接上key，并打平成一个SchemaPCollection

    Args:
		ptype (PTable): 输入数据集，需要是一个PTable，key,value都必须为字典

    Returns:
        SchemaPCollection: 返回的SchemaPCollection拥有key,value所有的字段，如果key,value中有相同字段，则以value为准

    Examples:
        >>> from bigflow import base, schema
        >>> p = base.Pipeline.create('local')
        >>> analytics = p.parallelize([('a,b,c', 1), ('b,c', 2), ('a,c', 3), ('a,b', 2), ('a,d', 1)])
        >>>     .apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>>     .apply(schema.select, lambda cols: {
        >>>         'website': cols['websites'].flat_map(lambda line: line.split(',')),
        >>>         'clicknum': cols['clicknum']
        >>>     }).apply(schema.group_by, ['website'])
        >>>     .apply(schema.flatten)
        >>> print analytics.get()
    	输出结果为:
    	[{'website': 'a', 'clicknum': 1}, {'website': 'b', 'clicknum': 1}, {'website': 'c', 'clicknum': 1},
    	 {'website': 'b', 'clicknum': 2}, {'website': 'c', 'clicknum': 2}, {'website': 'a', 'clicknum': 3},
    	 {'website': 'c', 'clicknum': 3}, {'website': 'a', 'clicknum': 2}, {'website': 'b', 'clicknum': 2},
    	 {'website': 'a', 'clicknum': 1}, {'website': 'd', 'clicknum': 1}]
    """
    def _new_dict(*dicts):
        """ 内部函数 """
        ret = {}
        for dct in dicts:
            ret.update(dct)
        return ret

    def _merge_kv(tp, level):
        """ 内部函数 """
        kvs=[]
        for i in xrange(level):
            kvs.append(tp[0])
            tp = tp[1]
        kvs.append(tp)
        return kvs

    def _merge_serde(serdes, dft=None):
        """ 内部函数 """
        ret = {}
        for sd in serdes:
            if not isinstance(serde.origin(sd), FieldsDictSerde):
                return dft
            for field, field_serde in serde.origin(sd)._fields_to_types.iteritems():
                ret[field] = field_serde
        return of(ret)

    if not isinstance(ptype, ptable.PTable):
        raise ValueError("flatten should only be applied on PTable")

    level = ptype.nested_level() + 1
    from bigflow import schema_pcollection
    return schema_pcollection.SchemaPCollection(ptype.flatten().map(
        lambda kv: _new_dict(*_merge_kv(kv, level)),
        serde=_merge_serde(ptype.key_serdes() + [ptype.serde()],
	    ptype.pipeline().default_objector())))


def _ret_dict_handler(ret_dict, record_val):
    """ 内部函数 """
    inter_type_keys = []
    inter_type_values = []
    inter_type_flag = False
    ptype_keys = []
    ptype_values = []
    ptype_flag = False

    for key, value in ret_dict.items():
        if isinstance(value, ptype.PType):
            ptype_keys.append((key, value.serde()))
            ptype_values.append(value)
            ptype_flag = True
        else:
            inter_type_keys.append((key, type(value)))
            inter_type_values.append(value)
            inter_type_flag = True

    if ptype_flag and inter_type_flag:
        ptype_keys.extend(inter_type_keys)
        return tuple_to_dict(transforms.cartesian(*ptype_values)\
            .apply(transforms.map, lambda record: record + tuple(inter_type_values)),
                ptype_keys)
    elif not ptype_flag and inter_type_flag:
        from bigflow import schema_pcollection
        return schema_pcollection.SchemaPCollection(record_val.apply(transforms.map,
            lambda record: dict(zip(tuple(key_sd[0] for key_sd in inter_type_keys),
                inter_type_values)), serde=of(dict(inter_type_keys))))
    else:
        return tuple_to_dict(transforms.cartesian(*ptype_values), ptype_keys)


def _ret_tuple_handler(ret_tuple, record_val):
    """ 内部函数 """
    inter_type_sds = []
    inter_type_values = []
    inter_type_flag = False
    ptype_sds = []
    ptype_values = []
    ptype_flag = False

    for item in ret_tuple:
        if isinstance(item, ptype.PType):
            ptype_sds.append((item.serde()))
            ptype_values.append(item)
            ptype_flag = True
        else:
            inter_type_sds.append((type(item)))
            inter_type_values.append(item)
            inter_type_flag = True

    if ptype_flag and inter_type_flag:
        ptype_sds.extend(inter_type_sds)
        return transforms.cartesian(*ptype_values)\
            .apply(transforms.map, lambda record: record + tuple(inter_type_values),
                serde=serde.of(tuple(ptype_sds)))
    elif not ptype_flag and inter_type_flag:
        return record_val.apply(transforms.map,
            lambda record: tuple(inter_type_values), serde=serde.of(tuple(inter_type_sds)))
    else:
        return transforms.cartesian(*ptype_values)


def _check_set_args(fields, pcollections):
    """ 内部函数 """
    if fields is None:
        raise ValueError("cogroup must set fields=str/tuple/list(一个列表、元组、一个逗号)"
            "分隔的字符串，表示要group的字段)")
    new_pcollections = []
    for pcollection in pcollections:
        new_pcollections.append(_transform_schema_pcollection(pcollection))
        if _is_tuple_serde(pcollection.serde()):
            raise ValueError("schema.cogroup only applied on SchemaPCollection(FieldsDict)")

    if isinstance(fields, str):
        fields = ''.join(fields.split()).split(',')

    def _get_key_dict(record):
        """ 内部函数 """
        key_dict = {}
        for field in fields:
            key_dict[field] = record[field]
        return key_dict

    ret_pcollection = []
    for p in new_pcollections:
        ret_pcollection.append(p.map(lambda record: (_get_key_dict(record), record),
           serde = serde.TupleSerde(_get_serde_of_fields(p.serde(), fields), p.serde())))
    return ret_pcollection


def _get_none_dict(pcollections):
    """ 内部函数 """
    def _get_dict(fields):
        """ 内部函数 """
        none_dict = {}
        for field in fields:
            none_dict[field] = None
        return none_dict
    new_pcollections = []
    for pcollection in pcollections:
        new_pcollections.append(_transform_schema_pcollection(pcollection))
    return [_get_dict(pcollection._get_fields()) for pcollection in new_pcollections]


def _none_to_dict(records, none_dict):
    """ 内部函数 """
    new_record = []
    record = records[1]
    for index in xrange(len(record)):
        if record[index] is None:
            new_record.append(none_dict[index])
        else:
            new_record.append(record[index])
    return tuple(new_record)


def cogroup(*pcollections, **options):
    """
    对多个输入SchemaPCollection进行协同分组，返回一个PTable表示分组结果
    PTable的value为所有输入PCollection
    cogroup会按照输入字段来group到一起

    Args:
        *pcollection (SchemaPCollection): 输入的多个SchemaPCollection
        **options: 配置选项，需要配置fields=str/tuple/list(一个列表、元组、一个逗号分隔的字符串，表示要group的字段)

    Returns:
        PTable: 分组结果

    Examples:
        >>> from bigflow import base, schema, transforms
        >>> p = base.Pipeline.create('local')
        >>> p1 = p.parallelize([('a', 1), ('c', 2), ('a', 3), ('b', 2), ('d', 1)])
		>>> sp1 = p1.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>> p2 = p.parallelize([('a', 6), ('c', 8), ('a', 9), ('b', 0), ('d', 7)])
		>>> sp2 = p2.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
		>>> csp = sp1.apply(schema.cogroup, sp2, fields=['websites'])
		>>> csp.apply_values(lambda x, y: transforms.union(x, y)).apply(schema.flatten).get()
		输出结果为：
       	[{'clicknum': 8, 'websites': 'b'}, {'clicknum': 2, 'websites': 'b'},
	     {'clicknum': 9, 'websites': 'd'}, {'clicknum': 1, 'websites': 'd'},
	     {'clicknum': 5, 'websites': 'a'}, {'clicknum': 7, 'websites': 'a'}, {'clicknum': 1, 'websites': 'a'},
	     {'clicknum': 3, 'websites': 'a'}, {'clicknum': 6, 'websites': 'c'}, {'clicknum': 2, 'websites': 'c'}]
    """
    if len(pcollections) == 0:
        raise ValueError("No argument")

    fields = options.get('fields', None)

    def _trans_to_sp(*records):
        """ 内部函数 """
        from bigflow import schema_pcollection
        return tuple(schema_pcollection.SchemaPCollection(record) for record in records)

    return transforms.cogroup(*_check_set_args(fields, pcollections)) \
            .apply_values(_trans_to_sp)


def _value_serde(sd):
    """ 内部函数 """
    if isinstance(sd, serde.Optional):
        sd = sd.origin_serde()
    sd_val = sd.get_args()[1]
    if isinstance(sd_val, serde.Optional):
        sd_val = sd_val.origin_serde()
    def _optional_sd(sd_val):
        """ 内部函数 """
        new_serde = []
        for fd in sd_val.get_args():
            new_fd = {}
            if isinstance(fd, serde.Optional):
                fd = fd.origin_serde()
            for field, tp in fd.get_fields_to_types().items():
                new_fd[field] = serde.Optional(serde.of(tp))
            new_serde.append(of(new_fd))
        return serde.of(tuple(new_serde))
    return _optional_sd(sd_val)


def _merge_result_after_join(ret, join_type):
    """
    内部函数
    在 join，left_join，right_join，full_join后将结果进行合并
    例如：
        left_join后的结果为 [({'clicknum': 2, 'websites': 'a'}, {'click': 9, 'websites': 'a'})]
        merge后的结果为 [{'clicknum': 2, 'websites': 'a', 'click': 9}]

        left_join后的结果为 [({'clicknum': 2, 'websites': 'a'}, {'clicknum': 9, 'websites': 'a'})]
        merge后的结果为 [{'clicknum': 2, 'website': 'a'}]

    Args:
        ret: PCollection, 进行join or left_join or right_join or full_join 之后的结果
        join_type: Python string，可以是 'join', 'left_join', 'right_join', 'full_join'

    Returns:
        PCollection: merge后的结果
    """

    if join_type not in ["join", "left_join", "right_join", "full_join"]:
        raise ValueError("`join_type` only accept one of "\
                         "{'join', 'left_join', 'right_join', 'full_join'}.")

    # ret.serde() return an Optional(serde.py)
    # we need the origin serde, so call origin_serde()
    # finally, ret_serde is a TupleSerde(serde.py)
    ret_serde = ret.serde().origin_serde()

    # ret_serde is a TupleSerde.
    # get_args() return a python tuple consists of two FieldsDictSerde.
    (left_serde, right_serde) = ret_serde.get_args()

    # fields_to_type is a python dict.
    # key is field name, value is serde, so we merge both of them
    left_fields_to_type = left_serde.get_fields_to_types()
    right_fields_to_type = right_serde.get_fields_to_types()

    if join_type == "right_join":
        merge_serde = left_fields_to_type.update(right_fields_to_type) or left_fields_to_type
    else:
        merge_serde = right_fields_to_type.update(left_fields_to_type) or right_fields_to_type

    if join_type == "right_join":
        return ret.map(lambda t: t[0].update(t[1]) or t[0],
                       serde = of(merge_serde))
    else:
        return ret.map(lambda t: t[1].update(t[0]) or t[1],
                       serde = of(merge_serde))


def join(*pcollections, **options):
    """
    对多个输入SchemaPCollection，根据指定的字段(dict)对SchemaPCollection做内连接操作,
    连接结果为(fields, (value1, value2, ..., value n))

    Args:
        *pcollection (SchemaPCollection): 输入的多个SchemaPCollection
        **options: 配置选项，需要配置fields=str/tuple/list(一个列表、元组、一个逗号分隔的字符串，表示指定的字段)

    Returns:
        SchemaPCollection: 连接结果

    Examples:
        >>> from bigflow import base, schema, transforms
        >>> p = base.Pipeline.create('local')
        >>> p1 = p.parallelize([('a', 2), ('e', 4), ('c', 6)])
		>>> sp1 = p1.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>> p2 = p.parallelize([('a', 9), ('b', 8), ('d', 7)])
		>>> sp2 = p2.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
		>>> csp = sp1.apply(schema.join, sp2, fields=['websites'])
		>>> csp.get()
		输出结果为：
        [({'clicknum': 2, 'websites': 'a'}, {'clicknum': 9, 'websites': 'a'})]
    """
    if len(pcollections) < 2:
        raise ValueError("require at least 2 pcollections")

    fields = options.get('fields', None)
    from bigflow import schema_pcollection
    pc = transforms.join(*_check_set_args(fields, pcollections))
    ret = schema_pcollection.SchemaPCollection(pc.apply(transforms.map,
        lambda record: record[1], serde=_value_serde(pc.serde())))

    # default: merge = False, don't merge the result
    merge = options.get("merge", False)
    if not merge:
        return ret
    else:
        return _merge_result_after_join(ret, "join")


def left_join(*pcollections, **options):
    """
    对多个输入SchemaPCollection，根据指定的字段(dict)对SchemaPCollection做内连接操作,
    连接结果为(fields, (value1, value2, ..., value n)), 若第m个SchemaPCollection没有元素,
    则value m为None

    Args:
        *pcollection (SchemaPCollection): 输入的多个SchemaPCollection
        **options: 配置选项，需要配置fields=str/tuple/list(一个列表、元组、一个逗号分隔的字符串，表示指定的字段)

    Returns:
        SchemaPCollection: 连接结果

    Examples:
        >>> from bigflow import base, schema, transforms
        >>> p = base.Pipeline.create('local')
        >>> p1 = p.parallelize([('a', 2), ('e', 4), ('c', 6)])
		>>> sp1 = p1.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>> p2 = p.parallelize([('a', 9), ('b', 8), ('d', 7)])
		>>> sp2 = p2.apply(schema.tuple_to_dict, ['websites', 'click'])
		>>> csp = sp1.apply(schema.left_join, sp2, fields=['websites'])
		>>> csp.get()
		输出结果为：
        [({'clicknum': 4, 'websites': 'e'}, {'click': None, 'websites': None}),
         ({'clicknum': 2, 'websites': 'a'}, {'click': 9, 'websites': 'a'}),
         ({'clicknum': 6, 'websites': 'c'}, {'click': None, 'websites': None})]

    """
    if len(pcollections) < 2:
        raise ValueError("require at least 2 pcollections")

    fields = options.get('fields', None)
    from bigflow import schema_pcollection
    pc = transforms.left_join(*_check_set_args(fields, pcollections))
    none_dict = _get_none_dict(pcollections)
    ret = schema_pcollection.SchemaPCollection(pc.apply(transforms.map,
        lambda tp: _none_to_dict(tp, none_dict), serde=_value_serde(pc.serde())))

    # default: merge = False, don't merge the result
    merge = options.get("merge", False)
    if not merge:
        return ret
    else:
        return _merge_result_after_join(ret, "left_join")


def right_join(*pcollections, **options):
    """
    对多个输入SchemaPCollection，根据指定的字段(dict)对SchemaPCollection做内连接操作,
    连接结果为(fields, (value1, value2, ..., value n)), 若第m个SchemaPCollection没有元素,
    则value m为None

    Args:
        *pcollection (SchemaPCollection): 输入的多个SchemaPCollection
        **options: 配置选项，需要配置fields=str/tuple/list(一个列表、元组、一个逗号分隔的字符串，表示指定的字段)

    Returns:
        SchemaPCollection: 连接结果

    Examples:
        >>> from bigflow import base, schema, transforms
        >>> p = base.Pipeline.create('local')
        >>> p1 = p.parallelize([('a', 2), ('e', 4), ('c', 6)])
		>>> sp1 = p1.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>> p2 = p.parallelize([('a', 9), ('b', 8), ('d', 7)])
		>>> sp2 = p2.apply(schema.tuple_to_dict, ['websites', 'click'])
		>>> csp = sp1.apply(schema.right_join, sp2, fields=['websites'])
		>>> csp.get()
		输出结果为：
        [({'clicknum': None, 'websites': None}, {'click': 8, 'websites': 'b'}),
         ({'clicknum': None, 'websites': None}, {'click': 7, 'websites': 'd'}),
         ({'clicknum': 2, 'websites': 'a'}, {'click': 9, 'websites': 'a'})]

    """
    if len(pcollections) < 2:
        raise ValueError("require at least 2 pcollections")

    fields = options.get('fields', None)
    from bigflow import schema_pcollection
    pc = transforms.right_join(*_check_set_args(fields, pcollections))
    none_dict = _get_none_dict(pcollections)
    ret = schema_pcollection.SchemaPCollection(pc.apply(transforms.map,
        lambda tp: _none_to_dict(tp, none_dict), serde=_value_serde(pc.serde())))

    # default: merge = False, don't merge the result
    merge = options.get("merge", False)
    if not merge:
        return ret
    else:
        return _merge_result_after_join(ret, "right_join")


def full_join(*pcollections, **options):
    """
    对多个输入SchemaPCollection，根据指定的字段(dict)对SchemaPCollection做内连接操作,
    连接结果为(fields, (value1, value2, ..., value n)), 若第m个SchemaPCollection没有元素,
    则value m为None

    Args:
        *pcollection (SchemaPCollection): 输入的多个SchemaPCollection
        **options: 配置选项，需要配置fields=str/tuple/list(一个列表、元组、一个逗号分隔的字符串，表示指定的字段)

    Returns:
        SchemaPCollection: 连接结果

    Examples:
        >>> from bigflow import base, schema, transforms
        >>> p = base.Pipeline.create('local')
        >>> p1 = p.parallelize([('a', 2), ('e', 4), ('c', 6)])
		>>> sp1 = p1.apply(schema.tuple_to_dict, ['websites', 'clicknum'])
        >>> p2 = p.parallelize([('a', 9), ('b', 8), ('d', 7)])
		>>> sp2 = p2.apply(schema.tuple_to_dict, ['websites', 'click'])
		>>> csp = sp1.apply(schema.full_join, sp2, fields=['websites'])
		>>> csp.get()
		输出结果为：
        [({'clicknum': 4, 'websites': 'e'}, {'click': None, 'websites': None}),
         ({'clicknum': None, 'websites': None}, {'click': 8, 'websites': 'b'}),
         ({'clicknum': None, 'websites': None}, {'click': 7, 'websites': 'd'}),
         ({'clicknum': 2, 'websites': 'a'}, {'click': 9, 'websites': 'a'}),
         ({'clicknum': 6, 'websites': 'c'}, {'click': None, 'websites': None})]

    """
    if len(pcollections) < 2:
        raise ValueError("require at least 2 pcollections")

    fields = options.get('fields', None)
    from bigflow import schema_pcollection
    pc = transforms.full_join(*_check_set_args(fields, pcollections))
    none_dict = _get_none_dict(pcollections)
    ret = schema_pcollection.SchemaPCollection(pc.apply(transforms.map,
        lambda tp: _none_to_dict(tp, none_dict), serde=_value_serde(pc.serde())))

    # default: merge = False, don't merge the result
    merge = options.get("merge", False)
    if not merge:
        return ret
    else:
        return _merge_result_after_join(ret, "full_join")


def distinct(pcollection, columns):
    """
    按指定列去重

    Args:
        pcollection (SchemaPCollection): 输入的pcollecton
        columns (list of str): 要distinct的列

    Returns:
        SchemaPCollection: 做过distinct的pcollection

    Examples:
        >>> from bigflow import base, schema, transforms
        >>> p = base.Pipeline.create('local')
        >>> sp = p.parallelize([('a', 2), ('e', 4), ('a', 3)])
        >>>       .apply(schema.tuple_to_dict, ['website', 'clicknum'])
        >>> print sp.apply(schema.distinct, ['website']).get()
        [{'website': 'a', 'clicknum': 2}, {'website': 'e', 'clicknum': 4}]
    """

    return pcollection.apply(group_by, columns).apply_values(transforms.first).flatten_values()
