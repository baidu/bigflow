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
:class:`bigflow.pcollection.PCollection` 定义

.. module:: bigflow.pcollection
   :synopsis: PCollection

"""

from bigflow import error
from bigflow import ptype
from bigflow import transforms

class PCollection(ptype.PType):
    """
    用于表示分布式数据集的 :class:`bigflow.ptype.PType`

    .. note:: 用户不应当直接使用其构造方法

    Args:
      node (Node):  LogicalPlan.Node
    """

    def accumulate(self, zero, accumulate_fn, *side_inputs, **options):
        """
        等同于
        :func:`bigflow.transforms.accumulate(self, zero, accumulate_fn, *side_inputs, **options)
        <bigflow.transforms.accumulate>`

        Args:
          zero (value or function):  初始值，或是一个返回初始值的方法
          accumulate_fn (function):  聚合方法
          *side_inputs:  参与运算的SideInputs
          **options:  可配置选项

        Returns:
          PObject:  聚合结果

        >>> from operator import add
        >>> p.parallelize([1, 2, 3]).accumulate(0, add).get()
        6
        """
        return transforms.accumulate(self, zero, accumulate_fn, *side_inputs, **options)

    def aggregate(self, zero, aggregate_fn, combine_fn, *side_inputs, **options):
        """
        等同于
        :func:`bigflow.transforms.aggregate(self, aggregate_fn, combine_fn, *side_inputs, **options)
        <bigflow.transforms.aggregate>`

        Args:
          pcollection (PCollection):  输入PCollection
          zero (value or function):  初始值，或是一个返回初始值的方法
          accumulate_fn (function):  聚合方法
          *side_inputs:  参与运算的SideInputs
          **options:  可配置选项

        Returns:
          PObject:  聚合结果

        """

        return transforms.aggregate(self, zero, aggregate_fn, combine_fn, *side_inputs, **options)

    def as_pobject(self):
        """
        等同于 to ``self.first()``

        Returns:
          PObject:  转换结果

        >>> _pipeline.parallelize(["A"]).as_pobject().get()
        "A"

        """
        return self.first()

    def cogroup(self, other, *others):
        """
        等同于
        :func:`bigflow.transforms.cogroup(self, other, *others)
        <bigflow.transforms.cogroup>`,

        Args:
          other (PCollection):  用于协同分组的PCollection
          *others:  更多的PCollection

        Returns:
          PTable:  分组结果
        """
        return transforms.cogroup(self, other, *others)

    def combine(self, fn, **options):
        """
        等同于 :func:`bigflow.transforms.combine(self, fn) <bigflow.transforms.combine>`

        Args:
          fn (callable):  合并函数
          **options:  可配置选项

        Returns:
          PObject:  合并结果

        >>> _pipeline.parallelize([2, 4, 6, 10]).combine(sum).get()
        22
        """
        return transforms.combine(self, fn, **options)

    def count(self, **options):
        """
        返回元素的数量，等同于
        :func:`bigflow.transforms.count(self)
        <bigflow.transforms.count>`

        Returns:
          PObject:  元素数量

        >>> p.parallelize([1, 2, 3, 4]).count().get()
        4
        """
        return transforms.count(self, **options)

    def distinct(self):
        """
        元素去重，等同于
        :func:`bigflow.transforms.distinct(self)
        <bigflow.transforms.distinct>`

        Args:
          **options:  可配置选项

        Returns:
          PCollection:  不重复元素，以PCollection给出

        >>> p.parallelize([2, 2, 1, 9, 3, 3]).distinct().get()
        [2, 3, 1, 9]
        """
        return transforms.distinct(self)

    def filter(self, fn, *side_inputs, **options):
        """
        过滤元素，等同于
        :func:`bigflow.transforms.filter(self, fn, *side_inputs, **options)
        <bigflow.transforms.filter>`,

        Args:
          fn (function):  断言函数
          *side_inputs:  参与运算的SideInputs
          **options:  可配置选项

        Returns:
          PCollection:  过滤结果

        >>> p.parallelize([3, 7, 1, 3, 2, 8]).filter(lambda x: x % 2 == 0).get()
        [2, 8]
        """
        return transforms.filter(self, fn, *side_inputs, **options)

    def map(self, fn, *side_inputs, **options):
        """
        对所有元素进行一对一映射变换，等同于
        :func:`bigflow.transforms.map(self, fn, *side_inputs, **options)
        <bigflow.transforms.map>`

        Args:
          fn (function):  变换函数
          *side_inputs:  参与运算的SideInputs
          **options:  可配置选项

        Returns:
          PCollection:  变换结果

        >>> p.parallelize([1, 3, 5, 7]).map(lambda x: x + 1).get()
        [2, 4, 6, 8]
        """
        return transforms.map(self, fn, *side_inputs, **options)

    def foreach(self, fn, *side_inputs, **options):
        """
        等同于
        :func:`bigflow.transforms.foreach(self, fn, *side_inputs, **options)
        <bigflow.transforms.foreach>`

        Args:
          fn (function):  变换函数
          *side_inputs:  参与运算的SideInputs

        Returns:
          None
        """
        return transforms.foreach(self, fn, *side_inputs, **options)

    def flat_map(self, fn, *side_inputs, **options):
        """
        对所有元素进行一对多映射，等同于
        :func:`bigflow.transforms.flat_map(self, fn, *side_inputs, **options)
        <bigflow.transforms.flat_map>`

        Args:
          fn (function):  变换函数
          *side_inputs:  参与运算的SideInputs
          **options:  可配置选项

        Returns:
          PCollection:  变换结果

        >>> _pipeline.parallelize([1, 3, 5, 7]).flat_map(lambda x: [x, x * 2]).get()
        [1, 2, 3, 5, 6, 7, 10, 14]
        """
        return transforms.flat_map(self, fn, *side_inputs, **options)

    def group_by(self, key_extractor, value_extractor=None, **options):
        """
        对元素分组，等同于
        :func:`bigflow.transforms.group_by(self, key_extractor, value_extractor)
        <bigflow.transforms.group_by>`

        Args:
          key_extractor (function):  用于提取key的函数
          value_extractor (function, optional):  用于提取value的函数
          **options:  可配置选项

        Returns:
          PTable:  分组结果

        >>> _pcollection = _pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
        >>> _grouped = _pcollection.group_by(lambda x: x[0], lambda x: x[1]).get()
        >>> _grouped.get()
        {"A": [4, 3, 1], "B": [2]}
        """
        return transforms.group_by(self, key_extractor, value_extractor, **options)

    def group_by_key(self, **options):
        """
        与 ``group_by`` 变换类似，但使用默认的key/value提取函数对元素分组

        Args:
          **options:  可配置参数

        Returns:
          PTable:  分组结果

        >>> _pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)]).group_by_key().get()
        {"A": [4, 3, 1], "B": [2]}
        """
        return transforms.group_by_key(self, **options)

    def window_into(self, window, **options):
        """
        对元素根据Window分组

        Args:
          window (Window):  用于分组的Window
          **options:  可配置选项

        Returns:
          PTable:  分组结果
        """
        return transforms.window_into(self, window, **options)

    def max(self, key=None, **options):
        """
        取最大元素，等同于
        :func:`bigflow.transforms.max(self, key)
        <bigflow.transforms.max>`

        Args:
          key (function, optional):  用于提取key的函数，与Python内置``max()``中的 ``key`` 参数相同
          **options:  可配置选项

        Returns:
          PObject:  包含最大元素的PObject

        >>> p.parallelize([3, 7, 1, 3, 2, 8]).max().get()
        8
        """
        return transforms.max(self, key)

    def max_elements(self, n, key=None, **options):
        """
        取前n大元素，等同于
        :func:`bigflow.transforms.max_elements(self, n, key)
        <bigflow.transforms.max_elements>`

        Args:
          n (int):  必须大于0
          key (function, optional):  用于提取key的函数，与Python内置``max()``中的 ``key`` 参数相同
          **options:  可配置选项

        Returns:
          PCollection:  包含前n大元素的PCollection，注意对于n=1，这里仍然返回PCollection
          而非PObject

        >>> p.parallelize([3, 7, 1, 3, 2, 8]).max_elements(2).get()
        [8, 7]
        """
        return transforms.max_elements(self, n, key, **options)

    def min(self, key=None, **options):
        """
        取最小元素, 等同于
        :func:`bigflow.transforms.min(self, key)
        <bigflow.transforms.min>`

        Args:
          key (function, optional):  用于提取key的函数，与Python内置``min()``中的 ``key`` 参数相同
          **options:  可配置选项

        Returns:
          PObject:  最小元素

        >>> p.parallelize([3, 7, 1, 3, 2, 8]).min().get()
        1
        """
        return transforms.min(self, key, **options)

    def min_elements(self, n, key=None, **options):
        """
        取前n小元素，等同于
        :func:`bigflow.transforms.min_elements(self, key)
        <bigflow.transforms.min_elements>`

        Args:
          n (int):  必须大于0
          key (function, optional):  用于提取key的函数，与Python内置``min()``中的 ``key`` 参数相同
          **options:  可配置选项

        Returns:
          PCollection:  包含前n小元素的PCollection，注意对于n=1，这里仍然返回PCollection
          而非PObject

        >>> p.parallelize([3, 7, 1, 3, 2, 8]).min_elements(2).get()
        [1, 2]
        """
        return transforms.min_elements(self, n, key, **options)

    def transform(self, *args, **options):
        """
        等同于
        :func:`bigflow.transforms.transform(self, *args, **options)
        <bigflow.transforms.transform>`

        """
        return transforms.transform(self, *args, **options)

    def reduce(self, fn, *side_inputs, **options):
        """
        使用给定的fn将所有元素规约为单个元素，
        等同于
        :func:`bigflow.transforms.reduce(self, fn, *side_inputs, **options)
        <bigflow.transforms.reduce>`,

        Args:
          fn (function):  规约函数
          *side_inputs:  参与运算的SideInputs
          **options:  可配置参数

        Returns:
          PObject:  规约结果

        >>> p.parallelize([1, 2, 3, 4]).reduce(lambda x, y: x + y).get()
        10
        """
        return transforms.reduce(self, fn, *side_inputs, **options)

    def take(self, n, **options):
        """
        给定PCollection中的任意n个元素，等同于 :func:`bigflow.transforms.take`

        Args:
          n (int or PObject):  元素数量
          **options:  可配置参数

        Returns:
          PCollection:  表示结果的PCollection

        >>> _pipeline.parallelize([1, 2, 3, 4]).take(3).get()
        [1, 2, 3]

        >>> _n = _pipeline.parallelize(2)
        >>> _pipeline.parallelize([1, 2, 3, 4]).take(_n).get()
        [1, 2]
        """
        return transforms.take(self, n, **options)

    def first(self, **options):
        """
        取第一个元素

        Returns:
          PObject:  表示结果的PObject

        >>> p.parallelize([3, 7, 1, 3, 2, 8]).first.get()
        3
        """

        return transforms.first(self, **options)

    def sort(self, reverse=False):
        """
        对元素排序，等同于
        :func:`bigflow.transforms.sort(self, reverse)
        <bigflow.transforms.sort>`

        Args:
          reverse (bool):  若True则降序排列，否则为升序排列

        Returns:
          PCollection:  排序结果

        >>> _pipeline.parallelize([3, 1, 2, 8]).sort().get()
        [1, 2, 3, 8]

        """

        return transforms.sort(self, reverse)

    def sort_by(self, key, reverse=False):
        """
        使用给定的key对元素排序，等同于
        :func:`bigflow.transforms.sort_by(self, fn, reverse)
        <bigflow.transforms.sort_by>`

        Args:
          key (function, optional):  用于提取key的函数，与Python内置``sort()``中的 ``key`` 参数相同
          reverse (bool):  若True则降序排列，否则为升序排列

        Returns:
          PCollection:  排序结果

        >>> _pipeline.parallelize([3, 1, 2, 8]).sort_by().get()
        [1, 2, 3, 8]

        """

        return transforms.sort_by(self, key, reverse)

    def sum(self):
        """
        将所有元素相加，等同于
        :func:`bigflow.transforms.sum(self)
        <bigflow.transforms.sum>`

        Returns:
          PObject:  相加结果

        >>> _pipeline.parallelize([3, 1, 2, 8]).sum().get()
        14
        """

        return transforms.sum(self)

    def join(self, other, *others, **options):
        """
        与其他PCollection做连接操作，等同于
        :func:`bigflow.transforms.join(self, other, *others)
        <bigflow.transforms.join>`

        Args:
          other (PCollection):  做连接操作的PCollection
          *others:  更多的PCollection

        Returns:
          PCollection:  连接结果

        >>> x = _pipeline.parallelize([("a", 1), ("b", 4)])
        >>> y = _pipeline.parallelize([("a", 2), ("a", 3)])
        >>> x.join(y).get()
        [("a", (1, 2)), ("a", (1, 3))]
        """
        return transforms.join(self, other, *others, **options)

    def left_join(self, other, *others, **options):
        """
        与其他PCollection做左连接操作，等同于
        :func:`bigflow.transforms.left_join(self, other, *others)
        <bigflow.transforms.left_join>`

        Args:
          other (PCollection):  做连接操作的PCollection
          *others:  更多的PCollection

        Returns:
          PCollection:  左连接结果

        >>> x = _pipeline.parallelize([("a", 1), ("b", 4)])
        >>> y = _pipeline.parallelize([("a", 2)])
        >>> x.left_join(y).get()
        [("a", (1, 2)), ("b", (4, None))]
        """

        return transforms.left_join(self, other, *others, **options)

    def right_join(self, other, *others, **options):
        """
        与其他PCollection做右连接操作，等同于
        :func:`bigflow.transforms.right_join(self, other, *others)
        <bigflow.transforms.right_join>`

        Args:
          other (PCollection):  做连接操作的PCollection
          *others:  更多的PCollection

        Returns:
          PCollection:  右连接结果

        >>> x = _pipeline.parallelize([("a", 1), ("b", 4)])
        >>> y = _pipeline.parallelize([("a", 2), ("a", 3)])
        >>> x.right_join(y).get()
        [("a", (1, 2)), ("a", (1, 3))]
        """

        return transforms.right_join(self, other, *others, **options)

    def full_join(self, other, *others, **options):
        """
        与其他PCollection做全连接操作，等同于
        :func:`bigflow.transforms.full_join(self, other, *others)
        <bigflow.transforms.full_join>`

        Args:
          other (PCollection):  做连接操作的PCollection
          *others:  更多的PCollection

        Returns:
          PCollection:  全连接结果

        >>> x = _pipeline.parallelize([("a", 1)])
        >>> y = _pipeline.parallelize([("b", 2)])
        >>> x.full_join(y).get()
        [("a", (1, None)), ("b", (None, 2))]

        """
        return transforms.full_join(self, other, *others, **options)

    def union(self, other, *others, **options):
        """
        将元素与其他PCollection/PObject中的所有元素共同构成新的PCollection
        等同于
        :func:`bigflow.transforms.union(self, other, *others)
        <bigflow.transforms.union>`

        Args:
          other (PCollection or PObject):  其他PCollection/PObject
          *others:  其他PCollection/PObject

        Returns:
          PCollection:  表示结果的PCollection

        >>> _p1 = _pipeline.parallelize([1, 2, 3, 4])
        >>> _p2 = _pipeline.parallelize([5, 6, 7, 8])
        >>> _p1.union(_p2).get()
        [1, 2, 3, 4, 5, 6, 7, 8]
        """
        return transforms.union(self, other, *others, **options)

    def cartesian(self, other, *others, **options):
        """
        与其他的PCollection做笛卡尔积

        Args:
          other (PCollection):  其他的PCollection
          *others:  更多的PCollection

        Returns:
          PCollection:  表示结果的PCollection

        >>> _p1 = _pipeline.parallelize([1, 2, 3])
        >>> _p2 = _pipeline.parallelize([4, 5])
        >>> _p1.cartesian(_p2).get()
        [(1, 4), (1, 5), (2, 4), (2, 5), (3, 4), (3, 5)]
        """
        return transforms.cartesian(self, other, *others, **options)

    def substract(self, other):
        """
        已废弃，请使用subtract.

        """

        return transforms.substract(self, other)

    def subtract(self, other):
        """
        返回不存在另一个PCollection中的元素，相当于做容器减法

        Args:
          other (PCollection):  作为减数的PCollection

        Returns:
          PCollection:  表示减法结果的PCollection

        >>> a = _pipeline.parallelize([1, 2, 3, 3, 4])
        >>> b = _pipeline.parallelize([1, 2, 5])
        >>> a.subtract(b).get()
        [3, 3, 4]

        """
        return transforms.subtract(self, other)

    def is_empty(self):
        """
        判断此PCollection是否为空

        Returns:
          PObject:  表示结果的PObject

        >>> a = _pipeline.parallelize([1, 2, 3, 4])
        >>> transforms.is_empty(a).get()
        False
        >>> b = _pipeline.parallelize([])
        >>> transforms.is_empty(b).get()
        True

        """
        return transforms.is_empty(self)

    def intersection(self, other, output_duplicated = False):
        """
        返回与另一个PCollection的交集

        Args:
          other (PCollection):  另一个PCollection

        Returns:
          PCollection:  表示交集的PCollection

        >>> a = _pipeline.parallelize([1, 1, 2, 3])
        >>> b = _pipeline.parallelize([1, 1, 2, 2, 5])
        >>> a.intersection(b).get()
        [1, 2]
        >>> a.intersection(b, output_duplicated = True).get()
        [1, 1, 2]

        """
        return transforms.intersection(self, other, output_duplicated = output_duplicated)

    def diff(self, other):
        """
        返回与另一个PCollection中不相同的元素

        Args:
          other (PCollection):  另一个PCollection

        Returns:
          PCollection:  表示结果的PCollection

        >>> a = _pipeline.parallelize([1, 1, 2, 3])
        >>> b = _pipeline.parallelize([1, 1, 2, 2])
        >>> a.diff(b).get()
        [(2, (1, 2)), (3, (1, 0))]

        """
        return transforms.diff(self, other)

    def as_schema(self, fields):
        """
        根据字段，返回一个SchemaPCollection

        Args:
            fields: 类型可以是，tuple，list，dict；
                当fields是tuple或list时, 会判断每个元素的类型：
                    fields中的每个元素是python基本类型或一个serde；
                    接口将构造TupleSerde设置到PCollection每个元素

                    fields中的每个元素是python string，抛出异常

                当fields是dict时：
                    fields的key标识字段类型，value标识该字段的类型，如 {"name": str, "age": int}
                    当前PCollection中的每个元素必须是dict，dict内的key必须相同。
                    fields内的key要和PCollection内的key必须相同
        Returns:
            PCollection: 表示转化后的PCollection

        Examples:
            >>> data = self._pipeline.parallelize([("xiaoming", "PKU", 20)])
            >>> d1 = data.as_schema((str, str, int))
            >>> d2 = data.as_schema([str, str, int])
            >>> print d1.get()
            [('xiaoming', 'PKU', 20)]
            >>>
            >>> print d2.get()
            [('xiaoming', 'PKU', 20)]
            >>>
            >>> data = self._pipeline.parallelize([{"name": "xiaoming", "school": "PKU", "age": 20}])
            >>> d5 = data.as_schema({"name": str, "school": str, "age": int})
            >>> print d5.get()
            [{'age': 20, 'name': 'xiaoming', 'school': 'PKU'}]
            >>>
        """
        from bigflow import schema
        from bigflow import serde
        if isinstance(fields, tuple) or isinstance(fields, list):
            if len(fields) == 0:
                raise ValueError("the number of elems in fields is zero.")
            if isinstance(fields[0], str):
                _fields = {field: self._pipeline.default_objector() for field in fields}
                return self.map(lambda x: x, serde = schema.of(_fields))
            else:
                _fields = tuple(fields)
                return self.map(lambda x: x, serde = serde.of(_fields))
        elif isinstance(fields, dict):
            return self.map(lambda x: x, serde = schema.of(fields))
        else:
            raise ValueError("fields type only accept {`tuple`, `list`, `dict`}.")

    def _parse_cached_data(self, keys_value):
        objector = self.serde()
        result = []

        for kv in keys_value:
            if len(kv.key) > 0:
                raise error.InvalidDataException("Can not call get data in apply_values, \n" +
                    "SideInput is what you want, see this page:\n" +
                    "\thttp://bigflow.baidu.com/doc/guide.html#sideinputs")

            result.append(objector.deserialize(kv.value))

        return result

    def __repr__(self):
        return "[...]"
