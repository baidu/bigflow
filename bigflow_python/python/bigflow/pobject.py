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
:class:`bigflow.pobject.PObject` 定义

.. module:: bigflow.pobject
   :synopsis: pobject

"""

from bigflow import error
from bigflow import transforms
from bigflow import ptype


class PObject(ptype.PType):
    """
    用于表示单个元素的 :class:`bigflow.ptype.PType`，通常为聚合类变换的结果，例如 :func:`bigflow.pcollection.PCollection.combine()`,
    :func:`bigflow.pcollection.PCollection.aggregate()`

    .. note:: 用户不应当直接使用其构造方法
    .. note:: Python不允许重载这3个运算符(and, or, not)，用户不应该在PObject上使用这三个运算符。Bigflow重载的是按位运算(&, |, ^)

    PObject类上重载了以下操作符：

    双目操作符：

        __add__, __sub__, __mul__, __div__,
        __floordiv__, __mod__, __pow__, __lshift__,
        __rshift__, __and__, __xor__, __or__,
        __lt__, __le__, __eq__, __ge__, __gt__, __ne__
        __radd__, __rsub__, __rmul__, __rdiv__,
        __rfloordiv__, __rmod__, __rpow__, __rlshift__,
        __rrshift__, __rand__, __rxor__, __ror__

    单目操作符：

        __neg__, __pos__, __abs__, __invert__

    这些操作都将返回一个把相应数据进行相应变换后的pobject。

    例如：p.sum() / p.count()等价于p.sum().map(lambda s, c: s / c, p.count())

    同时, PObject 禁止了 bool 操作, 调用 bool(PObject) 或者 if PObject 将会抛出异常.
    Args:
      node (Node):  LogicalPlan.Node
    """

    def map(self, fn, *side_inputs, **options):
        """
        对包含的元素进行变换，等同于 ``transforms.map(self, fn, *side_inputs, **options)``

        Args:
          fn (function):  变换函数
          *side_inputs:  参与运算的SideInputs
          **options:  可配置选项

        Returns:
          PObject:  变换结果

        >>> p.parallelize(1).map(lambda x: x + 1).get()
        2
        """
        return transforms.map(self, fn, *side_inputs, **options)

    def flat_map(self, fn, *side_inputs, **option):
        """
        对包含的元素进行一对多变换，等同于 ``transforms.map(self, fn, *side_inputs, **options)``

        Args:
          fn (function):  变换函数
          *side_inputs:  参与运算的SideInputs
          **options:  可配置选项

        Returns:
          PCollection:  变换结果

        >>> _pipeline.parallelize(1).flat_map(lambda x: [x, x * 2]).get()
        [1, 2]
        """
        return transforms.flat_map(self, fn, *side_inputs, **option)

    def as_pcollection(self):
        """
        将PObject转为PCollection

        Returns:
          PCollection:  变换结果
        """
        from bigflow import pcollection
        return pcollection.PCollection(self.node(), self.pipeline())

    def union(self, other, *others, **option):
        """
        将元素与其他PCollection/PObject中的所有元素共同构成PCollection
        等同于 ``transforms.union(self, other, *others)``

        Args:
          other (PCollection or PObject):  其他PCollection/PObject
          *others:  其他PCollection/PObject

        Returns:
          PCollection:  表示结果的PCollection

        >>> _p1 = _pipeline.parallelize(1)
        >>> _p2 = _pipeline.parallelize([2, 3])
        >>> _p1.union(_p2).get()
        [1, 2, 3]
        """
        return transforms.union(self, other, *others, **option)

    def _parse_cached_data(self, keys_value):
        objector = self.serde()
        if len(keys_value) != 1:
            raise error.InvalidDataException("Invalid data: PObject should contain only 1 element" +
              ".\nmay be you should replace your 'reduce' or 'sum' method to 'aggregate'. \n" +
              "See this page:\nhttp://bigflow.baidu.com/doc/rst/bigflow.transforms.html" +
              "#bigflow.transforms.aggregate. \n The plain data is [" + str(keys_value) + "]")

        kv = keys_value[0]
        if len(kv.key) > 0:
            raise error.InvalidDataException("Can not call get data in apply_values, \n" +
                    "SideInput is what you want, see this page:\n" +
                    "\thttp://bigflow.baidu.com/doc/guide.html#sideinputs")
        return objector.deserialize(kv.value)

    def __nonzero__(self):
        """ Boolean operation is not allowed and an exception shall be thrown

        :throws: TypeError
        """
        raise TypeError("PObject is a distributed object, bool(PObject) is not allowed.")

    # prepare for Python3' adoption
    __bool__ = __nonzero__

    def __repr__(self):
        return "o"

    def __init__(self, node, pipeline):
        super(PObject, self).__init__(node, pipeline)
        self._generate_op_handlers()

    def _operator_helper(self, other, op, reversed=False, isSop=False):
        if isSop:
            # single operand operators
            return self.map(lambda x: op(x))
        elif not isinstance(other, ptype.PType):
            if reversed:
                return self.map(lambda x: op(other, x))
            else:
                return self.map(lambda x: op(x, other))
        else:
            return self.map(lambda x, other: op(x, other), other)

    def _generate_op_handlers(self):

        commutative_op = ["__add__", "__sub__", "__mul__", "__div__",\
                "__floordiv__", "__mod__", "__pow__", "__lshift__",\
                "__rshift__", "__and___", "__xor__", "__or___",\
                "__lt__", "__le__", "__eq__", "__ge__",\
                "__gt__", "__ne__"]
        non_commutative_op = ["__radd__", "__rsub__", "__rmul__", "__rdiv__",\
                "__rfloordiv__", "__rmod__", "__rpow__", "__rlshift__",\
                "__rrshift__", "__rand___", "__rxor__", "__ror___"]
        single_op = ["__neg__", "__pos__", "__abs__", "__invert__"]

        import operator
        for cop in commutative_op:
            op = getattr(operator, cop[2:-2])
            fn = lambda self, other, op=op: self._operator_helper(other, op)
            if cop.endswith('___'):
                cop = cop[:-1]
            setattr(self.__class__, cop, fn)
        for ncop in non_commutative_op:
            op = getattr(operator, ncop[3:-2])
            fn = lambda self, other, op=op: self._operator_helper(other, op, reversed=True)
            if ncop.endswith('___'):
                ncop = ncop[:-1]
            setattr(self.__class__, ncop, fn)
        for sop in single_op:
            op = getattr(operator, sop[2:-2])
            fn = lambda self, op=op: self._operator_helper(None, op, isSop=True)
            if sop.endswith('___'):
                sop = sop[:-1]
            setattr(self.__class__, sop, fn)

    def not_(self):
        """
        PObject not算子

        Returns:
            PObject

        >>> _p1 = _pipeline.parallelize(1)
        >>> print _p1.not_().get()
        False
        """
        from operator import not_ as op
        return self._operator_helper(None, op, isSop=True)

    def ceil(self):
        """
        PObject ceil(类似于math.ceil)算子

        Returns:
            PObject

        >>> _p1 = _pipeline.parallelize(1.2)
        >>> print _p1.ceil().get()
        2
        """
        from math import ceil as op
        return self._operator_helper(None, op, isSop=True)

    def round(self, n=0):
        """
        PObject round(类似于math.round)算子
        Args:
            n: 小数点后面保留的位数
        Returns:
            PObject
        >>> _p1 = _pipeline.parallelize(1.2)
        >>> print _p1.round().get()
        1
        """
        op = round
        return self._operator_helper(n, op)

    def floor(self):
        """
        PObject floor(类似于math.floor)算子

        Returns:
            PObject

        >>> _p1 = _pipeline.parallelize(1.2)
        >>> print _p1.floor().get()
        1
        """
        from math import floor as op
        return self._operator_helper(None, op, isSop=True)

    def cartesian(self, *pvalues, **options):
        """
        求当前算子与pvalues的笛卡尔积。
        等价于 :func:`bigflow.transforms.cartesian(self, *pvalues, **options)
        <bigflow.transforms.cartesian>`

        Args: *pvalues (PObject/PCollection)
        Returns:
            PCollection: 此PObject与所有参数的笛卡尔积。结果PCollection中的每条记录是一个tuple。
            每个tuple的第n个元素是第n个输入ptype对象的记录。

        >>> _p1 = _pipeline.parallelize(1)
        >>> _p2 = _pipeline.parallelize(2)
        >>> _p1.cartesian(_p2).get()
        [(1, 2)]
        >>> _p3 = _pipeline.parallelize([3, 4])
        >>> _p1.cartesian(_p3).get()
        [(1, 3), (1, 4)]
        >>> _p1.cartesian(_p2, _p3).get()
        [(1, 2, 3), (1, 2, 4)]

        """
        from bigflow import transforms
        return transforms.cartesian(self, *pvalues, **options)
