#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
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
########################################################################
"""
"""

from bigflow import transforms

class UDF(object):
    """
    User Defined Functions. Wrapper of transforms.
    UDF是对transforms的一个包装，为开发者提供另一个风格的编程接口

    transforms模块提供的变换方法，UDF都支持

    在apply中使用transforms和使用udf是等价的
    p.apply(transform.map, lambda x: x+1)
    p.apply(udf.map(lambda x: x+1))

    group_by后，在apply_values中使用transforms和使用udf是等价的
    p.group_by_key().apply_values(transform.sum)
    p.group_by_key().apply_values(udf.sum())

    Examples:
    >>> from bigflow import transforms
    >>> from bigflow import base
    >>> from bigflow import udf

    >>> pl = base.Pipeline.create("local")

    >>> p = pl.parallelize([1,2,3,4])
    >>> # 使用udf来包装自定义函数
    >>> # 等价于: p1 = p.apply(transform.map, lambda x: x+10)
    >>> p0 = p.apply(transforms.map, lambda x: x + 10)
    >>> p1 = p.apply(udf.map(lambda x: x + 10))
    >>> print p0.get()
    >>> print p1.get()

    >>> p0 = p.apply(transforms.count)
    >>> p1 = p.apply(udf.count())
    >>> print p0.get()
    >>> print p1.get()

    >>> p = pl.parallelize([1,2,3,4])
    >>> d = pl.parallelize(100)
    >>> # 在apply中传递参数
    >>> p2 = p.apply(udf.map(lambda x, incr: x + incr), d)
    >>> print p2.get()

    >>> # group_by之后，在apply_values中使用udf
    >>> # 等价于直接使用transforms
    >>> p = pl.parallelize([(1,1), (1,2), (2,3), (2,4)])
    >>> p4 = p.group_by_key() \
    >>>         .apply_values(udf.sum()) \
    >>>         .flatten()
    >>> p5 = p.group_by_key() \
    >>>         .apply_values(transforms.sum) \
    >>>         .flatten()
    >>> print p4.get()
    >>> print p5.get()
    """
    def __wrap(self, transform):
        def _transform_and_udf(*udf_args):
            return lambda p, *args, **kwargs: \
                transform(p, *(udf_args + args), **kwargs)

        return _transform_and_udf

    def __getattr__(self, func):
        if hasattr(transforms, func):
            return self.__wrap(getattr(transforms, func))
        else:
            raise AttributeError(func)

