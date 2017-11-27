#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""
定义Bigflow Python中所有的变换

Author: Wang, Cong(wangcong09@baidu.com), panyunhong(panyunhong@baidu.com)

**注意：除特殊说明外，所有变换的用户自定义方法(UDF)输入参数都不允许修改**
"""


def accumulate(pcollection, zero, accumulate_fn, *side_inputs, **options):
    """
    将给定的PCollection按照一个初始值和方法聚合为PObject

    假设输入类型为I，输出类型为O，则zero、accumulate_fn的期望签名为:

    zero:  O或zero() => O

    accumulate_fn:  accumulate_fn(O, I) => O  (accumulate_fn的第一个参数允许被修改)

    Args:
      pcollection (PCollection):  输入PCollection
      zero (value or function):  初始值，或是一个返回初始值的方法
      accumulate_fn (function):  聚合方法
      *side_inputs:  参与运算的SideInputs
      **options:  可配置选项

    Returns:
      PObject:  聚合结果

    由于该函数的语义是数据必然按顺序一条条的流过，限制了该函数可以进行的优化工作，
    所以如果可以使用aggregate或reduce替换时，尽量使用aggregate/reduce替换该函数。

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([1, 2, 3])
    >>> transforms.accumulate(_p, 0, lambda x, y: x + y).get()
    6

    TODO: Another example

    """
    import bigflow.transform_impls.accumulate
    return bigflow.transform_impls.accumulate.accumulate(pcollection,
        zero, accumulate_fn, *side_inputs, **options)


def aggregate(pcollection, zero, aggregate_fn, combine_fn, *side_inputs, **options):
    """
    将给定的PCollection按照初始值、初段聚合方法和汇总方法聚合为PObject

    假设输入类型I，输出类型为O，则zero、aggregate_fn、combine_fn的期望签名为:

    zero:  O或zero() => O

    aggregate_fn:  aggregate_fn(O, I) => O  (aggregate_fn的第一个参数允许被修改)

    combine_fn:  combine_fn(O, O) => O  (combine_fn的第一个参数允许被修改)


    在执行时aggregate会把输入pcollection先切分成许多个分片，然后对每个分片使用zero生成一个O类型的初始值。

    随后，在每个分片上，持续调用aggregate_fn，将分片上全部的数据聚合为一个O类型的值。

    最后，会再将所有分片上生成的那些O类型的值汇聚到一起，使用combine_fn最终聚合到一起。

    分片的规则用户不应作任何假设。


    Args:
      pcollection (PCollection):  输入PCollection
      zero (value or callable):  初始值，或是一个返回初始值的方法。
      aggregate_fn (callable):  初段聚合方法。该方法需要两个参数。
      combine_fn (callable):  汇总方法
      *side_inputs:  参与运算的SideInputs
      **options:  可配置选项

    Returns:
      PObject:  聚合结果

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize(["viva", "la", "vida"])
    >>> transforms.aggregate(_p, 0, lambda x, y: x + len(y), lambda x, y: x + y).get()  # sum words length
    10


    """
    import bigflow.transform_impls.aggregate
    return bigflow.transform_impls.aggregate.aggregate(pcollection,
                                                   zero,
                                                   aggregate_fn,
                                                   combine_fn,
                                                   *side_inputs, **options)


def cartesian(*pcollections, **options):
    """
    对多个输入PCollection求笛卡尔积，返回一个PCollection

    Args:
      *pcollections: 输入PCollection
      **options:  可配置选项

    Returns:
      PCollection:  笛卡尔积

    >>> from bigflow import transforms
    >>> p1 = _pipeline.parallelize([1, 2, 3])
    >>> p2 = _pipeline.parallelize(['a', 'b', 'c'])
    >>> transforms.cartesian(p1, p2)
    [(1, 'a'), (1, 'b'), (1, 'c'), (2, 'a'), (2, 'b'), (2, 'c'), (3, 'a'), (3, 'b'), (3, 'c')]

    >>> p3 = _pipeline.parallelize(1)
    >>> p4 = _pipeline.parallelize(2)
    >>> p3.cartesian(p4).get()
    [(1, 2)]
    >>> p5 = _pipeline.parallelize([3, 4])
    >>> p3.cartesian(p5).get()
    [(1, 3), (1, 4)]
    >>> p3.cartesian(p4, p5).get()
    [(1, 2, 3), (1, 2, 4)]

    """
    import bigflow.transform_impls.cartesian

    return bigflow.transform_impls.cartesian.cartesian(*pcollections, **options)


def cogroup(*pcollections, **options):
    """
    对传入的所有pcollection进行协同分组。

    cogroup要求所有传入的PCollection的每个元素都是一个(k, v)对，
    cogroup会用k来作为分组的key，对多个输入PCollection进行协同分组，
    返回一个PTable表示分组结果。

    这个返回的PTable的每个value为一个tuple，tuple的每个元素是一个PCollection，
    其中第n个PCollection表示输入的第n个PCollection在当前key下的全部数据。

    如果某个输入PCollection在某个key下无数据，则对应的PCollection为一个空PCollection。

    目前不能像group_by指定key_extractor。

    group_by_key可以理解成是cogroup只传有一个参数的特殊情况。

    Args:
      *pcollections: 输入PCollection
      **options:  可配置选项

    Returns:
      PTable:  分组结果

    >>> from bigflow import transforms
    >>> _p1 = _pipeline.parallelize([("A", 1), ("A", 2), ("B", 3)])
    >>> _p2 = _pipeline.parallelize([("A", 4)])
    >>> _p = transforms.cogroup(_p1, _p2)
    # _p的值为{"A": ([1, 2], [4]), "B": ([3], [])} ，但由于实现难度较大，PTable的value为tuple of PCollection时的get操作暂不支持。
    >>> _p.apply_values(lambda x, y: transforms.union(x, y)).get()
    {"A": [1, 2, 4], "B": [3]}

    >>> def distinct_and_join(p, q): # 去重，并join
    ...     return p.cogroup(q) \\
    ...             .apply_values(lambda a, b: (a.distinct(), b.distinct())) \\
    ...             .apply_values(transforms.cartesian) \\
    ...             .flatten()
    >>> _p1 = _pipeline.parallelize([("A", 1), ("A", 2), ("A", 1), ("C", 1)])
    >>> _p2 = _pipeline.parallelize([("A", 3), ("A", 3), ("B", 2)])
    >>> print distinct_and_join(_p1, _p2).get()
    [("A", (1, 3)), ("A", (2, 3))]
    >>> # 未来bigflow会自动将p.distinct().join(q.distinct())优化成上边的样子（正在进行中）

    >>> def semi_join(p, q): # 同key的join结果只输出一条
    ...     return p.cogroup(q) \\
    ...             .apply_values(lambda a, b: (a.take(1), b.take(1))) \\
    ...             .apply_values(transforms.cartesian) \\
    ...             .flatten()
    >>> print semi_join(_p1, _p2).get()
    [("A", (1, 3))]
    """
    import bigflow.transform_impls.cogroup
    return bigflow.transform_impls.cogroup.cogroup(*pcollections, **options)


def combine(pcollection, fn, **options):
    """
    给定一个合并函数，聚合输入PCollection中所有元素，这些元素以迭代器的形式给出

    默认情况下，输入类型与输出类型需要一致，假设为O类型，fn的期望签名为 fn([O...]) => O，[]表示输入可遍历

    在执行时会把输入pcollection先切分成许多个分片，然后对每个分片的数据组成一个列表，然后调用fn，
    将每个分片中的数据合并成一个O类型的变量。
    然后，会再将所有分片上生成的那些O类型的值汇聚到一起，组成一个列表，再使用fn最终聚合到一起。

    分片的规则用户不应作任何假设。

    用户可以显式的指定pre_combine=False，关掉预聚合。如果关掉预聚合，则会直接将全部的数据组成一个列表交给fn，
    聚合成一个值。则这种情况下，fn需要的输入类型与fn返回的类型可以是不同类型的。


    Args:
      pcollection (PCollection):  输入PCollection
      fn (function):  合并函数
      **options:  可配置选项
        其中重要配置项为：
        pre_combine(bool): 是否进行预聚合。默认为True。

    Returns:
      PObject:  合并结果

    >>> _p = _pipeline.parallelize([2, 4, 6, 10])
    >>> transforms.combine(_p, sum).get()
    22
    >>> number_list = self._pipeline.parallelize(['2', '3', '4'])
    >>> to_lookup = {'2': 2, '3': 3, '4': 4}
    >>> def lookup_dict_and_sum(numbers):
    ...     return sum(map(lambda s: to_lookup[s], numbers))
    >>> number_list.combine(lookup_dict_and_sum, pre_combine=False).get()
    9
    >>> number_list.combine(lookup_dict_and_sum).get()
    Error may occur (because of pre_combine, 1)
    """

    import bigflow.transform_impls.combine
    return bigflow.transform_impls.combine.combine(pcollection, fn, **options)


def count(pcollection, **options):
    """
    返回给定PCollection中元素的数量

    Args:
      pcollection (PCollection):  输入PCollection
      **options:  可配置选项

    Returns:
      PObject:  元素数量

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([1, 2, 3, 4])
    >>> transforms.count(_p).get()
    4
    """
    import bigflow.transform_impls.count
    return bigflow.transform_impls.count.count(pcollection, **options)


def distinct(pcollection, **options):
    """
    返回给定PCollection中所有不重复元素

    Args:
      pcollection (PCollection):  输入PCollection
      **options:  可配置选项

    Returns:
      PCollection:  不重复元素，以PCollection给出

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([2, 2, 1, 9, 3, 3])
    >>> transforms.distinct(_p).get()
    [2, 3, 1, 9]

    """
    import bigflow.transform_impls.distinct
    return bigflow.transform_impls.distinct.distinct(pcollection, **options)


def extract_keys(ptable, **options):
    """
    提取给定PTable中所有的key

    Args:
      ptable (PTable):  输入PTable
      **options:  可配置选项

    Returns:
      PCollection:  所有的key，以PCollection给出

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize({"A": [2, 3], "B": [4, 5]})
    >>> transforms.extract_keys(_p).get()
    ["A", "B"]

    """
    import bigflow.transform_impls.extract_keys
    return bigflow.transform_impls.extract_keys.extract_keys(ptable, **options)


def extract_values(ptable, **options):
    """
    提取给定PTable中所有的value

    Args:
      ptable (PTable):  输入PTable
      **options:  可配置选项

    Returns:
      PCollection:  所有的value，以PCollection给出

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize({"A": [2, 3], "B": [4, 5]})
    >>> transforms.extract_values(_p).get()
    [2, 3, 4, 5]

    无论PTable为多少层嵌套，都会抽取出最内层的value

    >>> _p = _pipeline.parallelize({"A": {"a": [2, 3], "b": [1, 4]}, "B": {"c": [6, 7], "d": [9, 9]}}
    >>> print _p
    >>> {k0: {k1: [...]}}  # 嵌套PTable
    >>> transforms.extract_values(_p).get()
    >>> [2, 3, 1, 4, 6, 7, 9, 9]

    """
    import bigflow.transform_impls.extract_values
    return bigflow.transform_impls.extract_values.extract_values(ptable, **options)


def flatten(ptable, **options):
    """
    对于给定PTable中的key和value中每一个元素，构造(key, value)对，结果保存在PCollection中

    Args:
      ptable (PTable):  输入PTable
      **options:  可配置选项

    Returns:
      PCollection:  (key, value)对，结果以PCollection表示

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize({"A": [2, 3], "B": [4, 5]})
    >>> transforms.flatten(_p).get()
    [("A", 2), ("A", 3), ("B", 4), ("B", 5)]

    """
    import bigflow.transform_impls.flatten
    return bigflow.transform_impls.flatten.flatten(ptable, **options)


def flatten_values(ptable, **options):
    """
    等价于 ``extract_values(ptable)``

    Args:
      ptable (PTable):  输入PTable
      **options:  可配置选项

    Returns:
      PCollection:  所有的value，以PCollection给出

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize({"A": [2, 3], "B": [4, 5]})
    >>> transforms.flatten_values(_p).get()
    [2, 3, 4, 5]

    """
    import bigflow.transform_impls.flatten_values
    return bigflow.transform_impls.flatten_values.flatten_values(ptable, **options)


def filter(pcollection, fn, *side_inputs, **options):
    """
    对于给定的PCollection和一个断言函数，返回只满足断言函数元素的PCollection

    假设输入类型为I，fn的期望签名为 fn(I) => bool

    Args:
      pcollection (PCollection):  输入PCollection
      fn (function):  断言函数
      **options:  可配置选项

    Returns:
      PCollection:  过滤结果

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([3, 7, 1, 3, 2, 8])
    >>> transforms.filter(_p, lambda x: x % 2 == 0).get()
    [2, 8]

    """
    import bigflow.transform_impls.filter
    return bigflow.transform_impls.filter.filter(pcollection, fn, *side_inputs, **options)


def first(pcollection, **options):
    """
    取出PCollection中的第一个元素

    Args:
      pcollection (PCollection):  输入PCollection
      **options:  可配置选项

    Returns:
      PObject:  取出的单个元素，以PObject给出

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([3, 7, 1, 3, 2, 8])
    >>> transforms.first(_p).get()
    3

    """
    import bigflow.transform_impls.first
    return bigflow.transform_impls.first.first(pcollection, **options)


def group_by(pcollection, key_extractor, value_extractor=None, **options):
    """
    利用给定的key_extractor和value_extractor对输入PCollection分组，返回一个表示分组结果的PTable

    Args:
      pcollection (PCollection):  输入PCollection
      key_extractor (function):  用于提取key的函数
      value_extractor (function, optional):  用于提取value的函数
      **options:  可配置选项

    Returns:
      PTable:  分组结果

    >>> _p = _pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
    >>> transforms.group_by(_p, lambda x: x[0], lambda x: x[1]).get()
    {"A": [4, 3, 1], "B": [2]}

    """

    import bigflow.transform_impls.group_by
    return bigflow.transform_impls.group_by.group_by(pcollection,
        key_extractor, value_extractor, **options)


def group_by_key(pcollection, **options):
    """
    利用给定的PCollection，使用一个默认的key/value提取函数对输入的PCollection分组
    ，返回一个表示分组的PTable

    Args:
      pcollection (PCollection):  输入PCollection
      **options:  可配置选项

    Returns:
      PTable:  分组结果

    >>> _p = _pipeline.parallelize([("A", 4), ("A", 3), ("B", 2), ("A", 1)])
    >>> transforms.group_by_key(_p).get()
    {"A": [4, 3, 1], "B": [2]}

    """
    import bigflow.transform_impls.functions
    from bigflow import serde

    pipeline = pcollection.pipeline()
    key_serde = options.get('key_serde', None)
    value_serde = options.get('value_serde', None)

    if key_serde is None:
        key_serde = serde._key_serde(pcollection.serde(), pipeline.default_objector())
    if value_serde is None:
        value_serde = serde._value_serde(pcollection.serde(), pipeline.default_objector())

    options['key_serde'] = key_serde
    options['value_serde'] = value_serde

    # if key_extractor is None, will use the default key extractor
    return group_by(pcollection,
                    key_extractor=None,
                    value_extractor=bigflow.core.entity.ExtractValueFn(),
                    **options
                    )


def window_into(pcollection, window, **options):
    """
    利用给定的window对输入PCollection分组，返回一个表示分组结果的PTable

    Args:
      pcollection (PCollection):  输入PCollection
      window(Window):  输入Window

    Returns:
      PTable:  分组结果

    """
    import bigflow.transform_impls.window_into
    return bigflow.transform_impls.window_into.window_into(pcollection, window, **options)



def foreach(pvalue, fn, *side_inputs, **options):
    """
    对给定的PCollection/PObject中的每个元素应用一个函数，函数并不期望有任何的
    返回，而是利用其副作用产生效果。
    该函数一般用于产出数据到外部存储，同一条数据可能会被多次调用，
    用户需要注意在下游去重，或想办法保证foreach操作具有幂等性质。

    假设输入类型为I，fn的期望签名为 fn(I) => object，即返回值类型任意(并被忽略)

    Args:
      pvalue (PCollection or PObject):  输入P类型
      fn (function):  变换函数
      *side_inputs:  参与运算的SideInputs
      **options:  可配置选项

    Results:
      None

    >>> from bigflow import lazy_var
    >>> r = lazy_var.declare(lambda: redis.Redis(host='x.x.x.x', port=x, db=0))
    >>> x = _pipeline.parallelize([("a", "1"), ("b", "4")])
    >>> x.foreach(lambda (k, v): r.get().set(k, v))
    >>> _pipeline.run() # all the data will be written into redis

    """
    import bigflow.transform_impls.foreach
    return bigflow.transform_impls.foreach.foreach(pvalue, fn, *side_inputs, **options)


def join(*pcollections, **options):
    """
    对于多个输入PCollection，根据key对PCollection做内连接操作
    ，连接结果为(key, (value1, value2, ..., valuen))

    Args:
      *pcollections:  输入PCollection
      **options:  可配置选项

    Returns:
      PCollection:  连接结果

    >>> x = _pipeline.parallelize([("a", 1), ("b", 4)])
    >>> y = _pipeline.parallelize([("a", 2), ("a", 3)])
    >>> transforms.join(x, y).get()
    [("a", (1, 2)), ("a", (1, 3))]

    """

    import bigflow.transform_impls.join
    return bigflow.transform_impls.join.join(*pcollections, **options)


def left_join(*pcollections, **options):
    """
    对于多个输入PCollection，根据key对PCollection做左连接操作
    ，连接结果为(key, (value 1, value 2, ..., value n))，若第m个PCollection没有元素，
    则value m为None

    Args:
      *pcollections:  输入PCollection
      **options:  可配置选项

    Returns:
      PCollection:  连接结果

    >>> x = _pipeline.parallelize([("a", 1), ("b", 4)])
    >>> y = _pipeline.parallelize([("a", 2)])
    >>> transforms.left_join(x, y).get()
    [("a", (1, 2)), ("b", (4, None))]
    """
    import bigflow.transform_impls.join
    return bigflow.transform_impls.join.left_join(*pcollections, **options)


def right_join(*pcollections, **options):
    """
    对于多个输入PCollection，根据key对PCollection做右连接操作
    ，连接结果为(key, (value 1, value 2, ..., value n))，若第m个PCollection没有元素，
    则value m为None

    Args:
      *pcollections:  输入PCollection
      **options:  可配置选项

    Returns:
      PCollection:  连接结果

    >>> x = _pipeline.parallelize([("a", 1), ("b", 4)])
    >>> y = _pipeline.parallelize([("a", 2), ("a", 3)])
    >>> transforms.right_join(x, y).get()
    [("a", (1, 2)), ("a", (1, 3))]
    """
    import bigflow.transform_impls.join
    return bigflow.transform_impls.join.right_join(*pcollections, **options)


def full_join(*pcollections, **options):
    """
    对于多个输入PCollection，根据key对PCollection做全连接操作
    ，连接结果为(key, (value 1, value 2, ..., value n))，若第m个PCollection没有元素，
    则value m为None

    Args:
      *pcollections:  输入PCollection
      **options:  可配置选项

    Returns:
      PCollection:  连接结果

    >>> x = _pipeline.parallelize([("a", 1)])
    >>> y = _pipeline.parallelize([("b", 2)])
    >>> transforms.full_join(x, y).get()
    [("a", (1, None)), ("b", (None, 2))]

    """
    import bigflow.transform_impls.join
    return bigflow.transform_impls.join.full_join(*pcollections, **options)


def map(pvalue, fn, *side_inputs, **options):
    """
    对PCollection中的每个元素做一对一映射

    对给定的PCollection/PObject中的每个元素应用一个变换函数，以函数的返回结果
    构造PCollection/PObject

    假设输入类型为I，fn的期望签名为 fn(I) => O

    Args:
      pvalue (PCollection or PObject):  输入P类型
      fn (function):  变换函数
      *side_inputs:  参与运算的SideInputs
      **options:  可配置选项

    Results:
      PType:  变换后的PCollection/PObject，与输入类型一致

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([1, 3, 5, 7])
    >>> transforms.map(_p, lambda x: x + 1).get()
    [2, 4, 6, 8]
    >>> transforms.map(_p, lambda x: [x, x * 2]).get()
    [[1, 2], [3, 6], [5, 10], [7, 14]]

    """
    import bigflow.transform_impls.map
    return bigflow.transform_impls.map.map(pvalue, fn, *side_inputs, **options)


def flat_map(pvalue, fn, *side_inputs, **options):
    """
    对PCollection中的每个元素做一对N映射

    对变换函数必须返回一个可遍历变量(即实现了__iter__()方法)，将迭代器中的所有元素
    构造PCollection

    假设输入类型为I，fn的期望签名为 fn(I) => [O...]，[]表示返回结果可遍历

    Args:
      pvalue (PCollection or PObject):  输入P类型
      fn (function):  变换函数
      *side_inputs:  参与运算的SideInputs
      **options:  可配置选项

    Results:
      PCollection:  变换后的PCollection

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([1, 3, 5, 7])
    >>> transforms.flat_map(_p, lambda x: [x, x * 2]).get()
    [1, 2, 3, 5, 6, 7, 10, 14]
    >>> transforms.flat_map(_p, lambda x: [[x, x * 2]]).get()
    [[1, 2], [3, 6], [5, 10], [7, 14]]
    >>> transforms.flat_map(_p, lambda x: (x, x * 2)).get()
    [1, 2, 3, 6, 5, 10, 7, 14]
    >>> transforms.flat_map(_p, lambda x: [(x, x * 2)]).get()
    [(1, 2), (3, 6), (5, 10), (7, 14)]

    注意返回结果可以为空:

    >>> transforms.flat_map(_p, lambda x: [])
    []

    如果返回的对象不能被遍历，则运行时会报错。
    典型的错误用法包括None或返回一个单个元素。

    返回对象只要是可迭代类型即可，不必一定是list。
    特别是，需要输出较多数据时，
    使用list可能会导致内存占用过大，
    用户可以直接利用python的yield语法生成一个generator，
    达到不需要占用大量内存的目的。

    >>> _p = _pipeline.parallelize([3, 5])
    >>> def make_partial_sum(x):
    ...     sum = 0
    ...     for i in xrange(1, x + 1):
    ...         sum += i
    ...         yield sum

    >>> transforms.flat_map(_p, make_partial_sum)
    [1, 3, 6, 1, 3, 6, 10, 15]

    这种用法可以避免产生大list，从而避免内存占用过大的问题。

    """
    import bigflow.transform_impls.flat_map
    return bigflow.transform_impls.flat_map.flat_map(pvalue, fn, *side_inputs, **options)


def max(pcollection, key=None, **options):
    """
    得到输入PCollection中最大的元素

    Args:
      pcollection (PCollection):  输入PCollection
      key (function, optional):  用于提取key的函数，与Python内置``max()``中的 ``key`` 参数相同
      **options:  可配置选项

    Returns:
      PObject:  包含最大元素的PObject

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([3, 7, 1, 3, 2, 8])
    >>> transforms.max(_p).get()
    8
    >>> transforms.max(_p, lambda val: -val).get()
    1

    """
    import bigflow.transform_impls.max
    return bigflow.transform_impls.max.max(pcollection, key, **options)


def max_elements(pcollection, n, key=None, **options):
    """
    得到输入PCollection中前n大的元素

    Args:
      pcollection (PCollection):  输入PCollection
      n (int/PObject):  必须大于0
      key (function, optional):  用于提取key的函数，与Python内置``max()``中的 ``key`` 参数相同
      **options:  可配置选项

    Returns:
      PCollection:  包含前n大元素的PCollection，注意对于n=1，这里仍然返回PCollection
      而非PObject

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([3, 7, 1, 3, 2, 8])
    >>> transforms.max_elements(_p, 2).get()
    [8, 7]
    >>> n = _p.count().map(lambda x: x - 1)   # n is PObject(5)
    >>> transforms.max_elements(_p, n, lambda val: -val).get()  # key is -val
    >>> [1, 3, 3, 2, 7]

    """
    import bigflow.transform_impls.max_elements
    return bigflow.transform_impls.max_elements.max_elements(pcollection, n, key, **options)


def min(pcollection, key=None, **options):
    """
    得到输入PCollection中最小的元素

    Args:
      pcollection (PCollection):  输入PCollection
      key (function, optional):  用于提取key的函数，与Python内置``min()``中的 ``key`` 参数相同
      **options:  可配置选项

    Returns:
      PObject:  包含最小元素的PObject

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([3, 7, 1, 3, 2, 8])
    >>> transforms.min(_p).get()
    1
    >>> transforms.min(_p, lambda val: -val).get()
    8


    """
    import bigflow.transform_impls.min
    return bigflow.transform_impls.min.min(pcollection, key, **options)


def min_elements(pcollection, n, key=None, **options):
    """
    得到输入PCollection中前n小的元素

    Args:
      pcollection (PCollection):  输入PCollection
      n (int/PObject):  必须大于0
      key (function, optional):  用于提取key的函数，与Python内置``max()``中的 ``key`` 参数相同
      **options:  可配置选项

    Returns:
      PCollection:  包含前n小元素的PCollection，注意对于n=1，这里仍然返回PCollection
      而非PObject

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([3, 7, 1, 3, 2, 8])
    >>> transforms.min_elements(_p, 2).get()
    [1, 2]
    >>> n = _p.count().map(lambda x: x - 1)   # n is PObject(5)
    >>> transforms.min_elements(_p, n, lambda val: -val).get()  # key is -val
    >>> [8, 3, 3, 2, 7]

    """
    import bigflow.transform_impls.min_elements
    return bigflow.transform_impls.min_elements.min_elements(pcollection, n, key, **options)


def reduce(pcollection, fn, *side_inputs, **options):
    """
    对于属于PCollection，使用给定的fn将所有元素规约为单个元素

    假设输入类型为I，fn的期望签名为 fn(I1, I2) => I，即输出的类型必须与输入相同  (fn的第一个参数允许被修改)

    Args:
      pcollection (PCollection):  输入PCollection
      fn (function):  规约函数
      *side_inputs:  参与运算的SideInputs
      **options:  可配置参数

    Returns:
      PObject:  规约结果

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([1, 2, 3, 4])
    >>> transforms.reduce(_p, lambda x, y: x + y).get()
    10

    """
    import bigflow.transform_impls.reduce
    return bigflow.transform_impls.reduce.reduce(pcollection, fn, *side_inputs, **options)


def sort(pcollection, reverse=False):
    """
    对于输入PCollection，将其进行排序

    Args:
      pcollection (PCollection):  输入PCollection
      reverse (bool):  若True则降序排列，否则为升序排列

    Returns:
      PCollection:  排序结果

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([3, 1, 2, 8])
    >>> transforms.sort(_p).get()
    [1, 2, 3, 8]

    """
    import bigflow.transform_impls.sort
    return bigflow.transform_impls.sort.sort(pcollection, reverse)


def sort_by(pcollection, key, reverse=False):
    """
    对于输入PCollection，使用给定的key将其进行排序

    Args:
      pcollection (PCollection):  输入PCollection
      key (function, optional):  用于提取key的函数，与Python内置``sort()``中的 ``key`` 参数相同。提取的key不能为None。可以返回一个key的列表，每个key都可以分别按照升序或者降序排
      reverse (bool):  若True则降序排列，否则为升序排列

    Returns:
      PCollection:  排序结果

    >>> from bigflow import transforms
    >>> from bigflow.transform_impls import sort
    >>> _p = _pipeline.parallelize([3, 1, 2, 8])
    >>> p2 = _pipeline.parallelize([(1, 2), (3, 4), (3, 5), (2, 6), (2, 4)]
    >>> transforms.sort_by(_p).get()
    >>> transforms.sort_by(p2, lambda rec:[sort.ASC(rec[0]), sort.DESC(rec[1])]).get()
    [1, 2, 3, 8]
    [(1, 2), (2, 6), (2, 4), (3, 5), (3, 4)]

    Note:
      1. sort时所有元素类型必须相同，否则可能出现结果不正确。例如，元素1与元素2.0可能排序结果不正确。
         但作为排序的key的多列可以类型不同。

      2. sort/sort_by后的数据集只有以下操作可以保证顺序：

         * accumulate,transform操作

         * aggregate的第一个聚合函数(即签名为O+I=>O的那个函数)。

         * first 和 take (暂不保证语义，未来会支持)

         由于操作性质，其它操作保序也无意义（或语义不明），故不保证顺序。

      3. sort后调用write并不保证顺序。如果想保证输出有序，可以参考此文档：
      http://bigflow.baidu.com/doc/rst/bigflow.output.html

    """
    import bigflow.transform_impls.sort
    return bigflow.transform_impls.sort.sort_by(pcollection, key, reverse)


def subtract(pcollection1, pcollection2):
    """
    对于给定的PCollection1和PCollection2，返回所有存在于PCollection1但不在PCollection2
    中的元素，相当于做容器减法

    Args:
      pcollection1 (PCollection):  作为被减数的PCollection
      pcollection2 (PCollection):  作为减数的PCollection

    Returns:
      PCollection:  表示结果的PCollection

    >>> from bigflow import transforms
    >>> a = _pipeline.parallelize([1, 2, 3, 4])
    >>> b = _pipeline.parallelize([1, 2, 5])
    >>> transforms.subtract(a, b).get()
    [3, 4]

    """
    import bigflow.transform_impls.subtract
    return bigflow.transform_impls.subtract.subtract(pcollection1, pcollection2)


def substract(pcollection1, pcollection2):
    """
    此接口已废弃。请使用subtract。
    """
    import bigflow.transform_impls.subtract
    return bigflow.transform_impls.subtract.subtract(pcollection1, pcollection2)


def sum(pcollection, **options):
    """
    对于输入PCollection，求其所有包含元素相加的结果

    Args:
      pcollection (PCollection):  输入PCollection
      **options:  可配置参数

    Returns:
      PObject:  表示结果的PObject

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([1, 2, 3, 4])
    >>> transforms.sum(_p).get()
    10

    """
    import bigflow.transform_impls.sum
    return bigflow.transform_impls.sum.sum(pcollection, **options)


def take(pcollection, n, **options):
    """
    取给定PCollection中的任意n个元素。
    （如果总元素数量不足n，则返回输入pcollection）

    Args:
      pcollection (PCollection):  输入PCollection
      n (int or PObject):  元素数量
      **options:  可配置参数

    Returns:
      PCollection:  表示结果的PCollection

    >>> from bigflow import transforms
    >>> _p = _pipeline.parallelize([1, 2, 3, 4])
    >>> transforms.take(_p, 3).get()
    [1, 2, 3]
    >>> _n = _pipeline.parallelize(2)
    >>> transforms.take(_p, _n).get()
    [1, 2]

    >>> _n = _pipeline.parallelize([1, 2, 3, 4])
    >>> transforms.take(_p, 10).get()
    [1, 2, 3, 4]

    """
    import bigflow.transform_impls.take
    return bigflow.transform_impls.take.take(pcollection, n, **options)


def transform(pcollection, first_arg, *other_args, **options):
    """

    对给定PCollection进行任意的变换，结果为另一个PCollection

    transform有两种形式，形式一：

    基本原型为`transform(pcollection, initializer, transformer, finalizer, *side_inputs, **options)`

    transform将PCollection的处理分为3个阶段: 初始化，遍历及结束，分别对应于
    initializer, transformer和finalizer三个处理函数。三个函数之间有一个状态
    status(也可以理解为上下文context)，同时有一个emitter参数可以向输出PCollection发送数据

    假定输入数据类型为I，输出数据类型为O，initializer, transformer, finalizer各自的期望签名为:

    initializer(emitter, *side_inputs) => status(object)

    transformer(status, emitter, I, *side_inputs) => status(object)
    (transformer的第一个参数允许被修改)

    finalizer(status, emitter, *side_inputs) => None  (finalizer的第一个参数允许被修改)

    emitter.emit(O)

    Args:
      pcollection (PCollection):  输入PCollection
      initializer (callable):  初始化函数
      transformer (callable):  变换函数
      finalizer (callable):  结束函数
      *side_inputs:  参与计算的SideInputs
      **options:  可配置选项

    Returns:
      PCollection:  表示返回结果的PCollection


    >>> from bigflow import transforms
    >>> import copy
    >>> def initializer(emitter):
    >>>     return []
    >>>
    >>> def transformer(status, emitter, inp):
    >>>     status.append(copy.deepcopy(inp)) #如果要缓存一个数据，最好复制一份。
    >>>     return status
    >>>
    >>> def finalizer(status, emitter):
    >>>     emitter.emit(status)
    >>>
    >>> _p = _pipeline.parallelize([1, 2, 3])
    >>> _plist = transforms.transform(_p, initializer, transformer, finalizer)
    >>> print _plist.count().get() # 只有一个元素，元素的内容是[1, 2, 3]这样一个列表。
    1
    >>> print _plist.get()
    [[1, 2, 3]]

    形式二：

    基本原型为`transform(pcollection, transformer, *side_inputs, **options)`
    其中transformer应为
    :class:`bigflow.base.Transformer <bigflow.base.Transformer>`
    类的子类，
    Transformer.begin_process在数据开始处理前会被调用。
    Transformer.process在数据开始处理时，每条数据调用一次，传入需要的数据。
    Transformer.end_process在数据处理完成后被调用。
    用户需要输出的数据以列表或其它可迭代对象的形式返回，其中所有元素都会被作为输出PCollection中的一个元素。
    （注意，如果不需要输出请返回一个空的[]，而不要返回None）

    >>> class SumTransformer(base.Transformer):
    ...
    ... def begin_process(self):
    ...     self._sum = 0
    ...     return []
    ...
    ... def process(self, record):
    ...     self._sum += record
    ...     return []
    ...
    ... def end_process(self):
    ...     yield self._sum
    ...
    >>> p1 = _pipeline.parallelize([1, 2, 3])
    >>> transforms.transform(p1, SumTransformer).get()
    6

    >>> class PartialSumTransformer(base.Transformer):
    ...
    ...     def begin_process(self):
    ...         self._sum = 0
    ...         return []
    ...
    ...     def process(self, record):
    ...         self._sum += record
    ...         yield self._sum
    ...
    >>> transforms.transform(p1, PartialSumTransformer()),get()
    [1, 3, 6]

    >>> class ZipTransformer(base.Transformer):
    ...
    ...     def begin_process(self, *si):
    ...         self.index = 0
    ...         lens = map(len, si)
    ...         self.min_len = min(lens)
    ...         return []
    ...
    ...     def process(self, inp, *si):
    ...         if self.index < self.min_len:
    ...             yield (inp, ) + tuple(map(lambda x: x[self.index], si))
    ...         self.index += 1
    ...
    >>> p2 = _pipeline.parallelize([4, 5]).sort()
    >>> transforms.transform(p1, ZipTransformer(), p2).get()
    [(1, 4), (2, 5)]

    本方法为Bigflow所提供的最底层和最复杂的变换方法，它可以表达对PCollection
    的任意变换。

    在有其它函数(如aggregate)能完成同样功能时，尽量不要使用该函数，框架无法了解该函数内部实现，
    无法进行许多深层次的优化工作。

    """
    import bigflow.transform_impls.transform
    return bigflow.transform_impls.transform.transform(pcollection, first_arg, *other_args,
                                                       **options)


def union(*pvalues, **options):
    """
    对于多个输入PCollection/PObject，返回包含它们所有元素的PCollection

    输入PCollection必须为同类型

    Args:
      *pvalues:  输入PCollection/PObject

    Returns:
      PCollection:  表示结果的PCollection

    >>> from bigflow import transforms
    >>> _p1 = _pipeline.parallelize([1, 2, 3, 4])
    >>> _p2 = _pipeline.parallelize([5, 6, 7, 8])
    >>> transforms.union(_p1, _p2).get()
    [1, 2, 3, 4, 5, 6, 7, 8]

    """
    import bigflow.transform_impls.union
    return bigflow.transform_impls.union.union(*pvalues, **options)


def is_empty(pcollection):
    """
    对于输入PCollection，返回其是否为空

    Args:
      pcollection (PCollection):  输入PCollection

    Returns:
      PObject:  表示返回结果的PObject

    >>> from bigflow import transforms
    >>> a = _pipeline.parallelize([1, 2, 3, 4])
    >>> transforms.is_empty(a).get()
    False
    >>> b = _pipeline.parallelize([])
    >>> transforms.is_empty(b).get()
    True

    """
    import bigflow.transform_impls.is_empty
    return bigflow.transform_impls.is_empty.is_empty(pcollection)


def intersection(pcollection1, pcollection2, output_duplicated = False):
    """
    对于给定的PCollection1和PCollection2，返回所有同时存在于PCollection1和PCollection2
    中的元素，即取两者交集

    Args:
      pcollection1 (PCollection):  输入1
      pcollection2 (PCollection):  输入2

    Returns:
      PCollection:  相交结果

    >>> from bigflow import transforms
    >>> a = _pipeline.parallelize([1, 1, 2, 3])
    >>> b = _pipeline.parallelize([1, 1, 2, 2, 5])
    >>> transforms.intersection(a, b).get()
    [1, 2]
    >>> transforms.intersection(a, b, output_duplicated = True).get()
    [1, 1, 2]

    """
    import bigflow.transform_impls.intersection
    return bigflow.transform_impls.intersection.intersection(
        pcollection1,
        pcollection2,
        output_duplicated = output_duplicated
    )


def diff(pcollection1, pcollection2):
    """
    对于给定的PCollection1和PCollection2，返回两者不相同的元素

    Args:
      pcollection1 (PCollection):  输入1
      pcollection2 (PCollection):  输入2

    Returns:
      PCollection:  表示差异的PCollection

    >>> from bigflow import transforms
    >>> a = _pipeline.parallelize([1, 1, 2, 3])
    >>> b = _pipeline.parallelize([1, 1, 2, 2])
    >>> transforms.diff(a, b).get()
    [(2, (1, 2)), (3, (1, 0))]

    """
    import bigflow.transform_impls.diff
    return bigflow.transform_impls.diff.diff(pcollection1, pcollection2)


def to_list_pobject(pvalue, **options):
    """
    对于给定的PCollection，聚合为PObject，PObject的内容为list

    Args:
      pvalue (PCollection): 输入
      **options:  可配置选项

    Returns:
      PObject: 聚合后的list

    >>> from bigflow import transforms
    >>> a = _pipeline.parallelize([1, 1, 2, 3])
    >>> transforms.to_list_pobject(a).get()
    [1, 1, 2, 3]

    >>> type(a)
    bigflow.pcollection.PCollection

    >>> a.map(lambda x: str(type(x))).get()
    ["<type 'int'>", "<type 'int'>", "<type 'int'>", "<type 'int'>"]

    >>> b = transforms.to_list_pobject(a)
    >>> type(b)
    bigflow.pobject.PObject

    >>> b.map(lambda x: str(type(x))).get()
    "<type 'list'>"

    Note:

        这个是最易被滥用的一个transform。
        它可以使一个PCollection转化为一个元素为list的PObject，
        用户可以在后续的map操作中拿到这个list进行任意的单机操作，
        在一些场景下，如复用现有单机代码时，比较有用。
        但是，该变换将使得Bigflow许多优化无法执行，导致运行效率下降，
        另外，在apply_values中使用时，
        由于每个分组的数据必须转化为一个list，
        则导致在一个分组内数据过多时，会占用大量内存资源，
        甚至可能引起作业因内存占用过多而Out-Of-Memory失败。

        故，使用该变换前，请三思，尽量使用以下其它算子替换掉此方法，
        一些较为通用的替代方案如下（下列替换方案，按顺序前边的比后边的效率高）：

        1. :func:`bigflow.transforms.aggregate <bigflow.transforms.aggregate>`
        2. :func:`bigflow.transforms.transform <bigflow.transforms.transform>`


    """
    import bigflow.transform_impls.to_list_pobject
    return bigflow.transform_impls.to_list_pobject.to_list_pobject(pvalue, **options)


def pipe(pvalue, command, **options):
    """
    对于给定的PCollection/PTable，返回通过command处理后的PCollection

    Args:
      pvalue (PCollection/PTable): 输入
      command: 命令行
      **options: 可配置选项

        type: pipe类型，目前支持streaming和bistreaming，默认为streaming

        buffer_size: 缓存大小（单条数据），默认64MB

        input_fields_num: 输入command的一条数据有几个field，默认为1。
        PTable上调用pipe不需要指定；如果PCollection上调用pipe需要输入多个field，则要指定改配置，并且PCollection的元素类型需为tuple

        output_fields_num: command输出的数据有几个field，默认为1

        field_delimiter: streaming模式下field的分割符，默认为tab（制表符）

    Returns:
      PCollection: 处理后的PCollection

    Note:

      1. pipe作用于PCollection上，pipe会将数据直接发送到管道中，框架对数据如何划分不做任何保证；

      2. pipe作用于PTable上，pipe会将PTable的Key和数据一起发送到管道中（支持嵌套），
      并保证相同Key的数据会顺序发送到管道中，例如下列代码：

        >>> from bigflow import transforms
        >>> p = _pipeline.parallelize({
        >>>         'key1_a': {
        >>>             'key2_a': ['value1', 'value2'],
        >>>             'key2_b': ['value3', 'value4']
        >>>         },
        >>>         'key2_b': {
        >>>             'key2_c': ['value5', 'value6']
        >>>         }
        >>>     })
        >>> transforms.pipe(p, 'cat').get()

         用户程序（cat）接收到的数据为，column间默认使用制表符（tab）作为分割符：

            key1_a\tkey2_a\tvalue1

            key1_a\tkey2_a\tvalue2

            key1_a\tkey2_b\tvalue3

            key1_a\tkey2_b\tvalue4

            key1_b\tkey2_c\tvalue5

            key1_b\tkey2_c\tvalue6

      3. 尽量不要在PTable上通过apply_values中使用pipe（应该使用apply），
      不仅性能极差而且发送给管道的数据不包含Key；

    >>> from bigflow import transforms
    >>> p = _pipeline.parallelize([1, 1, 2, 3])
    >>> transforms.pipe(p, 'cat').get()
    ['1', '1', '2', '3']

    >>> from bigflow import transforms
    >>> p = _pipeline.parallelize({'A': [1, 2], 'B': [2, 3]})
    >>> transforms.pipe(p, 'cat').get()
    ['A\t1', 'A\t2', 'B\t2', 'B\t3']

    >>> from bigflow import transforms
    >>> p = _pipeline.parallelize([(1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')])
    >>> transforms.pipe(p, 'cat', type='bistreaming', input_fields_num=2, output_fields_num=2).get()
    [('1', 'a'), ('1', 'a'), ('2', 'b'), ('3', 'c')]
    """
    import bigflow.transform_impls.pipe
    return bigflow.transform_impls.pipe.pipe(pvalue, command, **options)


def idl_to_str(pcollection, **options):
    """
    对于给定的PCollection，对每条数据执行idl解包。并过滤掉idl packet类型为Heartbeat和EOF的数据。

    Args:
      pcollection (PCollection): 输入
      **options:  可配置选项

        log_type: idl数据类型，目前支持log_text和log_bin，默认为log_text

    Returns:
      PCollection: 处理后的PCollection
    """
    from bigflow import serde
    from bigflow.core import entity
    log_type = options.get("log_type", "log_text")
    idl_serde = serde.IdlPacketSerde(log_type=log_type)
    deserialize = entity.SerdeWrapper(idl_serde, is_serialize=False)
    return pcollection.map(deserialize, serde=serde.StrSerde()) \
            .filter(lambda x: x is not None)


def str_to_idl(pcollection, **options):
    """
    对于给定的PCollection，对每条数据执行idl打包。要求输入的数据类型为str。

    Args:
      pcollection (PCollection): 输入
      **options:  可配置选项

        log_type: idl数据类型，目前支持log_text和log_bin，默认为log_text

    Returns:
      PCollection: 处理后的PCollection
    """
    from bigflow import serde
    from bigflow.core import entity
    log_type = options.get("log_type", "log_text")
    idl_serde = serde.IdlPacketSerde(log_type=log_type)
    serialize = entity.SerdeWrapper(idl_serde, is_serialize=True)
    return pcollection.map(serialize, serde=serde.StrSerde())

def to_pobject(pvalue, **options):
    """
    对于给定的PCollection/PTable，聚合为PObject，PObject的内容为list/dict

    Args:
        pvalue (PCollection/PTable): 输入
        **options:  可配置选项

    Returns:
        PObject: 聚合后的list/dict

    >>> from bigflow import transforms
    >>> a = _pipeline.parallelize([1, 1, 2, 3])
    >>> transforms.to_pobject(a).get()
    [1, 1, 2, 3]

    >>> from bigflow import transforms
    >>> b = _pipeline.parallelize({'e': 'f', 'g': 'h'})
    >>> transforms.to_pobject(b).get()
    {'e': 'f', 'g': 'h'}

    """

    from bigflow import pobject
    from bigflow import pcollection
    from bigflow import ptable

    def _to_pobject(item):
        if isinstance(item, pobject.PObject):
            return item
        elif isinstance(item, pcollection.PCollection):
            return to_list_pobject(item)
        elif isinstance(item, ptable.PTable):
            return to_list_pobject(item.apply_values(lambda x: _to_pobject(x)) \
                                   .flatten()) \
                                   .map(lambda x: dict(x))
        else:
            raise ValueError("to_pobject only applied on PCollections/PObject/PTable")

    return _to_pobject(pvalue)

def make_tuple(*pobjects, **options):
    """
        将所有输入的PObject合并成一个PObject(tuple)。

        除返回值类型及输入类型都全是PObject外，
        结果与
        :func:`bigflow.transforms.cartesian(self, *pvalues, **options)
        <bigflow.transforms.cartesian>` 相同。

        Args: *pobjects (PObject) 待操作PObjects。所有输入都必须是PObject.
        Returns:
            PObject: 返回一个PObject(tuple), tuple中的第n个元素是第n个输入PObject对应的值。

        >>> p1 = _pipeline.parallelize(1)
        >>> p2 = _pipeline.parallelize(2)
        >>> transforms.make_tuple(p1, p2).get()
        (1, 2)
        >>> p3 = _pipeline.parallelize([3, 4])
        >>> transforms.make_tuple(p1, p3).get()
        !!! AssertionError
        >>> transforms.make_tuple(p1, p1, p2).get()
        (1, 1, 2)

    """
    from bigflow.transform_impls import make_tuple
    return make_tuple.make_tuple(*pobjects, **options)
