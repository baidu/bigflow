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
.. module:: bigflow.counter
   :synopsis: counter api

"""

import os
import types
from bigflow import error

counter_dict = {}


def increase(name, increment=1):
    """
    累加name对应的counter，累加值为increment

    Args:
      name (str):  counter名称，只接受str类型
      increment (int): 累加值，只接受正数

    Raises:
      error.BigflowPlanningException:   此函数仅允许在 :mod:`Bigflow变换<bigflow.transforms>` 的用户自定义方法(UDF)中调用，否则抛出此异常

    Note:
      1. counter 具有 group 的概念, 如果 name 格式为"group1|name1"，则 group1 为 counter 所在 group; 若不包含, 则默认的 group 为 'Flume'

      2. counter 属于一个 Bigflow Pipeline，并在 Pipeline 多次运行时累加，若需要将 counter 清零，请使用 Pipeline 的 :meth:`reset_counter<bigflow.pipeline.pipeline_base.PipelineBase.reset_counter>` 或 :meth:`reset_all_counters<bigflow.pipeline.pipeline_base.PipelineBase.reset_all_counters>` 方法
         当前实现中, reset_counter 是个全局操作. reset_counter 将会重置所有 pipeline 中定义的 counter. 如有多 pipeline 重置 counter 的需求, 请为每个 pipeline 设置不同的 counter idenfier
    >>> from bigflow import base, counter
    >>> _pipeline = base.Pipeline.create("LOCAL")
    >>> _p = _pipeline.parallelize([3, 7, 1])
    >>> def all_num_counting(record):
    ...     counter.increase("all_num")
    ...     return record
    ...
    >>> _p = _p.map(all_num_counting)
    >>> _p.get()
    """
    if os.getenv("__PYTHON_IN_REMOTE_SIDE", None) is None:
        raise error.BigflowPlanningException("counter.increase should only called at runtime")
    if type(name) is not types.StringType:
        name = str(name)
    if name not in counter_dict:
        counter_dict[name] = increment
    else:
        counter_dict[name] += increment


def _get(name, group=None):
    """ get a counter value with counter_name: name
    If name is canonical, a.k.a the form of 'group|name', then the second group parameter is
    ignored. Otherwise group will be prepended to the name to generate the counter_name. Default
    group name is 'Flume' if no group is supplied.

    ... Note: To get counter that has `|` in its name, 'g1|a|b' for example, use _get('g1|a|b')
              rather than _get('a|b', 'g1')
    """
    if os.getenv("__PYTHON_IN_REMOTE_SIDE", None) is not None:
        raise error.BigflowPlanningException("counter.get should not called at runtime")
    from bigflow.rpc import requests
    result_counters = requests.get_counters()

    group_name = "Flume" if group is None else str(group)
    for index, counter_name in enumerate(result_counters.name):
        counter_key = name if "|" in name else group_name + "|" + name
        if counter_name == counter_key:
            return result_counters.value[index]


def _get_all(grouped=False):
    """ get the counter dict include all counters. Group name is prepended to the key in the return
        dict if grouped is False.

    :param grouped: boolean, the returned dict should be grouped by group name or not
    :return dict.
    """
    if os.getenv("__PYTHON_IN_REMOTE_SIDE", None) is not None:
        raise error.BigflowPlanningException("counter.get_all should not called at runtime")
    from bigflow.rpc import requests
    result_counters = requests.get_counters()

    c_dict = {}
    if not grouped:
        for index, counter_name in enumerate(result_counters.name):
            c_dict[counter_name] = result_counters.value[index]
    else:
        for index, counter_name in enumerate(result_counters.name):
            group, name = "Flume", counter_name
            if "|" in counter_name:
                group, name = counter_name.split("|", 1)
            c_dict.setdefault(group, {})[name] = result_counters.value[index]
    return c_dict


def _get_group_counter(group="Flume", *other_groups):
    """ Get all counters in specific groups
    Usage:
    >>> # Get all counters under the group 'Flume'. Result is one level dict: {'counter' -> value}
    >>> counter._get_group_counter("Flume")
    >>> counter._get_group_counter() # same sa previous call

    >>> # Get all counters under groups(Flume and HDFS). Result is nested dict, {group -> {counter->value}}
    >>> counter._get_group_counter("Flume", "HDFS")

    :type group: str
    :param group: group name
    :type other_groups: list<str>
    :param other_groups: group names
    :return: dict, {key(group_name) -> { key(counter_name) -> value(counter_value)} if other_groups is supplied or
            {key(counter_name) -> value(counter_value)}
    """
    assert isinstance(group, str)
    groups = (group, ) + other_groups
    counter_with_group = _get_all(True)
    result = {k: counter_with_group.get(k, {}) for k in groups}
    return result if len(groups) > 1 else result[group]
