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
:class:`.PTable` 定义

.. module:: bigflow.ptable
   :synopsis: PTable

"""

from bigflow import error
from bigflow import pobject
from bigflow import ptype
from bigflow import transforms


class PTable(ptype.PType):
    """
    用于表示具有分布式Key-Value映射关系的 :class:`bigflow.ptype.PType`

    Constructor
    .. note:: 用户不应当直接使用其构造方法

    Args:
      value (PType):  PTable的Value

    Methods:
    """

    def __init__(self, value, **options):
        super(PTable, self).__init__(None, None)

        if isinstance(value, PTable):
            self.__nested_level = value.nested_level() + 1
        else:
            self.__nested_level = 0

        if isinstance(value, ptype.PType):
            self._node = value.node()
            self._pipeline = value.pipeline()
        elif isinstance(value, tuple):
            if all(isinstance(e, ptype.PType) for e in value):
                self._pipeline = value[0].pipeline()
            else:
                raise ValueError("Error value type for PTable")
        else:
            raise ValueError("Error value type for PTable")

        assert self._pipeline is not None

        self.__value = value
        self.__key = None

        self._key_serdes = [options.get('key_serde', self.pipeline().default_objector())]
        if isinstance(value, PTable):
            self._key_serdes.extend(value.key_serdes())
        assert len(self._key_serdes) == self.__nested_level + 1

    # Override
    def node(self):
        """
        返回PTable所对应的Node

        Returns:
          LogicalPlan.Node:  node

        Raises:
          BigflowPlanningException:  若无法得到Node

        .. note:: 用户不应当使用此方法

        """
        if self._node is None:
            raise error.BigflowPlanningException("No node in PTable (whose value is %s), "
                                                  "such transform(s) is not supported."
                                                  % (str(self._value())))
        return self._node

    def _key(self, ensure_keep_group=False):
        '''
            内部函数

            ensure_keep_group的话，则返回至少发一条数据给reduce，确保每个group都保留着。
            否则，依赖于其它结点产生group。

        '''
        value = self._value()
        value = value[0] if isinstance(value, tuple) else value

        take_num = 1 if ensure_keep_group else 0

        if self.__key is None:

            import bigflow.transforms
            from bigflow.core import entity

            key_serde = self.key_serdes()[0]
            deserialize = entity.SerdeWrapper(key_serde, is_serialize=False)
            key_node = bigflow.transforms.flatten_values(value).node() \
                .process_by(entity.TakeProcessor(take_num)) \
                .as_type(value.serde()) \
                .set_debug_info("ExtractKeyPartial") \
                .input(0).allow_partial_processing().done() \
                .process_by(entity.GetLastKeyProcessor(deserialize)) \
                .as_type(key_serde) \
                .set_debug_info("ExtractKey")

            self.__key = pobject.PObject(key_node, self._pipeline)

        return self.__key

    def _value(self):
        return self.__value

    def nested_level(self):
        """
        返回该PTable的嵌套层级，即其Value中包含几个PTable

        Returns:
          int:  嵌套层级

        >>> _pipeline.parallelize({"A": 1}).nested_level()
        >>> 0

        >>> _pipeline.parallelize({"A": "a": 1}).nested_level()
        >>> 1
        """
        return self.__nested_level

    def inner_most_type(self):
        """
        返回其最内部Value的类型

        Returns:
          class:  最内部Value类型，PCollection或PObject

        >>> _pipeline.parallelize({"A": 1}).inner_most_type()
        >>> bigflow.pcollection.PCollection
        """
        return self.__inner_most_value().__class__

    def extract_keys(self, **options):
        """
        提取给定PTable中所有的key，等价于 ``transforms.extract_keys(self, options)``
        Args:
          **options:  可配置选项

        Returns:
          PCollection:  所有的key，以PCollection给出
        """
        return transforms.extract_keys(self, **options)

    def extract_values(self, **options):
        """
        提取给定PTable中所有的value，等价于 ``transforms.extract_values(self, options)``

        Args:
          **options:  可配置选项

        Returns:
          PCollection:  所有的value，以PCollection给出
        """
        return transforms.extract_values(self, **options)

    def apply_values(self, transform, *args, **options):
        """
        对Value进行一个变换

        Args:
          transform (callable):  作用在Value上的变换函数
          *args (object):  变换所需要的参数列表
          **options:  可配置选项

        Returns:
          PTable:  变换结果

        ::

            >> nums = _pipeline.parallelize([1, 2, 3])
            >> grouped = nums.group_by(lambda n: n % 2)
            >> grouped.apply_values(transforms.sum).get()
            {0: [2], 1: [4]}

        """
        if isinstance(self.__value, ptype.PType):
            _transformed = self.__value.apply(transform, *self._broadcast(args), **options)
        else:
            # Hardcode for multi-nodes in PTable
            all_args = self.__value + self._broadcast(args)
            _transformed = transform(*all_args, **options)

        return PTable(_transformed, key_serde = self.key_serdes()[0])

    def apply_key_values(self, transform, *side_inputs, **options):
        """
        将Key和Value做一个变换

        Args:
          transform (function):  变换函数
          *side_inputs:  参与计算的SideInputs
          **options:  可配置选项

        Returns:
          PTable:  变换结果

        ::

            >> nums = _pipeline.parallelize([1, 2, 3, 4, 5, 6, 7])
            >> grouped = nums.group_by(lambda n: n % 2)
            >> def in_every_group(key, value):
            ..     key = key.map(lambda k: 2 if k == 1 else 3)
            ..     return value.take(key)
            >> grouped.apply_key_values(in_every_group).get()
            {0: [2, 4, 6], 1: [1, 3]}

        """
        ensure_keep_group = False
        if 'ensure_keep_group' in options:
            ensure_keep_group = options['ensure_keep_group']
            del options['ensure_keep_group']

        if isinstance(self._value(), tuple):
            _transformed = transform(
                    self._key(ensure_keep_group=ensure_keep_group),
                    *self._value() + self._broadcast(side_inputs),
                    **options)
        else:
            _transformed = transform(
                    self._key(ensure_keep_group=ensure_keep_group),
                    self._value(),
                    *self._broadcast(side_inputs),
                    **options)

        return PTable(_transformed, key_serde=self.key_serdes()[0])

    def flatten(self, **option):
        """
        对于每个Key和Value中的每个元素(value 1, value 2, ... value m)，构造(Key, value 1), (Key, value 2), ... (Key, value m)，结果使用PCollection表示

        Returns:
          PCollection:  表示结果的PCollection
        """
        return transforms.flatten(self, **option)

    def flatten_values(self):
        """
        使用Value中的每个元素(value 1, value 2, ... value m)，构造PCollection，等价于 ``self.extract_values()``

        Returns:
          PCollection:  包含所有Value的PCollection

        """
        return transforms.flatten_values(self)

    def _broadcast(self, side_input_tuple):
        from bigflow.util import broadcast

        broadcasted = []
        for p in side_input_tuple:
            if isinstance(p, PTable):
                raise error.BigflowPlanningException(" PTable can not be broadcasted.")
            if not broadcast.is_same_working_scope(p, self):
                raise error.BigflowPlanningException("Broadcasted values not in "
                                                      "correct working scope")
            broadcasted.append(broadcast.broadcast_to(p, broadcast.working_scope(self._value())))

        return tuple(broadcasted)

    def key_serdes(self):
        """
        返回Key的序列化/反序列化器
        """
        return self._key_serdes

    def _parse_cached_data(self, keys_value):
        value_serde = self.serde()
        key_serdes = self.key_serdes()
        previous_keys = None
        current_values = None
        dict_root = dict()

        is_pobject_value = isinstance(self.__inner_most_value(), pobject.PObject)

        for kv in keys_value:
            result_value = value_serde.deserialize(kv.value)

            if len(kv.key) == 0:
                raise error.InvalidDataException("PTable should contain keys")

            assert len(kv.key) == len(key_serdes), "key number is incorrect"

            result_keys = map(lambda x: x[1].deserialize(x[0]), zip(kv.key, key_serdes))

            if is_pobject_value:
                last_dict = PTable.__get_dict_from_keys(dict_root, result_keys)
                last_key = result_keys[-1]
                if last_key in last_dict:
                    raise error.InvalidDataException("Duplicate (keys, values) pair")

                last_dict[last_key] = result_value
            else:
                if previous_keys is None or previous_keys != result_keys:
                    last_dict = PTable.__get_dict_from_keys(dict_root, result_keys)
                    last_key = result_keys[-1]

                    if last_key in last_dict:
                        raise error.InvalidDataException("Duplicate (keys, values) pair!")

                    if is_pobject_value:
                        last_dict[last_key] = result_value
                    else:
                        current_values = []
                        last_dict[last_key] = current_values

                    previous_keys = result_keys

                current_values.append(result_value)

        return dict_root

    def __inner_most_value(self):
        self_value = self._value()
        while isinstance(self_value, PTable):
            self_value = self_value._value()

        return self_value

    def __repr__(self):
        name = "{k0: "
        for i in range(1, self.nested_level() + 1):
            name += "{k%d: " % i

        name += "%s%s" % (repr(self.__inner_most_value()), "}" * (self.nested_level() + 1))

        return name

    @staticmethod
    def __get_dict_from_keys(root, keys):
        current = root
        for key in keys[:-1]:

            if key not in current:
                current[key] = dict()
            current = current[key]

        return current
