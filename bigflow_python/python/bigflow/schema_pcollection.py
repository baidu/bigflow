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
:class:`bigflow.schema_pcollection.SchemaPCollection` 定义

.. module:: bigflow.schema_pcollection
   :synopsis: SchemaPCollection

   """

from bigflow import pcollection
from bigflow import ptable
from bigflow import schema
from bigflow import serde

class SchemaPCollection(pcollection.PCollection):
    """
    用来表示结构化的, 带字段的PCollection, 它拥有普通PCollection的所有操作,
    等价于每个元素是一个dict(FieldsDict)的分布式数据集(PCollection) :class:`bigflow.pcollection.PCollection`
    或者PCollection中每个元素是tuple类型, 用户需要根据下标来操作(select/group_by/agg/等)

    Constructor
    .. note:: 根据实际传入的node类型决定不同的构造方法

    Args:
        node_pc (PCollection/Node): 当node_pc是PCollection,每个元素应是FieldsDict或者是tuple;
                                    当node_pc是LogicalPlan.Node, pipeline != None
        pipeline (Pipeline): 用户一个分布式计算任务的抽象

    """

    def __init__(self, node_pc, pipeline=None):
        if pipeline is None:
            if isinstance(node_pc, ptable.PTable):
                raise ValueError("SchemaPCollection can't be constructed by PTable")
            sd = node_pc.serde()
            if isinstance(sd, serde.Optional):
                sd = sd.origin_serde()
            if isinstance(sd, schema.FieldsDictSerde):
                self._fields = sd.get_fields()
                super(SchemaPCollection, self).__init__(node_pc.node(), node_pc.pipeline())
            elif isinstance(sd, serde.TupleSerde):
                self._fields = None
                super(SchemaPCollection, self).__init__(node_pc.node(), node_pc.pipeline())
            else:
                raise ValueError("pcollection's element should be FieldsDictSerde or TupleSerde")
        else:
            self._fields = None
            super(SchemaPCollection, self).__init__(node_pc, pipeline)

    def _get_fields(self):
        """ 内部函数 """
        return self._fields

    def __str__(self):
        fields_str = ','.join(self._fields) if self._fields is not None else ''
        return "SchemaPCollection(%s)" % fields_str
