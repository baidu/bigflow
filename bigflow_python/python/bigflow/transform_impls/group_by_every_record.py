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

# -*- coding: utf-8 -*-
"""
@Author: zhangyuncong
@Date:   2016-03-07 19:58:12
@Last Modified by:   zhangyuncong
@Last Modified time: 2016-03-08 10:33:05
"""
from bigflow import pcollection
from bigflow import ptable

def group_by_every_record(pvalue, **options):
    """
    group by every record
    """

    pipeline = pvalue.pipeline()
    node = pvalue.node()
    plan = node.plan()
    scope = node.scope()
    shuffle = plan.shuffle(scope, [node])
    shuffle_node = shuffle.node(0).distribute_every()

    from bigflow import serde
    key_serde = serde.StrSerde()
    return ptable.PTable(pcollection.PCollection(shuffle_node, pipeline), key_serde=key_serde)
