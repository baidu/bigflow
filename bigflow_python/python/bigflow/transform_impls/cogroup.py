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
Author: panyunhong(bigflow-opensource@baidu.com)
"""
from bigflow import pcollection
from bigflow import ptable
from bigflow.core import entity


class KeyReader(object):
    def __init__(self, key_extractor, key_serde):
        self.objector = entity.Entity.of(entity.Entity.objector, key_serde) \
            .to_proto_message().SerializeToString()
        self.read_key = key_extractor


def cogroup(*pcollections, **kargs):
    from bigflow import serde
    """ inner function"""

    if len(pcollections) == 0:
        raise ValueError("No argument")

    pipeline = pcollections[0].pipeline()
    key_serde = kargs.get('key_serde', None)
    if key_serde is None:
        key_serde = serde._key_serde(pcollections[0].serde(), pipeline.default_objector())

    value_serdes = kargs.get('value_serdes', None)

    if value_serdes is None:
        value_serdes = []
        for p in pcollections:
            value_serdes.append(serde._value_serde(p.serde(), pipeline.default_objector()))

    def _make_shuffle(node, value_serde):
        return pcollection.PCollection(node.match_by(KeyReader(None, key_serde)), pipeline)\
            .map(entity.ExtractValueFn(), serde=value_serde, scale=0.8)

    if not all(isinstance(p, pcollection.PCollection) for p in pcollections):
        raise ValueError("cogroup only applied on PCollections")

    plan = pcollections[0].node().plan()
    scope = pcollections[0].node().scope()

    nodes = map(lambda p: p.node(), pcollections)
    shuffle = plan.shuffle(scope, nodes)

    if 'concurrency' in kargs:
        concurrency = kargs['concurrency']
        shuffle.with_concurrency(concurrency)
    elif pipeline.estimate_concurrency:
        concurrency = sum(node.size() for node in nodes) / pipeline.size_per_concurrency
        shuffle.with_concurrency(concurrency)

    results = []
    for i in range(len(nodes)):
        results.append(_make_shuffle(shuffle.node(i), value_serdes[i]))

    return ptable.PTable(tuple(results), key_serde = key_serde)
