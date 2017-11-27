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
File: join.py
Author: panyunhong(bigflow-opensource@baidu.com), Wang Cong(bigflow-opensource@baidu.com)
Date: 2015/03/14 10:43:07
"""

from bigflow import pcollection
from bigflow import serde
from bigflow.core import entity
from bigflow.transform_impls import cartesian
from bigflow.transform_impls import cogroup

def _one_side_join_append_tuple(t1, t2):
    result = []
    t2 = list(t2)
    if t2 is None or len(t2) == 0:
        result.append(t1 + (None,))
    else:
        for e in t2:
            result.append(t1 + (e,))
    return result


def __left_join_in_every_group(*pcollections, **options):
    serdes = (pcollections[0].serde(), pcollections[1].serde())
    current = pcollections[0].flat_map(entity.OneSideJoinFn(),
        pcollections[1],
        serde = serde.tuple_of(*serdes), **options)

    for i in range(2, len(pcollections)):
        serdes = serdes + (pcollections[i].serde(),)
        current = current.flat_map(_one_side_join_append_tuple,
            pcollections[i],
            serde = serde.tuple_of(*serdes), **options)
    return current


def __right_join_in_every_group(*pcollections, **options):
    serdes = serde.of(tuple(map(lambda p: p.serde(), pcollections)))
    return __left_join_in_every_group(*pcollections[::-1], **options).map(lambda x: x[::-1], serde = serdes)


def __full_join_in_every_group(*pcollections, **options):

    def transform_append_tuple(left_table_empty, emitter, record, side_input):
        left_table_empty = False
        right_table_empty = True
        for e in side_input:
            right_table_empty = False
            emitter.emit(record + (e,))

        if right_table_empty:
            emitter.emit(record + (None,))
        return left_table_empty

    def finalize_append_tuple(left_table_empty, emitter, side_input):
        if left_table_empty:
            len_tuple = i
            nones_tuple = tuple([None for x in range(len_tuple)])
            for e in side_input:
                emitter.emit(nones_tuple + (e,))

    serdes = (pcollections[0].serde(), pcollections[1].serde())
    current = pcollections[0].transform(entity.FullJoinInitializeFn(),
            entity.FullJoinTransformFn(),
            entity.FullJoinFinalizeFn(),
            pcollections[1],
            serde = serde.tuple_of(*serdes), **options)

    for i in range(2, len(pcollections)):
        serdes = serdes + (pcollections[i].serde(), )
        current = current.transform(
            entity.FullJoinInitializeFn(),
            transform_append_tuple,
            finalize_append_tuple,
            pcollections[i],
            serde = serde.tuple_of(*serdes), **options)

    return current


def check_argument(pcollections):
    if not all(isinstance(p, pcollection.PCollection) for p in pcollections):
        raise ValueError("joins only applied on PCollections")

    if len(pcollections) < 2:
        raise ValueError("require at least 2 pcollections")


def _get_cogroup_option(options):
    ret = {}
    def _get_opt(name):
        opt = options.get(name)
        if opt is not None:
            ret[name] = opt
    _get_opt('key_serde')
    _get_opt('value_serdes')
    _get_opt('concurrency')
    return ret


def join(*pcollections, **options):
    """ inner function """
    check_argument(pcollections)
    return cogroup.cogroup(*pcollections, **_get_cogroup_option(options)) \
        .apply_values(cartesian.cartesian, **options) \
        .flatten()


def left_join(*pcollections, **options):
    """ inner function """
    check_argument(pcollections)

    serdes = map(lambda p: p.serde(), pcollections)

    return cogroup.cogroup(*pcollections, **_get_cogroup_option(options)) \
        .apply_values(__left_join_in_every_group, **options) \
        .flatten()


def right_join(*pcollections, **options):
    """ inner function """
    check_argument(pcollections)

    return cogroup.cogroup(*pcollections, **_get_cogroup_option(options)) \
        .apply_values(__right_join_in_every_group, **options) \
        .flatten()


def full_join(*pcollections, **options):
    """ inner function """
    check_argument(pcollections)
    return cogroup.cogroup(*pcollections, **_get_cogroup_option(options)) \
        .apply_values(__full_join_in_every_group, **options) \
        .flatten()
