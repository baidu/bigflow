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
File: cartesian.py
Author: panyunhong(bigflow-opensource@baidu.com)
Date: 2015/03/22 22:39:50
"""
from bigflow import pcollection
from bigflow import serde
from bigflow import pobject
from bigflow.core import entity
from bigflow.util import utils


def cartesian_append_fn(x, y):
    for e in y:
        yield x + (e,)


def cartesian(*pcollections, **kargs):
    """ inner function"""

    if len(pcollections) == 1:
        return pcollections[0].map(lambda x: (x, ),
            serde=serde.of((pcollections[0].node().serde(), )))

    def _is_flat_ptype(ptype):
        is_flat = isinstance(ptype, pcollection.PCollection) or isinstance(ptype, pobject.PObject)
        return is_flat and not utils.is_infinite(ptype)

    if not all(_is_flat_ptype(p) for p in pcollections):
        raise ValueError("cartesian only applied on finite PCollections/PObjects")

    if len(pcollections) < 2:
        raise ValueError("cartesian expect 2 or more arguments")

    serde1 = pcollections[0].node().serde()
    serde2 = pcollections[1].node().serde()
    rserde = (serde1, serde2)

    def to_pcollection(pobject):
        """ inner fn"""
        return pcollection.PCollection(pobject.node(), pobject.pipeline())

    current = pcollections[0].flat_map(entity.CartesianFn(), to_pcollection(pcollections[1]),
        serde=serde.of(rserde), **kargs)

    for i in range(2, len(pcollections)):
        rserde = rserde + (pcollections[i].node().serde(),)
        current = current.flat_map(cartesian_append_fn,
            to_pcollection(pcollections[i]),
            serde=serde.of(rserde), **kargs)

    return current
