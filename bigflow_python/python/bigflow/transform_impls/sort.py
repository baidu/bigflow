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
File: sort.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/03/14 13:01:57
"""

import pickle
import struct

import bigflow.pcollection
import bigflow.pobject
from bigflow.core.serde import omnitypes_objector
from bigflow.core import entity
from bigflow import serde
from bigflow.util import utils

class DefaultKeyReader(object):
    def __init__(self, key_read_fn=None, reverse=False):
        self.reverse = reverse
        self.read_key = key_read_fn
        self.serialize = None


class ASC(object):
    """
    wrapper for asc key
    """
    def __init__(self, key):
        self.reverse = False
        self.key = key


class DESC(object):
    """
    wrapper for desc key
    """
    def __init__(self, key):
        self.reverse = True
        self.key = key


def sort_by(pvalue, key_read_fn, reverse=False):
    if utils.is_infinite(pvalue):
        raise ValueError("sort_by not supported infinite PType")
    result_node = pvalue.node().sort_by(DefaultKeyReader(key_read_fn, reverse))
    return bigflow.pcollection.PCollection(result_node, pvalue.pipeline())


def _sort_str(pvalue, reverse=False):

    class ReverseStrSerde(serde.Serde):
        """ test """

        def serialize(self, obj):
            """ inner """
            return ''.join(chr(255 - ord(ch)) for ch in obj)

        def deserialize(self, buf):
            """ inner """
            return ''.join(chr(255 - ord(ch)) for ch in buf)

    def _serde_to_string(serde):
        return entity.Entity.of(entity.Entity.objector, serde) \
                .to_proto_message().SerializeToString()

    str_serde = None

    if not reverse:
        str_serde = serde.of(str)
    else:
        str_serde = serde.Optional(ReverseStrSerde())

    class _StrSortKeyReader(object):

        def __init__(self, serde):
            self.objector = _serde_to_string(serde)
            self.read_key = lambda x: x

    class SetValueNoneProcessor(entity.Processor):
        """ inner """
        def __init__(self):
            super(SetValueNoneProcessor, self).__init__()

    class SetKeyToValueProcessor(entity.Processor):
        """ inner """
        def __init__(self, serde):
            super(SetKeyToValueProcessor, self).__init__()
            self.set_config(_serde_to_string(serde))

    key_reader_obj = _StrSortKeyReader(str_serde)

    result_node = pvalue.node()._plan.shuffle(pvalue.node()._scope, from_nodes=[pvalue.node()]) \
        .sort() \
        .node(0).match_by(key_reader_obj, entity.Entity.key_reader) \
        .set_debug_info("Sort: " + repr(key_reader_obj)) \
        .process_by(SetValueNoneProcessor())\
        .as_type(serde.of(str)) \
        .set_effective_key_num(0) \
        .ignore_group() \
        .input(0).allow_partial_processing().done() \
        .process_by(SetKeyToValueProcessor(str_serde)) \
        .as_type(serde.of(str)) \
        .leave_scope()

    return bigflow.pcollection.PCollection(result_node, pvalue.pipeline())

def sort(pvalue, reverse=False):
    """ sort """

    if isinstance(serde.origin(pvalue.serde()), serde.StrSerde):
        return _sort_str(pvalue, reverse)

    return sort_by(pvalue, None, reverse)
