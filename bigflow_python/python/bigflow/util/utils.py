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
from bigflow import pobject
from bigflow import ptype


def is_ptype(obj):
    """
    Return if an object is a PType

    Args:
      obj (object):  object

    Returns:
      bool:  True if the object is a PType, False otherwise
    """
    return isinstance(obj, ptype.PType)


def is_infinite(obj):
    """
    Return if a PType is infinite

    Args:
      obj (PType):  PType

    Returns:
      bool:  True if the PType is infinite, False otherwise
    """
    if not isinstance(obj, ptype.PType):
        raise TypeError("Invalid type for infinite")
    return obj.node().is_infinite()


def flatten_runtime_value(runtime_value):
    """
    Flatten a Python dict to tuple

    Args:
      runtime_value (object):  value to flatten

    Returns:
      object:  flatten result
    """
    import collections

    def helper(keys, current):
        for k, v in current.items():
            new_key = keys + (k, )
            if isinstance(v, collections.MutableMapping):
                helper(new_key, v)
            elif isinstance(v, list):
                for e in v:
                    results.append(new_key + (e, ))
            else:
                results.append(new_key + (v, ))

    results = []

    if isinstance(runtime_value, collections.MutableMapping):
        helper(tuple(), runtime_value)
    elif isinstance(runtime_value, list):
        for e in runtime_value:
            results.append(e)
    else:
        results.append(runtime_value)

    return results


def detect_ptype(runtime_value):
    """
    Detect the default PType type for a runtime value

    Args:
      runtime_value (object):  a runtime value, cannot be PType

    Returns:
      class:  detected PType class
    """
    import collections

    def helper(nested_level, v):
        if isinstance(v, collections.MutableMapping):
            return helper(nested_level + 1, v.values()[0])
        elif isinstance(v, list):
            return nested_level, pcollection.PCollection
        else:
            return nested_level, pobject.PObject

    if isinstance(runtime_value, ptype.PType):
        raise ValueError("Input cannot be PType")

    if isinstance(runtime_value, collections.MutableMapping):
        return helper(0, runtime_value.values()[0])
    elif isinstance(runtime_value, list):
        return -1, pcollection.PCollection
    else:
        return -1, pobject.PObject


def construct(pipeline, node, type, nested_level=None, inner_most_type=None, key_serdes=None):
    """
    Construct a PType from a LogicalPlan node

    Args:
      pipeline (Pipeline):  the Pipeline constructed PType belongs to
      node (LogicalPlan.Node):  node
      type (class):  class of PType to construct

    Kwargs:
      nested_leve: specify PTable's nested level if PType is a PTable
      inner_most_type:  specify PTable's inner-most type if PType is a PTable

    Returns:
      PType:  PType
    """
    if inner_most_type is ptable.PTable:
        raise ValueError("Invalid value type for PTable")

    if type is pobject.PObject:
        pvalue = pobject.PObject(node, pipeline)
    elif type is pcollection.PCollection:
        pvalue = pcollection.PCollection(node, pipeline)
    else:
        if key_serdes is None:
            key_serdes = [pipeline.default_objector()] * (nested_level + 1)
        if nested_level > 0:
            pvalue = ptable.PTable(construct(pipeline,
                                             node,
                                             type,
                                             nested_level - 1,
                                             inner_most_type,
                                             key_serdes[1:]), key_serde = key_serdes[0])
        else:
            pvalue = ptable.PTable(inner_most_type(node, pipeline))

    return pvalue
