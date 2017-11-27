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
File: select_elements_impl.py
Author: wenchunyang(bigflow-opensource@baidu.com)
Date: 2016/05/03 13:01:57
"""

import math
from bigflow import pcollection
from bigflow import pobject
from bigflow import transforms
from bigflow.core import entity
from bigflow.util import side_input_util
from bigflow.transform_impls import sort as _sort


def _select_elements_pobject(pvalue, n, key=None, isMaxed=True, **options):
    """pobject as side input for select elements"""
    scale = options.get('scale', 0.1)
    partial_scale = math.sqrt(scale)
    size = options.get('output_size', None)
    select_type = "Max" if isMaxed else "Min"
    tserde = options.get('serde', pvalue.serde())

    if size is None:
        partial_size = None
    else:
        partial_size = pvalue.node().size() * \
                math.sqrt(size / pvalue.node().size())

    partial_helper = side_input_util.SideInputsUtil(pvalue, (n, ))
    partial_node = partial_helper.process_with_side_inputs() \
            .by(entity.SelectElementsProcessor(n, isMaxed, key)) \
            .as_type(tserde) \
            .set_debug_info("%sElementsPartial: PObject" % (select_type)) \
            .set_effective_key_num(0) \
            .input(-1).allow_partial_processing() \
            .done() \
            .set_size(partial_size, partial_scale)

    partial = pcollection.PCollection(partial_node, pvalue.pipeline())

    partial = partial.sort_by(key, isMaxed)
    result_helper = side_input_util.SideInputsUtil(partial, (n, ))

    result_node = result_helper.process_with_side_inputs() \
            .by(entity.TakeProcessor(n))\
            .as_type(tserde) \
            .set_effective_key_num(0) \
            .set_debug_info("%sElements: PObject" % (select_type))\
            .set_size(size, partial_scale)

    return result_node


def _select_elements_int(pvalue, n, key=None, isMaxed=True, **options):
    """normal select elements"""
    scale = options.get('scale', 0.1)
    partial_scale = math.sqrt(scale)
    size = options.get('output_size', None)
    select_type = "Max" if isMaxed else "Min"
    tserde = options.get('serde', pvalue.serde())

    if size is None:
        partial_size = None
    else:
        partial_size = pvalue.node().size() * \
                math.sqrt(size / pvalue.node().size())

    if n == 1:
        if isMaxed:
            return transforms.max(pvalue, key).node()
        else:
            return transforms.min(pvalue, key).node()

    node = pvalue.node()

    result_node = node.process_by(entity.SelectElementsProcessor(n, isMaxed, key)) \
            .as_type(tserde) \
            .set_effective_key_num(0) \
            .set_debug_info("%sElementsPartial: %d" % (select_type, n)) \
            .input(0).allow_partial_processing().done() \
            .set_size(partial_size, partial_scale) \
            .sort_by(_sort.DefaultKeyReader(key, isMaxed))\
            .process_by(entity.TakeProcessor(n)) \
            .as_type(tserde) \
            .set_effective_key_num(0) \
            .set_size(size, partial_scale) \
            .set_debug_info("%sElements: %d" % (select_type, n))

    return result_node


def select_elements(pvalue, n, key=None, isMaxed=True, **options):
    """implementation of select elements"""

    if not isinstance(pvalue, pcollection.PCollection):
        raise ValueError("Invalid arguments: pvalue must be of type PCollection")

    if isinstance(n, pobject.PObject):
        result_node = _select_elements_pobject(pvalue, n, key, isMaxed, **options)
    elif isinstance(n, (int, long)):
        result_node = _select_elements_int(pvalue, n, key, isMaxed, **options)

    return pcollection.PCollection(result_node, pvalue.pipeline())
