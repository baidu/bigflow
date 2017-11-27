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
File: take.py
Author: panyunhong(bigflow-opensource@baidu.com)
Date: 2015/03/14 13:01:57
"""
import math

from bigflow import pcollection
from bigflow import pobject
from bigflow.util import utils

from bigflow.core.serde import omnitypes_objector
from bigflow.core import entity
from bigflow.transform_impls import processor
from bigflow.util import side_input_util

def take(pvalue, n, **options):
    """ inner function """

    if utils.is_infinite(pvalue):
        raise ValueError("take not supported infinite PType")

    objector = options.get('serde', pvalue.serde())

    scale = options.get('scale', 0.1)
    partial_scale = math.sqrt(scale)
    size = options.get('output_size', None)
    if size is None:
        partial_size = None
    else:
        partial_size = ptype.node().size() * math.sqrt(size / ptype.node().size())

    if isinstance(n, pobject.PObject):
        # treat the pobject param as side input
        partial_helper = side_input_util.SideInputsUtil(pvalue, (n, ))
        partial_node = partial_helper.process_with_side_inputs() \
            .by(entity.TakeProcessor(n)) \
            .as_type(objector) \
            .set_debug_info("TakePartial: " + str(n.node())) \
            .set_effective_key_num(0) \
            .input(-1).allow_partial_processing() \
            .done() \
            .set_size(partial_size, partial_scale)

        partial = pcollection.PCollection(partial_node, pvalue.pipeline())
        result_helper = side_input_util.SideInputsUtil(partial, (n, ))

        result_node = result_helper.process_with_side_inputs() \
            .by(entity.TakeProcessor(n)) \
            .as_type(objector) \
            .set_debug_info("Take: " + str(n.node())) \
            .set_effective_key_num(0) \
            .set_size(size, partial_scale)

    elif isinstance(n, (int, long)):
        result_node = pvalue.node() \
            .process_by(entity.TakeProcessor(n)) \
            .as_type(objector) \
            .set_debug_info("TakePartial: %d" % n) \
            .input(0).allow_partial_processing().done() \
            .set_size(partial_size, partial_scale) \
            .process_by(entity.TakeProcessor(n)) \
            .as_type(objector) \
            .set_debug_info("Take: %d" % n) \
            .set_effective_key_num(0) \
            .set_size(size, partial_scale)

    else:
        raise ValueError("Wrong argument, only integers are accepted")

    return pcollection.PCollection(result_node, pvalue.pipeline())
