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
Implementation of transforms.accumulate()

"""


from bigflow import pobject
from bigflow.util import side_input_util
from bigflow.util import utils
from bigflow.core import entity


def accumulate(pvalue, zero, accumulator, *side_inputs, **kargs):
    """
    Implementation of transforms.accumulate()
    """

    if utils.is_infinite(pvalue):
        raise ValueError("accumulate not supported infinite PType")

    objector = kargs.get('serde', pvalue.pipeline().default_objector())

    side_inputs = side_input_util.SideInputsUtil.get_dealt_side_inputs_tuple(side_inputs)
    helper = side_input_util.SideInputsUtil(pvalue, side_inputs)
    result_node = helper.process_with_side_inputs() \
        .by(entity.AccumulateProcessor(entity.Functor.of(zero), entity.Functor.of(accumulator))
            .set_side_inputs(*side_inputs)) \
        .set_debug_info("accumulate(" + repr(zero) + ',' + repr(accumulator)) \
        .as_type(objector) \
        .set_effective_key_num(0) \
        .set_size(kargs.get('output_size', None), kargs.get('scale', 0.1)) \
        .set_memory(kargs.get('memory_limit', -1)) \
        .set_cpu(kargs.get('cpu_limit', -1))

    return pobject.PObject(result_node, pvalue.pipeline())
