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
"""
Script Definition

"""
import math

from bigflow import pobject
from bigflow.core import entity
from bigflow.util import side_input_util
from bigflow.util import utils


def reduce(ptype, fn, *side_inputs, **kargs):
    """
    inner fun
    """

    if utils.is_infinite(ptype):
        raise ValueError("reduce not supported infinite PType")

    scale = kargs.get('scale', 0.1)
    partial_scale = math.sqrt(scale)
    size = kargs.get('output_size', None)
    if size is None:
        partial_size = None
    else:
        partial_size = ptype.node().size() * math.sqrt(size / ptype.node().size())
    memory = kargs.get('memory_limit', -1)
    cpu = kargs.get('cpu_limit', -1)

    objector = kargs.get('serde', ptype.serde()) # use the same serde of the input
    side_inputs = side_input_util.SideInputsUtil.get_dealt_side_inputs_tuple(side_inputs)
    partial_helper = side_input_util.SideInputsUtil(ptype, side_inputs)

    partial_node = partial_helper.process_with_side_inputs()\
        .by(entity.ReduceProcessor(fn).set_side_inputs(*side_inputs))\
        .as_type(objector)\
        .set_debug_info("ReducePartial: " + repr(fn)) \
        .set_effective_key_num(0) \
        .input(-1).allow_partial_processing().done() \
        .set_size(partial_size, partial_scale) \
        .set_memory(memory) \
        .set_cpu(cpu)

    non_partial_helper = side_input_util.SideInputsUtil(partial_node, side_inputs)

    non_partial_node = non_partial_helper.process_with_side_inputs()\
        .by(entity.ReduceProcessor(fn).set_side_inputs(*side_inputs))\
        .as_type(objector)\
        .set_debug_info("Reduce: " + repr(fn)) \
        .set_effective_key_num(0) \
        .set_size(size, partial_scale) \
        .set_memory(memory) \
        .set_cpu(cpu)

    return pobject.PObject(non_partial_node, ptype.pipeline())
