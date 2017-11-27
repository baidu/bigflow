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
from bigflow.transform_impls import processor
from bigflow.util import reiterable_input
from bigflow.util import utils


def combine(ptype, fn, **kargs):
    """ inner function"""

    if utils.is_infinite(ptype):
        raise ValueError("combine not supported infinite PType")

    objector = kargs.get('serde', ptype.serde()) # default, use the input serde

    pre_combine = kargs.get('pre_combine', True)

    scale = kargs.get('scale', 0.1)
    partial_scale = math.sqrt(scale)
    size = kargs.get('output_size', None)

    memory = kargs.get('memory_limit', -1)
    cpu = kargs.get('cpu_limit', -1)

    def _build_combine_node(from_node, is_partial, size, scale):
        debug_info = ("Partial" if is_partial else "") + "Combine: " + repr(fn)

        result = from_node.process_by(entity.CombineProcessor(fn).set_side_inputs(ptype)) \
            .as_type(objector) \
            .set_debug_info(debug_info) \
            .set_effective_key_num(0) \
            .set_size(size, scale) \
            .set_memory(memory) \
            .set_cpu(cpu) \
            .input(0).prepare_before_processing().done()

        if is_partial:
            result = result.input(0).allow_partial_processing().done()
        return result

    if size is None:
        partial_size = None
    else:
        partial_size = ptype.node().size() * math.sqrt(size / ptype.node().size())

    combined_node = ptype.node()

    if pre_combine:
        combined_node = _build_combine_node(combined_node, True, partial_size, partial_scale)
        combined_node = _build_combine_node(combined_node, False, partial_size, partial_scale)
    else:
        combined_node = _build_combine_node(combined_node, False, size, scale)

    return pobject.PObject(combined_node, ptype.pipeline())
