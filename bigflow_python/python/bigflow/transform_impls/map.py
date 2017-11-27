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

from bigflow.core import entity
from bigflow.util import side_input_util


def map(pvalue, fn, *side_inputs, **kargs):
    """ Transform map implementation

    :param pvalue: PType
    :param fn: UDF
    :param side_inputs: SideInputs
    :return:
    """
    serde = kargs.get('serde', pvalue.pipeline().default_objector())
    scale = kargs.get('scale', 1.0)
    size = kargs.get('output_size', None)
    memory = kargs.get('memory_limit', -1)
    cpu = kargs.get('cpu_limit', -1)

    side_inputs = side_input_util.SideInputsUtil.get_dealt_side_inputs_tuple(side_inputs)
    helper = side_input_util.SideInputsUtil(pvalue, side_inputs)

    result_node = helper.process_with_side_inputs().by(entity.MapProcessor(fn).set_side_inputs(*side_inputs))\
        .as_type(serde)\
        .set_debug_info("Map: " + repr(fn)) \
        .ignore_group() \
        .set_effective_key_num(0) \
        .input(-1).allow_partial_processing().done() \
        .set_size(size, scale) \
        .set_memory(memory) \
        .set_cpu(cpu)

    return pvalue.__class__(result_node, pvalue.pipeline())
