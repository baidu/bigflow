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
Implementation of transforms.flat_map()

"""

from bigflow import pcollection
from bigflow import pobject
from bigflow import ptable
from bigflow.core.serde import omnitypes_objector
from bigflow.core import entity
from bigflow.transform_impls import processor
from bigflow.util import reiterable_input
from bigflow.util import side_input_util

def flat_map(pvalue, fn, *side_inputs, **kargs):
    """
    Implementation of transforms.flat_map()
    """
    objector = kargs.get('serde', pvalue.pipeline().default_objector())
    side_inputs = side_input_util.SideInputsUtil.get_dealt_side_inputs_tuple(side_inputs)
    helper = side_input_util.SideInputsUtil(pvalue, side_inputs)

    scale = kargs.get('scale', 1.5)
    size = kargs.get('output_size', None)
    memory = kargs.get('memory_limit', -1)
    cpu = kargs.get('cpu_limit', -1)

    assert isinstance(pvalue, pcollection.PCollection) or isinstance(pvalue, pobject.PObject)

    result_node = helper.process_with_side_inputs() \
        .ignore_group() \
        .by(entity.FlatMapProcessor(fn).set_side_inputs(*side_inputs)) \
        .as_type(objector) \
        .set_debug_info("FlatMap: " + repr(fn)) \
        .set_effective_key_num(0) \
        .input(-1).allow_partial_processing() \
        .done() \
        .set_memory(memory) \
        .set_cpu(cpu) \
        .set_size(size, scale)

    return pcollection.PCollection(result_node, pvalue.pipeline())
