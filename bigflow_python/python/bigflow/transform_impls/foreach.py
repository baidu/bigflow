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

from bigflow.core.serde import omnitypes_objector
from bigflow.transform_impls import processor
from bigflow.util import side_input_util
from bigflow import output
from bigflow import pcollection


class ForeachProcessor(processor.ProcessorWithSideInputs):
    """ A Processor for foreach

    """
    def process(self, index, record):
        self._fn(record, *self._side_inputs)


def foreach(pvalue, fn, *side_inputs, **options):
    """ Transform foreach implementation

    :param pvalue: PType
    :param fn: UDF
    :param side_inputs: SideInputs
    :return:
    """
    memory = options.get('memory_limit', -1)
    cpu = options.get('cpu_limit', -1)

    side_inputs = side_input_util.SideInputsUtil.get_dealt_side_inputs_tuple(side_inputs)
    helper = side_input_util.SideInputsUtil(pvalue, side_inputs)

    tserde = pvalue.pipeline().default_objector()

    map_node = helper.process_with_side_inputs().by(ForeachProcessor(fn, *side_inputs))\
        .as_type(tserde)\
        .set_debug_info("Foreach: " + repr(fn)) \
        .set_effective_key_num(0) \
        .input(-1).allow_partial_processing().done() \
        .set_memory(memory) \
        .set_cpu(cpu)

    plan = map_node.plan()
    shuffle_scope = plan.shuffle(map_node.scope(), [map_node])
    node = shuffle_scope.node(0)
    node = node.distribute_by_default()

    node.sink_by(output._EmptyOutputFormat())
