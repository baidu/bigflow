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

from bigflow import pcollection
from bigflow import ptable
from bigflow import transforms
from bigflow import serde
from bigflow.core import entity
from bigflow.util import utils

def pipe(pvalue, command, **options):
    """
    Transform pipe implementation
    :param pvalue: PType
    :return: PCollection
    """
    if utils.is_infinite(pvalue):
        raise ValueError("pipe not supported infinite PType")

    if isinstance(pvalue, ptable.PTable):
        def merge_value(pvalue):
            """ inner """
            if isinstance(pvalue, ptable.PTable):
                return pvalue.apply_values(merge_value)
            else:
                return pvalue.apply(transforms.to_list_pobject)

        def merge_kv(tp, level):
            """ inner """
            kvs=[]
            for i in xrange(level):
                kvs.append(tp[0])
                tp = tp[1]
            kvs.append(tp)
            return kvs

        level = pvalue.nested_level() + 1

        transformed = pvalue.apply(merge_value).flatten() \
            .apply(transforms.map, lambda kv: merge_kv(kv, level),
                serde=serde.of(pvalue.key_serdes() + [pvalue.serde()]))

        options['input_fields_num'] = level + 1
        options['is_nested_ptype'] = True
    else:
        transformed = pvalue

    output_fields_num = options.get('output_fields_num', 1)
    if output_fields_num == 1:
        options['serde'] = serde.StrSerde()
    else:
        serdes = [serde.StrSerde()] * output_fields_num
        options['serde'] = serde.TupleSerde(*serdes)

    scale = options.get('scale', 1.0)
    size = options.get('output_size', None)
    memory = options.get('memory_limit', -1)
    cpu = options.get('cpu_limit', -1)

    result_node = transformed.node() \
        .process_by(entity.PipeProcessor(command, **options)) \
        .as_type(options['serde']) \
        .set_debug_info("Pipe: " + repr(command)) \
        .ignore_group() \
        .set_effective_key_num(0) \
        .input(-1).allow_partial_processing().done() \
        .set_size(size, scale) \
        .set_memory(memory) \
        .set_cpu(cpu)

    return pcollection.PCollection(result_node, transformed.pipeline())
