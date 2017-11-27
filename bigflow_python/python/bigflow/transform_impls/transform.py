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

from bigflow.util import side_input_util
from bigflow.core import entity


def _transform_with_fns(pvalue, initializer, transformer, finalizer, *side_inputs, **kargs):
    """
    pcollection transform
    """

    objector = kargs.get('serde', pvalue.pipeline().default_objector())
    status_objector = kargs.get('status_serde', pvalue.pipeline().default_objector())
    debug_info = "transform" + repr((initializer, transformer, finalizer))
    side_inputs = side_input_util.SideInputsUtil.get_dealt_side_inputs_tuple(side_inputs)
    helper = side_input_util.SideInputsUtil(pvalue, side_inputs)
    pnode = helper.process_with_side_inputs()
    initializer = entity.Functor.of(initializer)
    transformer = entity.Functor.of(transformer)
    finalizer = entity.Functor.of(finalizer)

    result_node = pnode \
        .by(entity.TransformProcessor(status_objector, initializer, transformer, finalizer)
                .set_side_inputs(*side_inputs)) \
        .as_type(objector) \
        .is_stateful() \
        .set_effective_key_num(0) \
        .set_debug_info(debug_info) \
        .set_size(kargs.get('output_size', None), kargs.get('scale', 0.1)) \
        .set_memory(kargs.get('memory_limit', -1)) \
        .set_cpu(kargs.get('cpu_limit', -1))

    return pcollection.PCollection(result_node, pvalue.pipeline())


def _transform_with_transformer(pvalue, transformer, *side_inputs, **options):

    from bigflow import base
    assert(isinstance(transformer, base.Transformer))

    def _begin_process(emitter, *side_input_data):
        the_transformer = transformer
        for out_record in the_transformer.begin_process(*side_input_data):
            emitter.emit(out_record)
        return the_transformer

    def _process(status, emitter, record, *side_input_data):
        assert(isinstance(status, base.Transformer))
        the_transformer = status
        for out_record in the_transformer.process(record, *side_input_data):
            emitter.emit(out_record)
        return the_transformer

    def _end_process(status, emitter, *side_input_data):
        the_transformer = status
        for out_record in the_transformer.end_process(*side_input_data):
            emitter.emit(out_record)

    from bigflow import serde
    # DefaultSerde can not serialize transformer, so we use CPickleSerde by default.
    transformer_serde = options.get('transformer_serde', serde.CPickleSerde())
    options = options.copy()
    options.update({"status_serde": transformer_serde})
    return _transform_with_fns(pvalue, _begin_process, _process, _end_process,
                               *side_inputs, **options)


def transform(pvalue, first_arg, *args, **kargs):
    from bigflow import base
    if isinstance(first_arg, base.Transformer):
        return _transform_with_transformer(pvalue, first_arg, *args, **kargs)
    else:
        return _transform_with_fns(pvalue, first_arg, *args, **kargs)
