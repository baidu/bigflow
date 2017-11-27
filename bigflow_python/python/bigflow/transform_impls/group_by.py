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
from bigflow.core import entity
from bigflow.core.serde import omnitypes_objector
from bigflow.transform_impls import processor


class KeyReader(object):
    def __init__(self, key_extractor, key_serde):
        self.objector = entity.Entity.of(entity.Entity.objector, key_serde) \
            .to_proto_message().SerializeToString()
        self.read_key = key_extractor


def node_group_by(
        node,
        key_extractor,
        value_extractor,
        key_serde,
        value_serde,
        concurrency=None,
        pipeline=None):
    """
    group by fn, for internal use
    """
    plan = node.plan()
    scope = node.scope()

    shuffle = plan.shuffle(scope, [node])

    if concurrency is not None:
        shuffle.with_concurrency(concurrency)
    elif pipeline is not None and pipeline.estimate_concurrency:
        concurrency = node.size() / pipeline.size_per_concurrency
        shuffle.with_concurrency(concurrency)

    shuffle_node = shuffle.node(0).match_by(KeyReader(key_extractor, key_serde))

    process_node = None
    if value_extractor is None:
        process_node = shuffle_node.set_debug_info("ExtractValue")
    else:
        process_node = shuffle_node.process_by(entity.ValueProcessor(value_extractor)) \
            .as_type(value_serde) \
            .set_debug_info("ExtractValue") \
            .ignore_group() \
            .input(0).allow_partial_processing() \
            .done()

    return process_node


def group_by(pvalue, key_extractor, value_extractor, **options):
    """
    only the tuple pair elements of pvalue accepted
    """

    key_serde = options.get('key_serde', pvalue.pipeline().default_objector())
    if value_extractor is None:
        value_serde = options.get('value_serde', pvalue.serde())
    else:
        value_serde = options.get('value_serde', pvalue.pipeline().default_objector())

    pipeline = pvalue.pipeline()

    node = node_group_by(
            pvalue.node(),
            key_extractor,
            value_extractor,
            key_serde,
            value_serde,
            options.get('concurrency', None),
            pipeline)
    return ptable.PTable(pcollection.PCollection(node, pipeline), key_serde=key_serde)
