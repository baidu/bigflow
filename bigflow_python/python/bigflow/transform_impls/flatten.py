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
from bigflow import ptable
from bigflow import pcollection
from bigflow import serde
from bigflow.core import entity


def flatten(pvalue, **kargs):
    """ Transform flatten implmentation

    :param ptable: PTable
    :return: flattened PCollection
    """

    def _flatten_once(node, key_serde, value_serde):
        return node.process_by(entity.FlattenProcessor(key_serde)) \
                   .as_type(value_serde) \
                   .set_debug_info("FlattenProcessor") \
                   .input(0).allow_partial_processing() \
                   .done() \
                   .set_size(scale_factor=1.25) \
                   .leave_scope()

    if isinstance(pvalue, ptable.PTable):
        key_serdes = pvalue.key_serdes()
        value_serde = pvalue.serde()
        assert len(key_serdes) == pvalue.nested_level() + 1
        it = reversed(key_serdes)
        node = pvalue.node()
        for i in range(0, pvalue.nested_level() + 1):
            key_serde = it.next()
            value_serde = serde.tuple_of(key_serde, value_serde)
            node = _flatten_once(node, key_serde, value_serde)

        pvalue = pcollection.PCollection(node, pvalue.pipeline())

    return pvalue
