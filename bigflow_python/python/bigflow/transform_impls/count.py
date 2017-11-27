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
from bigflow import serde
from bigflow.util import utils


def count(ptype, **options):
    """
    count() implementation
    """

    if utils.is_infinite(ptype):
        raise ValueError("count not supported infinite PType")

    scale = options.get('scale', 0.1)
    size = options.get('output_size', None)
    memory = options.get('memory_limit', -1)
    cpu = options.get('cpu_limit', -1)

    return ptype.aggregate(
        0,
        aggregate_fn=lambda x, y: x + 1,
        combine_fn=lambda x, y: x + y,
        serde=serde.IntSerde(),
        scale=scale,
        output_size=size,
        memory_limit=memory,
        cpu_limit=cpu)
