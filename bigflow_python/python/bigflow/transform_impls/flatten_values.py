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
import logging

from bigflow import pcollection
from bigflow import ptable
from bigflow import transforms


_logger = logging.getLogger("Bigflow-" + __name__)


def flatten_values(pvalue):
    """ Transform flatten implmentation

    :param pvalue: pvalue
    :return: flattened PCollection
    """
    if isinstance(pvalue, ptable.PTable):
        node = pvalue.node().leave_scope()
        for i in range(0, pvalue.nested_level()):
            node = node.leave_scope()

        return pcollection.PCollection(node, pvalue.pipeline())

    return pvalue
