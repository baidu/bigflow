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

import copy

from bigflow import pobject
from bigflow import ptable
from bigflow import pcollection
from bigflow import serde
from bigflow.util import utils


def to_list_pobject(pvalue, **options):
    """
    Transform listing implementation
    :param pvalue: PCollection/PObject
    :return: PObject
    """
    def __initializer(emitter):
        return list()

    def __transformer(status, emitter, record):
        status.append(copy.deepcopy(record))
        return status

    def __finalizer(status, emitter):
        emitter.emit(status)

    if utils.is_infinite(pvalue):
        raise ValueError("to_list_pobject not supported infinite PType")
    elif isinstance(pvalue, pobject.PObject):
        result = pvalue.map(lambda x: [x])
    elif isinstance(pvalue, ptable.PTable):
        raise ValueError("to_list_pobject only applied on PCollections/PObject")
    else:
        result = pvalue.transform(
            __initializer,
            __transformer,
            __finalizer,
            serde=serde.list_of(pvalue.serde()))

    return pobject.PObject(result.node(), result.pipeline())
