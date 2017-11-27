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
A transform that union PCollections/PObjects`

"""

from bigflow import pcollection
from bigflow import pobject
from bigflow import error
from bigflow import serde


def union(*pvalues, **options):
    """ inner function"""
    if len(pvalues) == 0:
        raise ValueError("No argument")

    if not all(isinstance(p, pcollection.PCollection) or isinstance(p, pobject.PObject)
               for p in pvalues):
        raise ValueError("Union only applied on PCollections or PObjects")

    serdes = [p.serde() for p in pvalues]
    com_serde = options.get("serde", serde.common_serde(*serdes))
    if com_serde:
        def _inner_map(p):
            """use com_serde to convert"""
            if p.serde().__class__ != com_serde.__class__:
                p = p.map(lambda x:x, serde=com_serde)
            return p
        pvalues = map(_inner_map, pvalues)

    common_scope = pvalues[0].node().scope()
    all_nodes = map(lambda p: p.node(), pvalues)
    if not all(node.scope() is common_scope for node in all_nodes):
        raise ValueError("PCollections to union should work on same scope only")

    plan = pvalues[0].node().plan()
    return pcollection.PCollection(plan.union(nodes=all_nodes), pvalues[0].pipeline())
