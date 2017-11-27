#
# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.

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

from bigflow.transform_impls import cartesian
from bigflow import pobject


def make_tuple(*pobjects, **options):
    """
        make tuple of pobjects
    """
    for pobj in pobjects:
        assert isinstance(pobj, pobject.PObject)

    result = cartesian.cartesian(*pobjects, **options)
    return pobject.PObject(result.node(), result.pipeline())

