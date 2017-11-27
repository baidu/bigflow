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

import math


from bigflow import pcollection
from bigflow import transforms
from bigflow.core import entity
from bigflow.transform_impls import select_elements_impl


def SelectMaxComparer(first_key, second_key):
    return first_key < second_key


def max_elements(pvalue, n, key=None, **options):
    """
    Implementation of transforms.max_elements
    """

    return select_elements_impl.select_elements(pvalue, n, key, isMax=True, **options)
