#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
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
########################################################################

"""
File: distinct.py
Author: panyunhong(bigflow-opensource@baidu.com)
Date: 2015/03/27 15:05:05
"""

from bigflow import transforms
from bigflow.util import utils


def distinct(ptype, **kargs):
    """
    distinct() implementation
    """

    if utils.is_infinite(ptype):
        raise ValueError("distinct not supported infinite PType")

    distinct_gbk = ptype.group_by(lambda x: x, lambda x: None)
    distinct_gbk.node().set_debug_info("Distinct")
    return distinct_gbk.apply(transforms.extract_keys)
