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
File: first.py
Author: panyunhong(bigflow-opensource@baidu.com)
Date: 2015/03/14 10:41:58
"""

import bigflow.pcollection
import bigflow.pobject
from bigflow.util import utils


def first(ptype):
    """
    Implementation of first
    """
    if utils.is_infinite(ptype):
        raise ValueError("first not supported infinite PType")
    return bigflow.pobject.PObject(ptype.take(1).node(), ptype.pipeline())
