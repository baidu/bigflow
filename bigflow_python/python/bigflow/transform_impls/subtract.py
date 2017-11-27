#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @Author: zhangyuncong
# @Date:   2015-06-30 20:22:17
# @Last Modified by:   zhangyuncong
# @Last Modified time: 2016-04-18 19:46:09

from bigflow.util import utils

def subtract(a, b):

    if utils.is_infinite(a) or utils.is_infinite(b):
        raise ValueError("subtract not supported infinite PType")

    def filter_if_b_is_empty(a, b):
        return a.filter(lambda input, is_empty: is_empty, b.is_empty())
    a = a.map(lambda x: (x, None))
    b = b.map(lambda x: (x, None))
    return a.cogroup(b).apply_values(filter_if_b_is_empty).flatten().map(lambda kv:kv[0])
