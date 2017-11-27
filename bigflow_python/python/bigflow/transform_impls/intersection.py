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
# @Last Modified time: 2017-02-06 17:14:09

from bigflow.util import utils

def intersection(a, b, output_duplicated = False):

    if utils.is_infinite(a) or utils.is_infinite(b):
        raise ValueError("intersectio not supported infinite PType")

    def filter_if_neither_empty(a, b):
        return a.is_empty() \
                .flat_map(lambda x:[x]) \
                .filter(lambda a_empty, b_empty: not a_empty and not b_empty, b.is_empty())
    a = a.map(lambda x: (x, None))
    b = b.map(lambda x: (x, None))

    if output_duplicated:
        filter_rule = lambda a, b: a.take(b.count())
    else:
        filter_rule = filter_if_neither_empty
    return a.cogroup(b).apply_values(filter_rule).flatten().map(lambda kv: kv[0])
