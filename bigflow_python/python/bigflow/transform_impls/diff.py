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
# @Last Modified time: 2015-12-17 17:35:15
from bigflow import serde
from bigflow.util import utils


def diff(a, b):
    """
    Implementation of transforms.diff()
    """

    if utils.is_infinite(a) or utils.is_infinite(b):
        raise ValueError("diff not supported infinite PType")

    def filter_count_ne(a, b):
        return a.count() \
            .flat_map(lambda c1, c2: [(c1, c2)], b.count(), serde = serde.of((int, int))) \
            .filter(lambda tp: tp[0] != tp[1])
    a = a.map(lambda x: (x, None), serde = serde.tuple_of(a.serde(), serde.of(int)))
    b = b.map(lambda x: (x, None), serde = serde.tuple_of(b.serde(), serde.of(int)))

    return a.cogroup(b).apply_values(filter_count_ne).flatten()
