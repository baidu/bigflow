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
File: select_elements_processor.py
Author: panyunhong(panyunhong@baidu.com)
Date: 2015/05/08 11:49:39
"""

import heapq
from bigflow.transform_impls import processor


def default_select_min_comparator(first_key, second_key):
    return first_key > second_key


class SelectElementsProcessor(processor.AbstractProcessor):
    class __wrapper(object):
        def __init__(self, key, record, comparer):
            self._key = key
            self._record = record
            self._comparer = comparer

        def __lt__(self, wrapper_obj):
            return self._comparer(self._key(self._record), \
                    wrapper_obj._key(wrapper_obj._record))

    def __init__(self, _n, _key, comparer=default_select_min_comparator):
        super(SelectElementsProcessor, self).__init__(_key)
        self.n = _n
        self.heap = []
        self.key = (lambda x: x) if _key is None else _key
        self._comparer = comparer

    def process(self, index, record):
        wrapper_obj = SelectElementsProcessor.__wrapper(self.key, record, self._comparer)
        if len(self.heap) < self.n:
            heapq.heappush(self.heap, wrapper_obj)
        else:
            if self.heap[0] < wrapper_obj:
                heapq.heapreplace(self.heap, wrapper_obj)

    def end(self):
        while len(self.heap) > 0:
            wrapper_obj = heapq.heappop(self.heap)
            self._emitter.emit(wrapper_obj._record)

