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
# @Date:   2015-05-21 17:01:58
# @Last Modified by:   zhangyuncong
# @Last Modified time: 2015-05-22 16:19:30

class DisjointSet(object):
    def __init__(self):
        self._father = {}

    def find(self, a):
        if not self._father.has_key(a) or self._father[a] == a:
            self._father[a] = a
            return a
        else:
            self._father[a] = self.find(self._father[a])
            return self._father[a]

    def union(self, a, b):
        fa = self.find(a)
        fb = self.find(b)
        self._father[fa] = fb
        return self

    def merge(self, other):
        for (child, father) in other._father.items():
            self.union(child, father)
        return self

    def items(self):
        return map(lambda key:(self.find(key), key), self._father)

