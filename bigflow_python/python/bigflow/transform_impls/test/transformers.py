#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
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
########################################################################

"""
File: transformers.py
Date: 2017/10/26

Transfomers for test

"""

from bigflow import base


class PartialSumTransformer(base.Transformer):

    def begin_process(self):
        self._sum = 0
        return []

    def process(self, record):
        self._sum += record
        yield self._sum


class ZipTransformer(base.Transformer):

    def begin_process(self, *si):
        self.index = 0
        lens = map(len, si)
        self.min_len = min(lens)
        return []

    def process(self, inp, *si):
        if self.index < self.min_len:
            yield (inp, ) + tuple(map(lambda x: x[self.index], si))
        self.index += 1


class FakeOutputTransformer(base.Transformer):

    def __init__(self, fake_output):
        self._fake_output = fake_output

    def begin_process(self):
        return self._fake_output


class SumTransformer(base.Transformer):

    def begin_process(self):
        self._sum = 0
        return []

    def process(self, record):
        self._sum += record
        return []

    def end_process(self):
        yield self._sum
