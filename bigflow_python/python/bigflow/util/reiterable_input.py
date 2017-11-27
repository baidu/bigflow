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
A utility to convert a Python iterator to iterable

"""


class ReIterableInput(object):
    """
    A utility to convert a Python iterator to re-iterable object

    Args:
      input_iter (iterator):  input iterator
    """

    def __init__(self, input_iter):
        self.__input = input_iter

    def __iter__(self):
        # re-iterable
        self.__input.reset()
        return self.__input

    def input(self):
        return self.__input
