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
A utility class to convert PType class name to a string

"""


class PTypeInfo(object):
    """
    A utility to keep class type of a PType and store its class name in its ``type`` attribute

    Args:
      pvalue (PType):  PType instance

    >>> _pobject = _pipeline.parallelize("A")
    >>> print PTypeInfo(_pobject)
    >>> "PObject"
    """

    def __init__(self, pvalue):
        from bigflow import pcollection
        from bigflow import pobject

        if isinstance(pvalue, pobject.PObject):
            self.type = "PObject"
        elif isinstance(pvalue, pcollection.PCollection):
            self.type = "PCollection"
        else:
            self.type = "Runtype"
