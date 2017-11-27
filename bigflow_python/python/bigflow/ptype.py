#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""
Definition of :class:`.PType`.

.. module:: bigflow.ptype
   :synopsis: PType

Author: Wang, Cong(wangcong09@baidu.com)
"""
import os
from bigflow import error


class PType(object):
    """
    PType is a basic abstraction of data, which represents an immutable, partitioned
    collection of elements that can be operated on paraellel.

    A PType can be either produced by:

    * :func:`bigflow.pipeline.pipeline_base.PipelineBase.read`
    * :func:`bigflow.pipeline.pipeline_base.PipelineBase.parallelize`
    * :mod:`bigflow.transforms` of other PTypes

    A PType can be consumed by:

    * :func:`bigflow.pipeline.pipeline_base.PipelineBase.write`
    * :func:`bigflow.pipeline.pipeline_base.PipelineBase.get`
    * :mod:`bigflow.transforms` of itself

    .. note:: End-users are not supposed to use its raw constructor.

    Args:
      node (Node):  LogicalPlan.Node

    """

    def __init__(self, node, pipeline):
        self._node = node
        self._pipeline = pipeline

    def apply(self, transform, *args, **kargs):
        """
        Apply a transform on itself, p.apply(transform, *args) is equivalent to p.transform(*args)

        Args:
          transform (function):  transform to be applied
          *args:  variable length argument list

        Returns:
          PType:  result of transform
        """
        return transform(self, *args, **kargs)

    def cache(self):
        """
        Persist this PType on storage.
        """
        self._node.cache()
        self._pipeline.add_cache_id(self._node.id())

    def get(self):
        """
        Get runtime value from this PType, equivalent to ``pipeline.get(self)``.

        Returns:
          object:  runtime value
        """
        if not self._is_readable():
            self.cache()
            self._pipeline.run()
            if not self._is_readable():
                raise error.BigflowRuntimeException("Unable to get value from PType")

        return self._get_cached_data()

    def pipeline(self):
        """
        Return the pipeline this PType belongs to

        Returns:
          Pipeline:  pipeline
        """
        return self._pipeline

    def node(self):
        """
        Return the internal node it holds.

        .. note:: End-users are not supposed to use this function.

        Returns:
          LogicalPlan.Node:  The node.
        """
        return self._node

    def serde(self):
        """
        Return the serde of the dataset

        Returns:
          LogicalPlan.Node:  The serde.

        """
        return self.node().serde()

    def _is_readable(self):
        from bigflow.rpc import requests
        return requests.is_node_cached(self._pipeline.id(), self._node.id())

    def _get_cached_data(self):
        from bigflow.rpc import requests
        keys_value = requests.get_cached_data(self._pipeline.id(), self._node.id())

        return self._parse_cached_data(keys_value)

    def _parse_cached_data(self, keys_value):
        pass

    def __repr__(self):
        return "%s at %s" % (self._node, self._pipeline)

    def __str__(self):
        if os.getenv("BIGFLOW_PTYPE_GET_ON_STR") == "true":
            return "%s: %s" % (self.__class__.__name__, self._pipeline.get(self))
        else:
            return repr(self)

    def __getstate__(self):
        """
            let all the ptype can not be pickled, to make sure ptype will not
            be used in lambda. Please use use sideinput for PType
        """
        raise error.InvalidLogicalPlanException("Please use sideinput for PType." +\
            "More info here: http://bigflow.baidu.com/doc/guide.html#sideinputs")

if __name__ == '__main__':
    pass
