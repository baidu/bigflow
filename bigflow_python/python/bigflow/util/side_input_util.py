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
A utility to generate LogicalPlan node for SideInputs

"""

from bigflow import pcollection
from bigflow import pobject
from bigflow import ptable
from bigflow import transforms


class SideInputsUtil:
    """
    A utility to generate LogicalPlan node for SideInputs

    Args:
      input (PType/Node):  the input to be processed
      side_inputs (tuple):  a tuple with PType as SideInputs

    .. note:: End-users are not supposed to use this class.
    """
    def __init__(self, input, side_input_tuple):
        if not isinstance(side_input_tuple, tuple):
            raise ValueError("Invalid arguments: side_input_tuple must be of type tuple")

        # __nodes has first N side input nodes and last one input node
        self.__nodes = []

        for side_input in side_input_tuple:
            if isinstance(side_input, pcollection.PCollection) \
                    or isinstance(side_input, pobject.PObject):
                self.__nodes.append(side_input.node())
            else:
                raise ValueError("Only PCollection/PObject can be used as SideInput")
        from bigflow.core import logical_plan
        if not isinstance(input, logical_plan.LogicalPlan.Node):
            input = input.node()
        self.__nodes.append(input)

    @staticmethod
    def get_dealt_side_inputs_tuple(side_input_tuple):
        side_input_list = []
        for side_input in side_input_tuple:
            if isinstance(side_input, ptable.PTable):
                side_input = transforms.to_pobject(side_input)
            side_input_list.append(side_input)

        return tuple(side_input_list)

    def process_with_side_inputs(self):
        """
        Convert all SideInputs as prepared nodes in LogicalPlan

        Returns:
          LogicalPlan.Node:  processed node
        """
        from bigflow.core import logical_plan
        common_scope = logical_plan.LogicalPlan.Scope.common_scope(self.__nodes)
        plan = self.get_input().plan()

        process = plan.process(common_scope, self.__nodes)
        if process.input_size() != len(self.__nodes):
            raise ValueError("Invalid input size")

        for i in xrange(0, len(self.__nodes) - 1):
            process.input(i).prepare_before_processing()

        return process

    def get_input(self):
        """
        Get the corresponding node of input

        Returns:
          LogicalPlan.Node:  input node
        """
        return self.__nodes[-1]
