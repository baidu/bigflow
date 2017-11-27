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
A utility module containing some helper method for broadcasting mechnism

.. note:: End-users are not supposed to use the functions.

Author: Wang, Cong(wangcong09@baidu.com)
"""

from bigflow import pcollection
from bigflow import ptable
from bigflow import ptype
from bigflow import pobject
from bigflow.util import utils


def is_same_working_scope(v1, v2):
    """
    Judge if v1 and v2 works on the same scope.
    When both of them are PType but their working scopes are different, return False; otherwise
    returns true.

    Args:
      v1 (object):  object 1
      v2 (object):  object 2

    Returns:
      bool:  if v1 and v2 work on the same scope
    """
    if (not isinstance(v1, ptype.PType)) or (not isinstance(v2, ptype.PType)):
        return True

    return working_scope(v1) == working_scope(v2)


def working_scope(pvalue):
    """
    Returns the working scope of a given PType instance

    Args:
      pvalue (PType):  PType instance

    Returns:
      LogicalPlan.Scope:  working scope

    Raises:
      ValueError:  if invalid argument is given
    """
    if isinstance(pvalue, tuple) and isinstance(pvalue[0], ptype.PType):
        return working_scope(pvalue[0])

    if not isinstance(pvalue, ptype.PType):
        raise ValueError("Invalid argument: pvalue(%s) must be of type PType only" % type(pvalue))

    if isinstance(pvalue, pcollection.PCollection) or isinstance(pvalue, pobject.PObject):
        return pvalue.node().scope()

    if isinstance(pvalue, ptable.PTable):
        if not isinstance(pvalue._value(), ptype.PType):
            return working_scope(pvalue._value()[0]).father()
        else:
            return working_scope(pvalue._value()).father()


def broadcast_to(pvalue, scope):
    """
    Broadcast given PType instance to given scope

    Args:
      pvalue (PType):  PType instance
      scope (LogicalPlan.Scope):  scope

    Returns:
      PType:  new PType after broadcast
    """
    if not isinstance(pvalue, ptype.PType):
        return pvalue

    if isinstance(pvalue, ptable.PTable):
        flattned = pvalue.flatten()

        node = flattned.node()
        plan = node.plan()

        broadcasted = pcollection.PCollection(plan.broadcast_to(node, scope), pvalue.pipeline())

        broadcasted = utils.construct(pvalue.pipeline(),
                                      broadcasted.node(),
                                      ptable.PTable,
                                      pvalue.nested_level(),
                                      pvalue.inner_most_type())

        return broadcasted

    # else:
    node = pvalue.node()
    plan = node.plan()
    broadcasted_node = plan.broadcast_to(node, scope)

    if isinstance(pvalue, pcollection.PCollection):
        return pcollection.PCollection(broadcasted_node, pvalue.pipeline())
    else:
        return pobject.PObject(broadcasted_node, pvalue.pipeline())
