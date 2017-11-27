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
Python API异常定义

.. module:: bigflow.error
   :synopsis: all exceptions

"""


class Error(Exception):
    """
    基类

    Args:
      msg (str, optional):  异常信息
      internal (Exception, optional):  导致该异常的内部异常
    """
    def __init__(self, msg=None, internal=None):
        if internal is not None:
            super(Error, self).__init__(msg + ", caused by: " + repr(internal))
        else:
            super(Error, self).__init__(msg)


class BigflowHDFSException(Error):
    """
    访问HDFS抛出的异常
    """
    pass

class BigflowRuntimeException(Error):
    """
    Bigflow运行期抛出异常的基类
    """
    pass


class InvalidDataException(BigflowRuntimeException):
    """
    数据错误所导致的运行期异常
    """
    pass


class BigflowPlanningException(Error):
    """
    Bigflow非运行期抛出异常的基类
    """
    pass


class BigflowRPCException(BigflowPlanningException):
    """
    Bigflow RPC失败抛出的异常

    .. note:: 本异常有可能在今后移除，如有需要，建议捕获其父类
    """


class InvalidLogicalPlanException(BigflowPlanningException):
    """
    Bigflow 执行计划构造抛出的异常
    """
    pass

class InvalidSeqSerdeException(BigflowPlanningException):
    """
    Bigflow 序列化文件传入key和value的serde异常
    """
    pass

class InvalidConfException(BigflowRuntimeException):
    """
    配置错误所导致的运行期异常
    """
    pass


if __name__ == '__main__':
    pass
