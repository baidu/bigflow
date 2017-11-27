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
Pipeline基类定义

"""

from bigflow import error
from bigflow.rpc import requests

class BigflowPipelineWaitStatusTimeoutException(error.Error):
    """
    Bigflow等待指定运行状态超时
    """
    pass

class PipelineStatus(object):
    """
    Pipeline状态
    """

    def __init__(self, pipeline, **options):
        """ inner """
        self._pipeline = pipeline

    def get_status(self):
        """
        获取当前的运行状态

        Returns:
            str:
                APP_SUBMIT/APP_ALLOCATE/APP_RUN/APP_KILL/APP_FAIL/APP_UNKNOWN
                AM_SUBMIT/AM_ALLOCATE/AM_RUN/AM_KILL/AM_FAIL/AM_UNKNOWN
        """
        return requests.get_status(self._pipeline._id, self._pipeline._plan_message)

    def wait_status(self, status, **options):
        """
        等待指定的运行状态

        Args:
            status: 要等待的运行状态
            options:
                timeout - 超时时间，默认1800s
                inspection_cycle - 检测周期，默认60s
        """
        timeout = options.get("timeout", 1800)
        inspection_cycle = options.get("inspection_cycle", 60)

        import time
        timestamp = int(time.time())
        while True:
            current_status = requests.get_status(self._pipeline._id, self._pipeline._plan_message)
            if status == current_status:
                return

            current_timestamp = int(time.time())
            if current_timestamp - timestamp > timeout:
                err_msg = "wait the status[%s] timeout" % (status)
                raise BigflowPipelineWaitStatusTimeoutException(err_msg)

            time.sleep(inspection_cycle)


if __name__ == '__main__':
    pass
