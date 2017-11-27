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
Script definition

File: subprocess.py
Author: Wang, Cong(wangcong09@baidu.com)
"""

import subprocess
import threading


class Subprocess(object):
    def __init__(self, cmd):
        self._cmd = cmd
        self._out_listener = []
        self._err_listener = []
        self._err_thread = None
        self._out_thread = None

    def handle_stdout(self, captured):
        for line in iter(captured.readline, b''):

            for listener in self._out_listener:
                listener(line)

    def handle_stderr(self, captured):
        for line in iter(captured.readline, b''):

            for listener in self._err_listener:
                listener(line)

    def add_stdout_listener(self, fn):
        self._out_listener.append(fn)
        return self

    def add_stderr_listener(self, fn):
        self._err_listener.append(fn)
        return self

    def open(self):
        process = subprocess.Popen(
            self._cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1)
        thread_out = threading.Thread(target=self.handle_stdout, args=(process.stdout, ))
        thread_out.daemon = True
        thread_out.start()
        self._out_thread = thread_out

        thread_err = threading.Thread(target=self.handle_stderr, args=(process.stderr, ))
        thread_err.daemon = True
        thread_err.start()
        self._err_thread = thread_err

        return process

    def join_all_reading_threads(self):
        self._out_thread.join()
        self._err_thread.join()


if __name__ == '__main__':
    pass
