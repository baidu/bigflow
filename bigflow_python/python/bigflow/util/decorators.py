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
Decorators used by Bigflow Python API

"""
import inspect
import time
from functools import wraps

from bigflow import error


def advance_logger(loglevel):
    """
    A decorator that log performance infos of a function call at runtime

    Args:
      loglevel (str):  "DEBUG" or "INFO"(case ignored), log level
    """

    def _get_line_number():
        return inspect.currentframe().f_back.f_back.f_lineno

    def _basic_log(fn, result, *args, **kwargs):
        print "function   = " + fn.__name__,
        print "    arguments = {0} {1}".format(args, kwargs)
        print "    return    = {0}".format(result)

    def _info_log_decorator(fn):
        @wraps(fn)
        def _wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            _basic_log(fn, result, args, kwargs)
        return _wrapper

    def _debug_log_decorator(fn):
        @wraps(fn)
        def _wrapper(*args, **kwargs):
            ts = time.time()
            result = fn(*args, **kwargs)
            te = time.time()
            _basic_log(fn, result, args, kwargs)
            print "    time      = %.6f sec" % (te-ts)
            print "    called_from_line : " + str(_get_line_number())
        return _wrapper

    if loglevel.lower == "debug":
        return _debug_log_decorator
    else:
        return _info_log_decorator


def singleton(clazz, *args, **kw):
    """
    A decorator that makes a class singleton

    Args:
      clazz (class):  class to decorate
    """
    instances = {}

    def _singleton():
        if clazz not in instances:
            instances[clazz] = clazz(*args, **kw)
        return instances[clazz]
    return _singleton

