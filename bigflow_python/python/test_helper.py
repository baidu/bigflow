#!/usr/bin/evn python
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
Test helper that discovers and groups test cases(methods).

Author: Ye, Xianjin(bigflow-opensource@baidu.com)
"""

from collections import defaultdict
import fnmatch
import unittest


def suite_to_cases_generator(suite):
    """ Flatten all the test cases(methods) in the suite into a generator

    :param suite: TestSuite, may contain another TestSuite
    :return: Generator
    """
    for test in suite:
        if not isinstance(test, unittest.TestSuite):
            yield test
        else:
            for t in suite_to_cases_generator(test):
                yield t


if __name__ == "__main__":

    import sys
    patterns = sys.argv[1:]
    loader = unittest.TestLoader()
    suite = loader.discover(".", "*test.py")
    cases = list(suite_to_cases_generator(suite))
    # transforms cases to (path/to/module.py, TestCase)
    module_cases = defaultdict(list)
    for case in cases:
        module_cases[case.__module__].append(case.id())

    for m in sorted(module_cases.keys()):
        path = m.replace(".", "/") + ".py"
        for test_method in set(module_cases[m]):
            abbr_test_method = test_method[len(m) + 1:]
            if len(patterns) == 0 or any({fnmatch.fnmatch(path, p) for p in patterns}) \
                    or any({fnmatch.fnmatch(abbr_test_method, p) for p in patterns}):
                print path, abbr_test_method
