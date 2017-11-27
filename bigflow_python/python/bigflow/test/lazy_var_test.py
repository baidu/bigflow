# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# coding: utf-8

"""
lazy var unit test
"""

import unittest

from bigflow import lazy_var
from bigflow import base
from bigflow.test import test_base
from bigflow.core.serde import cloudpickle


class TestLazyVar(test_base.PipelineBasedTest):
    """
    lazy_var unit test
    """

    def test_compare_lazy_var_and_simple_var(self):

        def _load_dict():
            return {str(x): str(x) for x in xrange(100)}

        my_lazy_var = lazy_var.declare(_load_dict)

        def _use_dict_by_lazy_var():
            my_dict = my_lazy_var.get()
            values = '\t'.join(my_dict.values())
            return values

        dump_object_before_call_get = cloudpickle.dumps(_use_dict_by_lazy_var)
        my_lazy_var.get()
        dump_object_after_call_get = cloudpickle.dumps(_use_dict_by_lazy_var)
        # assert equal
        self.assertEqual(dump_object_before_call_get, dump_object_after_call_get)

        my_simple_var = {}

        def _load_dict_to_simple_var():
            for x in xrange(100):
                my_simple_var[str(x)] = str(x)
            return my_simple_var

        def _use_dict_by_simple_var():
            values = '\t'.join(my_simple_var.values())
            return values

        dump_object_before_load_dict = cloudpickle.dumps(_use_dict_by_simple_var)
        _load_dict_to_simple_var()
        dump_object_after_load_dict = cloudpickle.dumps(_use_dict_by_simple_var)
        # assert not equal
        self.assertNotEqual(dump_object_before_load_dict, dump_object_after_load_dict)

    def test_use_lazy_var_in_local_and_transforms(self):
        local_keys = ["1", "2", "3", "4", "50000", "60000"]
        keys = self._pipeline.parallelize(local_keys)

        def _load_dict():
            return {str(x): str(x) for x in xrange(100)}

        my_lazy_var = lazy_var.declare(_load_dict)

        def _get_value(key):
            my_dict = my_lazy_var.get()
            return my_dict.get(key)

        # you can call lazy_var `get` method in local
        local_values = map(lambda k: _get_value(k), local_keys)
        self.assertEqual(sorted(local_values), sorted(["1", "2", "3", "4", None, None]))

        # also call it in transforms
        values = keys.map(_get_value)
        py_values = values.get()
        self.assertEqual(sorted(py_values), sorted(["1", "2", "3", "4", None, None]))


if __name__ == "__main__":
    unittest.main()
