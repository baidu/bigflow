#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
#
# Copyright (c) 2016 Baidu, Inc. All Rights Reserved.

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
"""

import unittest

from bigflow import base
from bigflow import input
from bigflow import output
from bigflow import schema
from bigflow import transforms

from bigflow.test import test_base

class TestSchemaSplitStringToTypes(test_base.PipelineBasedTest):
    def test_split_string_to_types(self):
        """
        replace python split string to types function with
        cpp implementation.
        """

        # "|a|b||c|d||e|f||" => ("", "a", "b", "", "c", "d", "", "e", "f", "", "")
        input_path = "./testdata/input_schema_split_string_to_types_2.txt"
        data = self._pipeline.read(input.SchemaTextFile(input_path, columns=11, separator="|"))
        result = data.get()[0]
        except_data = ("", "a", "b", "", "c", "d", "", "e", "f", "", "")
        self.assertEqual(result, except_data)

        # columns is list with fileds name
        input_path = "./testdata/input_schema_split_string_to_types.txt"
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = ["name", "age", "height"], separator = "|"))
        result = data.get()
        except_data = [
                {'age': '15', 'name': 'xiaoming', 'height': '170'},
                {'age': '16', 'name': 'xiaohong', 'height': '175'}
        ]
        self.assertEqual(sorted(result), sorted(except_data))

        # columns is list with fields name and fields type
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = [("name", str), ("age", int), ("height", int)], separator = "|"))
        result = data.get()
        except_data = [
                {'age': 15, 'name': 'xiaoming', 'height': 170},
                {'age': 16, 'name': 'xiaohong', 'height': 175}
        ]
        self.assertEqual(sorted(result), sorted(except_data))

        # columns is integer equal to the count of fields
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = 3, separator = "|"))
        result = data.get()
        except_data = [
                ('xiaoming', '15', '170'),
                ('xiaohong', '16', '175'),
        ]
        self.assertEqual(sorted(result), sorted(except_data))

    def test_split_string_to_types_ignore_overflow(self):
        """
        replace python split string to types function with
        cpp implementation with ignore overflow.
        """
        # columns is list with fileds name
        input_path = "./testdata/input_schema_split_string_to_types_ignore_overflow.txt"
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = ["name", "age"], separator = "|", ignore_overflow = True))
        result = data.get()
        except_data = [
                {'age': '15', 'name': 'xiaoming'},
                {'age': '16', 'name': 'xiaohong'}
        ]
        self.assertEqual(sorted(result), sorted(except_data))

        # columns is list with fields name and fields type
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = [("name", str), ("age", int)], separator = "|",
            ignore_overflow = True))
        result = data.get()
        except_data = [
                {'age': 15, 'name': 'xiaoming'},
                {'age': 16, 'name': 'xiaohong'}
        ]
        self.assertEqual(sorted(result), sorted(except_data))

        # columns is integer equal to the count of fields
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = 2, separator = "|", ignore_overflow = True))
        result = data.get()
        except_data = [
                ('xiaoming', '15'),
                ('xiaohong', '16'),
        ]
        self.assertEqual(sorted(result), sorted(except_data))

    def test_split_string_to_types_ignore_illegal_line(self):
        """
        replace python split string to types function with
        cpp implementation with ignore illegal line.
        """
        # columns is list with fileds name
        input_path = "./testdata/input_schema_split_string_to_types_ignore_illegal_line.txt"
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = ["name", "age", "height"], separator = "|",
            ignore_illegal_line = True))
        result = data.get()
        except_data = [
                {'age': '15', 'name': 'xiaoming', 'height': '170'},
                {'age': '16', 'name': 'xiaohong', 'height': '175'}
        ]
        self.assertEqual(sorted(result), sorted(except_data))

        # columns is list with fields name and fields type
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = [("name", str), ("age", int), ("height", int)], separator = "|",
            ignore_illegal_line = True))
        result = data.get()
        except_data = [
                {'age': 15, 'name': 'xiaoming', 'height': 170},
                {'age': 16, 'name': 'xiaohong', 'height': 175}
        ]
        self.assertEqual(sorted(result), sorted(except_data))

        # columns is integer equal to the count of fields
        data = self._pipeline.read(input.SchemaTextFile(input_path,
            columns = 3, separator = "|", ignore_illegal_line = True))
        result = data.get()
        except_data = [
                ('xiaoming', '15', '170'),
                ('xiaohong', '16', '175'),
        ]
        self.assertEqual(sorted(result), sorted(except_data))


if __name__ == "__main__":
    unittest.main()

