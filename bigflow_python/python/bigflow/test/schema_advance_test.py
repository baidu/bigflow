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

class TestSchemaAdvance(test_base.PipelineBasedTest):

    def _load_schema_data_by_parallelize(self):
        raw_data = [
            ["xiaoming", "school_1", 12, 150, 90],
            ["xiaogang", "school_2", 15, 170, 110],
            ["xiaohong", "school_3", 18, 160, 100],
        ]
        data = self._pipeline.parallelize(raw_data)
        data = data.apply(schema.tuple_to_dict,
                          [("name", str),
                           ("school", str),
                           ("age", int),
                           ("height", int),
                           ("weight", int)])
        return data

    def _load_data_by_parallelize(self):
        raw_data = [
            ["xiaoming", "school_1", 12, 150, 90],
            ["xiaogang", "school_2", 15, 170, 110],
            ["xiaohong", "school_3", 18, 160, 100],
        ]
        data = self._pipeline.parallelize(raw_data)
        return data

    def _cmp_result(self, output, expect_keys, expect_values):
        all_values = list()
        for idx, d in enumerate(output):
            keys = sorted(d.keys())
            values = [d.get(k) for k in keys]
            all_values.append(values)
            self.assertEqual(keys, expect_keys)

        self.assertEqual(sorted(all_values), sorted(expect_values))

    def test_select_on_nonschema_pcollection(self):
        """
        非schema的pcollection使用schema.select
        """
        data = self._load_data_by_parallelize()
        data = data.map(lambda t: \
                dict(zip(("name", "school", "age", "height", "weight"), t)))

        from bigflow import serde
        self.assertNotEqual(type(data.serde()), schema.FieldsDictSerde)
        self.assertNotEqual(type(data.serde()), serde.TupleSerde)

        output = schema.select(data, ("name", "age"))

        expect_result = [{"age": 12, "name": "xiaoming"},
                         {"age": 15, "name": "xiaogang"},
                         {"age": 18, "name": "xiaohong"}]

        self.assertEqual(sorted(output.get()),
                         sorted(expect_result))
        self.assertEqual(type(output.serde()), schema.FieldsDictSerde)

    def test_update_on_nonschema_pcollection(self):
        """
        非schema的pcollection使用schema.update
        """
        data = self._load_data_by_parallelize()
        data = data.map(lambda t: \
                dict(zip(("name", "school", "age", "height", "weight"), t)))

        from bigflow import serde
        self.assertNotEqual(type(data.serde()), schema.FieldsDictSerde)
        self.assertNotEqual(type(data.serde()), serde.TupleSerde)

        except_result = [d.update({"name": "My name is %s" % d["name"]}) or d for d in data.get()]
        output = schema.update(data, {"name": lambda name: "My name is %s" % name}).get()
        self.assertEqual(sorted(output), sorted(except_result))

    def test_SchemeTextFile_on_nonschema_pcollection(self):
        """
        非schemo pcollection使用SchemaTextFile进行输出
        """
        data = self._load_data_by_parallelize()
        data = data.map(lambda t: \
                dict(zip(("name", "school", "age", "height", "weight"), t)))

        tmp_output_path = self.generate_tmp_path()
        self._pipeline.write(data,
            output.SchemaTextFile(tmp_output_path, columns = ["name", "age"]))
        self._pipeline.run()

        def _func(p):
            item = p.split("\t")
            return [item[0], int(item[1])]

        pc = self._pipeline.read(input.TextFile(tmp_output_path)).map(_func)
        expect_result = [["xiaoming", 12], ["xiaogang", 15], ["xiaohong", 18]]
        self.passertEqual(expect_result, pc)

    def test_select(self):
        data = self._load_schema_data_by_parallelize()

        # 直接传入一个list或tuple，表示要select的字段
        d1 = schema.select(data, ("name", "age"))
        d2 = schema.select(data, ["name", "age"])
        d3 = schema.select(data, "name,age")

        output = [d1.get(), d2.get(), d3.get()]
        expect_keys = ["age", "name"]
        expect_values = [
            [12, "xiaoming"],
            [15, "xiaogang"],
            [18, "xiaohong"],
        ]
        for ld in output:
            self._cmp_result(ld, expect_keys, expect_values)

        # 指定需要select的字段并对字段执行一段变换逻辑
        fields = {
            # 复用bigflow提供的transforms：传入一个tuple，提供transforms及自定义函数
            "name": (transforms.map, lambda name: "My name is " + name),
            # 提供变化函数：传入一个function
            "school": lambda school: "My school is " + school,
        }
        d4 = schema.select(data, fields)
        output = d4.get()
        expect_keys = ["name", "school"]
        expect_values = [
            ["My name is xiaoming", "My school is school_1"],
            ["My name is xiaogang", "My school is school_2"],
            ["My name is xiaohong", "My school is school_3"],
        ]
        self._cmp_result(output, expect_keys, expect_values)

    def test_update(self):
        data = self._load_schema_data_by_parallelize()

        fields = {
            # 复用bigflow提供的transforms：传入一个tuple，提供transforms及自定义函数
            "name": (transforms.map, lambda name: "My name is "+ name),
            # 提供变化函数：传入一个function
            "school": lambda school: "My school is " + school,
            # 直接设置一个值
            "score": 100,
        }
        d1 = schema.update(data, fields)
        output = d1.get()
        expect_keys = sorted(["name", "age", "school", "score", "height", "weight"])
        expect_values = [
            [12, 150, 'My name is xiaoming', 'My school is school_1', 100, 90],
            [15, 170, 'My name is xiaogang', 'My school is school_2', 100, 110],
            [18, 160, 'My name is xiaohong', 'My school is school_3', 100, 100],
        ]
        self._cmp_result(output, expect_keys, expect_values)

    def test_merge_join_result(self):
        p1 = self._pipeline.parallelize([('a', 2), ('e', 4), ('c', 6)])
        sp1 = p1.apply(schema.tuple_to_dict, ['websites', 'clicknum'])

        p2 = self._pipeline.parallelize([('a', 9), ('b', 8), ('d', 7)])
        sp2 = p2.apply(schema.tuple_to_dict, ['websites', 'click'])

        csp = sp1.apply(schema.join, sp2, fields=['websites'], merge = True)
        output = csp.get()
        expect_keys = sorted(["websites", "click", "clicknum"])
        expect_values = [
            [9, 2, "a"],
        ]
        self._cmp_result(output, expect_keys, expect_values)

        csp = sp1.apply(schema.left_join, sp2, fields=['websites'], merge = True)
        output = csp.get()
        expect_keys = sorted(["websites", "click", "clicknum"])
        expect_values = [
            [None, 4, 'e'],
            [9, 2, 'a'],
            [None, 6, 'c'],
        ]
        self._cmp_result(output, expect_keys, expect_values)

        csp = sp1.apply(schema.right_join, sp2, fields=['websites'], merge = True)
        output = csp.get()
        expect_keys = sorted(["websites", "click", "clicknum"])
        expect_values = [
            [8, None, 'b'],
            [7, None, 'd'],
            [9, 2, 'a'],
        ]
        self._cmp_result(output, expect_keys, expect_values)

        csp = sp1.apply(schema.full_join, sp2, fields=['websites'], merge = True)
        output = csp.get()
        expect_keys = sorted(["websites", "click", "clicknum"])
        expect_values = [
            [None, 4, 'e'],
            [8, None, None],
            [7, None, None],
            [9, 2, 'a'],
            [None, 6, 'c'],
        ]
        self._cmp_result(output, expect_keys, expect_values)


if __name__ == "__main__":
    unittest.main()
