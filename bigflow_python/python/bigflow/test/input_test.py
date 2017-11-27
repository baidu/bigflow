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
Input/Output unit test

Author: Wang, Cong(wangcong09@baidu.com)
"""

import unittest
import uuid

from bigflow.test import test_base
from bigflow import input
from bigflow import output
from bigflow import error
from bigflow import serde
from bigflow.util.log import logger

class IOTestCase(test_base.PipelineBasedTest):
    """
    Input/Output unit test
    """
    @test_base.run_mode(fs=['local', 'hdfs'])
    def test_text_file(self):
        """
        Case: test text file
        """
        data = self._pipeline.parallelize(['1', '2', '3', '400', '5'])
        local_file = self.generate_tmp_path()
        self._pipeline.write(data, output.TextFile(local_file))
        self._pipeline.run()
        result = self._pipeline.read(input.TextFile(local_file))
        self.assertItemsEqual(['1', '2', '3', '400', '5'], result.get())
        # test read with \0 in the file
        null_data = data.map(lambda x: x + "\0")
        null_file = self.generate_tmp_path()
        self._pipeline.write(null_data, output.TextFile(null_file))
        self._pipeline.run()
        null_read = self._pipeline.read(input.TextFile(null_file))
        self.passertEqual(0, null_read.diff(null_data).count())

    @test_base.run_mode(fs=['local', 'hdfs'])
    def test_text_file_sync(self):
        """
        Case: test text file
        """
        data = self._pipeline.parallelize(['1', '2', '3', '400', '5'])
        local_file = self.generate_tmp_path()
        self._pipeline.write(data, output.TextFile(local_file, async_mode=False))
        self._pipeline.run()
        result = self._pipeline.read(input.TextFile(local_file))
        self.assertItemsEqual(['1', '2', '3', '400', '5'], result.get())

    @test_base.run_mode(fs=['local', 'hdfs'])
    def test_schema_text_file(self):
        """
        Case: test schema text file
        """
        data = self._pipeline.parallelize(['www.baidu.com,3', 'www.sina.com,6'])
        local_file = self.generate_tmp_path()
        self._pipeline.write(data, output.TextFile(local_file))
        self._pipeline.run()
        result_dict_sd = self._pipeline.read(input.SchemaTextFile(local_file,
            columns=[("website", str), ("clicknum", int)], separator=","))
        result_dict = self._pipeline.read(input.SchemaTextFile(local_file,
            columns=["website", "clicknum"], separator=","))
        result_tuple = self._pipeline.read(
            input.SchemaTextFile(local_file, columns=2, separator=","))
        result_tuple_type = self._pipeline.read(
            input.SchemaTextFile(local_file, columns=[str, int], separator=","))

        expect_dict_sd = \
        [{'clicknum': 3, 'website': 'www.baidu.com'},
         {'clicknum': 6, 'website': 'www.sina.com'}]
        self.assertItemsEqual(expect_dict_sd, result_dict_sd.get())

        expect_dict = \
        [{'clicknum': '3', 'website': 'www.baidu.com'},
         {'clicknum': '6', 'website': 'www.sina.com'}]
        self.assertItemsEqual(expect_dict, result_dict.get())

        expect_tuple = \
        [('www.baidu.com', '3'), ('www.sina.com', '6')]
        self.assertItemsEqual(expect_tuple, result_tuple.get())

        expect_tuple_type = \
        [('www.baidu.com', 3), ('www.sina.com', 6)]
        self.assertItemsEqual(expect_tuple_type, result_tuple_type.get())

        self._pipeline.write(result_dict, output.SchemaTextFile(local_file,
            columns=["website", "clicknum"], separator=","))
        self._pipeline.run()
        result_dict = self._pipeline.read(input.SchemaTextFile(local_file,
            columns=["website", "clicknum"], separator=","))
        self.assertItemsEqual(expect_dict, result_dict.get())

        self._pipeline.write(result_dict_sd, output.SchemaTextFile(local_file,
            columns=["website", "clicknum"], separator=","))
        self._pipeline.run()
        result_dict_sd = self._pipeline.read(input.SchemaTextFile(local_file,
            columns=[("website", str), ("clicknum", int)], separator=","))
        self.assertItemsEqual(expect_dict_sd, result_dict_sd.get())

        self._pipeline.write(result_tuple, output.SchemaTextFile(local_file))

        self._pipeline.write(result_tuple, output.SchemaTextFile(local_file))
        self._pipeline.run()
        result_tuple = self._pipeline.read(input.SchemaTextFile(local_file, columns=2))
        self.assertItemsEqual(expect_tuple, result_tuple.get())

        result_tuple_type = result_tuple_type.map(lambda (w, c): (w, c))
        self._pipeline.write(result_tuple_type, output.SchemaTextFile(local_file,
            columns = 2))
        self._pipeline.run()
        result_tuple_type = self._pipeline.read(input.SchemaTextFile(local_file, columns=[str, int]))
        self.assertItemsEqual(expect_tuple_type, result_tuple_type.get())

    @test_base.run_mode(mode = 'local', fs = 'local')
    def test_text_file_sort(self):
        """
        Case: test text file sort
        """
        data = self._pipeline.parallelize([500, 2, 100, 600, 7])
        local_file = self.generate_tmp_path()
        self._pipeline.write(data, output.TextFile(local_file).sort())
        self._pipeline.run()
        data_file = local_file + '/part-00000'
        file = open(data_file)
        self.assertItemsEqual(['2\n', '7\n', '100\n', '500\n', '600\n'], file.readlines())

    @test_base.run_mode(fs=['local', 'hdfs'])
    def test_sequence_file(self):
        """
        Case: test sequence file
        """
        data = self._pipeline.parallelize([1, 2, 3, 400, 5])
        local_file = self.generate_tmp_path()
        self._pipeline.write(data, output.SequenceFile(local_file))
        self._pipeline.run()
        result = self._pipeline.read(input.SequenceFile(local_file))
        self.assertItemsEqual([1, 2, 3, 400, 5], result.get())

    @test_base.run_mode(fs=['local', 'hdfs'])
    def test_sequence_file_sync(self):
        """
        Case: test sequence file
        """
        data = self._pipeline.parallelize([1, 2, 3, 400, 5])
        local_file = self.generate_tmp_path()
        self._pipeline.write(data, output.SequenceFile(local_file, async_mode=False))
        self._pipeline.run()
        result = self._pipeline.read(input.SequenceFile(local_file))
        self.assertItemsEqual([1, 2, 3, 400, 5], result.get())

    @test_base.run_mode('dagmr')
    def test_gz_file(self):
        """
        Case: test text file
        """
        data = self._pipeline.read(input.TextFile('./testdata/part-00001.gz'))
        self.assertEqual(5626, data.count().get())

    @test_base.run_mode(fs=['local', 'hdfs'])
    def test_sequence_file_invalid(self):
        """
        Case: test sequence file invalid
        """
        data = self._pipeline.parallelize(['1', '2', '3', '400', '5'])
        local_file = self.generate_tmp_path()
        self._pipeline.write(data, output.TextFile(local_file))
        self._pipeline.run()
        result = self._pipeline.read(input.SequenceFile(local_file))
        with self.assertRaises(error.BigflowRuntimeException):
            result.get()

    @test_base.run_mode(fs=['local', 'hdfs'])
    def test_sequence_file_serde(self):
        """
        Case: test sequence file serde
        """
        data = self._pipeline.parallelize([1, 2, 3, 400, 5])
        local_file = self.generate_tmp_path()
        self._pipeline.write(data,
                output.SequenceFile(local_file, serde = serde.IntSerde()))
        self._pipeline.run()
        result = self._pipeline.read(input.SequenceFile(local_file, serde = serde.IntSerde()))
        self.assertItemsEqual([1, 2, 3, 400, 5], result.get())
        result_invalid = self._pipeline.read(
                input.SequenceFile(local_file, serde = serde.TupleSerde()))
        with self.assertRaises(error.BigflowRuntimeException):
            result_invalid.get()

    def wildcard_case(self):
        """
        Case: test wildcard
        """
        input_data = [['1', '2', '3'], ['400', '5'], ['401', '501']]
        index = 0
        root_path = self.generate_tmp_path()
        for tmp_data in input_data:
            data = self._pipeline.parallelize(tmp_data)
            path = root_path + '/X' + str(index)
            self._pipeline.write(data, output.TextFile(path))
            index = index + 1
        self._pipeline.run()
        match_path = root_path + '/*'
        result = self._pipeline.read(input.TextFile(match_path))
        self.assertItemsEqual(['401', '501', '1', '2', '3', '400', '5'], result.get())

    @test_base.run_mode(mode=['dagmr', 'spark'])
    def test_wildcard_case1(self):
        """
            Case: test wildcard on hadoop and spark (support all filesystem)
        """
        self.wildcard_case()

    @test_base.run_mode('local', fs = ['local'])
    def test_wildcard_case2(self):
        """
        Case: test wildcard on local. (local mode only support wildcard for local path)
        """
        self.wildcard_case()


    def test_partitioned(self):
        """
        test
        """
        testdata = self._pipeline.read(input.TextFile(
                'testdata/part-00000',
                'testdata/part-00001',
                partitioned=True))
        expect = {'x': ['1', '2', '3'], 'y': ['4', '5', '6', '7']}
        table = self._pipeline.parallelize(expect)
        self.assertEqual(repr(table), repr(testdata))
        self.assertItemsEqual(table.flatten_values().get(), testdata.flatten_values().get())

    @test_base.run_mode('dagmr', fs = ['hdfs'])
    def test_commit(self):
        """
        Case: test commit
        """
        self.setConfig(immediately_commit=True)

        vertex0 = self._pipeline.parallelize([1, 1, 2, 2, 3, 3, 4, 4, 5, 5])

        vertex1 = vertex0.group_by(lambda x: x) \
                .apply_values(lambda x: x.reduce(lambda x, y: x + y)) \
                .flatten()

        vertex1_output = self.generate_tmp_path()
        self._pipeline.write(vertex1, output.SequenceFile(vertex1_output))

        def _initializer(emitter):
            return []

        def _transformer(status, emitter, inp):
            import copy
            status.append(copy.deepcopy(inp))
            return status

        def _finalizer(status, emitter):
            emitter.emit(len(status) / 0)

        vertex2 = vertex1.group_by(lambda x: x[0] % 2, lambda x: x[1]) \
                .apply_values(lambda x: x.transform(_initializer, _transformer, _finalizer)) \
                .flatten()

        vertex2_output = self.generate_tmp_path()
        self._pipeline.write(vertex2, output.SequenceFile(vertex2_output))
        with self.assertRaises(ZeroDivisionError):
            self._pipeline.run()

        from bigflow import base
        local_pipeline = base.Pipeline.create('local', hadoop_config_path = self._hadoop_config_path)
        result = local_pipeline.read(input.SequenceFile(vertex1_output))
        self.assertItemsEqual([(1, 2), (2, 4), (3, 6), (4, 8), (5, 10)], result.get())


if __name__ == "__main__":
    unittest.main()

