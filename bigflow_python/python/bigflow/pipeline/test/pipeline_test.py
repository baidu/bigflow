#!/usr/bin/env pyrun
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
Author: Zhang Yuncong(bigflow-opensource@baidu.com)
"""
import cPickle
import datetime
import os
import sys
import shutil
import unittest

from bigflow import error
from bigflow import input
from bigflow import output
from bigflow import serde
from bigflow import transforms
from bigflow.schema import FieldsDictSerde
from bigflow.test import test_base, magic


serde.USE_DEFAULT_SERDE=False


def to_pobject_list(pcollection):
    """
    convert result to a pobject list
    """
    return pcollection.aggregate([], lambda lst, elem: lst + [elem], lambda a, b: a + b)


def calculate_connected_component(edges):
    """
        edges : [(p1, p2), (p3, p4), (p2, p5),...]
        return: [ [p1, p2, p5], [p3, p4] ]
    """
    def _zero_func():
        import disjointset
        return disjointset.DisjointSet()

    return edges.aggregate(_zero_func,
                           lambda djset, edge: djset.union(edge[0], edge[1]),
                           lambda set1, set2: set1.merge(set2),
                           serde=serde.CPickleSerde()) \
                .flat_map(lambda djset: djset.items()) \
                .group_by_key() \
                .apply_values(to_pobject_list) \
                .flatten_values()


class TestCase(test_base.PipelineBasedTest):
    def test_normal(self):
        """
        Test simple case
        """
        p = self._pipeline.parallelize([1, 9, 6, 2])
        filtered = p.filter(lambda x: x % 2 == 0)

        p_result = self._pipeline.get(filtered)

        self.assertItemsEqual([6, 2], p_result)

    def test_multi_input(self):
        """
        Test multi input
        """
        p1 = self._pipeline.parallelize([1, 2, 3, 5])
        p2 = self._pipeline.parallelize([2, 4, 6, 10])

        p_result = self._pipeline.get(p1)
        self.assertItemsEqual([1, 2, 3, 5], p_result)

    def test_side_input(self):
        """
        Test sideinput in apply/apply_values
        """
        p = self._pipeline.parallelize([1, 9, 6, 2])
        two = self._pipeline.parallelize(2)
        filtered = p.filter(lambda x, y: x % y == 0, two)
        filtered.cache()
        self.assertItemsEqual([3, 11, 8, 4], two.map(lambda x, y: [x + item for item in y], p).get())
        p_result = self._pipeline.get(filtered)
        self.assertItemsEqual([6, 2], p_result)

    def test_in_apply_values(self):
        """
        Test apply_values
        """

        def if_in_set(x, y):
            """
            Simple function
            """
            return x in y

        p = self._pipeline.parallelize({"A": [1, 9, 6, 2], "B": [2, 3, 1, 5]})
        tdict = self._pipeline.parallelize([2, 6])
        filtered = p.apply_values(transforms.filter, if_in_set, tdict)

        p_result = self._pipeline.get(filtered)

        for k, v in p_result.items():
            v.sort()
        self.assertEqual({"A": [2, 6], "B": [2]}, p_result)

    def test_overwrite(self):
        """
        Test pipeline overwrite target path
        """
        p = self._pipeline.parallelize([1])
        self._pipeline.write(p, output.SequenceFile('test_output'))
        self._pipeline.run()
        self._pipeline.write(p, output.SequenceFile('test_output'))
        self._pipeline.run()
        p1 = self._pipeline.read(input.SequenceFile('test_output'))
        p.cache()
        p1.cache()
        self.assertEqual(p.get(), p1.get())
        shutil.rmtree('test_output')
        self.assertEqual(p.get(), p1.map(lambda x: x).get())
        self.assertFalse(os.path.exists('test_output'))

    def test_add_file(self):
        """
        Test add_file function
        """
        self._pipeline.add_file('./testdata/disjointset.py', './UBX_CC/5016')
        _ = self._pipeline.parallelize([1, 3])
        files = _.map(lambda x: os.listdir("./UBX_CC")[0])
        files.cache()

        self._pipeline.add_file('./testdata/disjointset.py', './IM!220/CK19', executable=True)
        _ = self._pipeline.parallelize(1)

        def if_is_writable(record):
            # CK19 is executable for the owner
            return (os.stat("./IM!220/CK19").st_mode & 0100) > 0

        is_writable = _.map(if_is_writable)
        is_writable.cache()

        self._pipeline.add_file('./testdata/disjointset.py', './disjointset.py')
        edges = self._pipeline.parallelize(
            [(1, 2), (2, 3), (3, 4), (7, 8), (9, 10), (1, 7), (10, 11)])
        connected_component = edges.apply(calculate_connected_component)
        lists = []
        for points in connected_component.get():
            lists.append(list(set(points)))
        self.assertItemsEqual([[1, 2, 3, 4, 7, 8], [9, 10, 11]], lists)
        self.assertItemsEqual(['5016', '5016'], files.get())
        self.assertEqual(True, is_writable.get())

    @test_base.run_mode(mode="local")
    def test_add_egg_file(self):

        def _remote_map(x):
            import columns
            return x

        self._pipeline.add_egg_file("testdata/columns/dist/columns-0.1.0.0-py2.7.egg")
        result = self._pipeline.parallelize([1, 2, 3, 4, 5]) \
                    .map(_remote_map)

        self.passertEqual([1, 2, 3, 4, 5], result)

    def test_add_directory(self):
        """
        Test add_directory function
        """
        def test_import(inp):
            """
            Simple test utilitiy
            """
            import testdata.disjointset

            return inp

        self._pipeline.add_directory('./testdata', './testdata', True)
        self.assertEqual(1, self._pipeline.parallelize(1).map(test_import).get())

    def test_add_directory_with_hidden_path(self):
        """
        Test add_directory function
        """
        def test_import(inp):
            """
            Simple test utilitiy
            """
            import testdata2.disjointset
            return inp

        os.system('mkdir -p ./.tmp/; cp -r ./testdata ./.tmp/')
        self._pipeline.add_directory('./.tmp/testdata', './testdata2', True)
        self.assertEqual(1, self._pipeline.parallelize(1).map(test_import).get())

    def test_cache(self):
        """ inner """
        f = open('lines.txt', 'w')
        f.writelines(['1 2 3 1 2 3', ' 1 2 3 4 5 6'])
        f.write('\n')
        f.close()
        lines = self._pipeline.read(input.TextFile('lines.txt'))

        def wordcount(plist):
            """ inner """
            return plist.group_by(lambda whole: whole) \
                .apply_values(transforms.count)

        wordcnt = lines.flat_map(lambda line: line.split()) \
            .group_by(lambda whole: whole) \
            .apply_values(wordcount)

        wordcnt.cache()
        expected = {
            "1": {"1": 3},
            "2": {"2": 3},
            "3": {"3": 3},
            "4": {"4": 1},
            "5": {"5": 1},
            "6": {"6": 1}
        }
        self.assertEqual(expected, wordcnt.get())

        os.system("rm -rf lines.txt")

        flattend = wordcnt.flatten()
        flattened_values = wordcnt.flatten_values()

        flattened_values.cache()
        self.assertItemsEqual([1, 1, 1, 3, 3, 3], flattened_values.get())

    def test_seqfile(self):
        """
        Test sequencefile io
        """
        tmp_file = "./.tmp/test_tmp"

        key1 = 123
        value1 = ["A", "B", "C"]

        key2 = 456
        value2 = ["D", "E", "F"]

        input_data = [(key1, value1), (key2, value2)]

        def kv_serializer(record):
            return str(record[0]), ":".join(record[1])

        pcollection_kv = self._pipeline.parallelize(input_data)
        self._pipeline.write(
            pcollection_kv,
            output.SequenceFile(tmp_file).as_type(kv_serializer))

        self._pipeline.run()

        def kv_deserializer(tp):
            return int(tp[0]), tp[1].split(":")

        result = self._pipeline.read(input.SequenceFile(tmp_file).as_type(kv_deserializer))
        result_data = result.get()

        self.assertItemsEqual(input_data, result_data)

    def test_seq_file_new_api(self):
        """
        test sequence file new api
        """
        import os
        class KeySerde(serde.Serde):
            """value serde"""

            def serialize(self, obj):
                """serialize"""
                return str(obj + 1)

            def deserialize(self, buf):
                """deserialize"""
                return int(buf) - 1

        class ValueSerde(serde.Serde):
            """value serde"""

            def serialize(self, obj):
                """serialize"""
                return str(obj * 2)

            def deserialize(self, buf):
                """deserialize"""
                return int(buf) / 2

        tmp_file = "./.tmp/test_file_1"
        os.system("rm " + tmp_file + " -rf")
        input_data = [(2, 2), (1, 6)]
        d = self._pipeline.parallelize(input_data)
        self._pipeline.write(d, output.SequenceFile(tmp_file,\
            key_serde=KeySerde(), value_serde=ValueSerde()))
        self._pipeline.run()

        read_data = self._pipeline.read(input.SequenceFile(tmp_file,\
            key_serde=KeySerde(), value_serde=ValueSerde()))
        result_data = read_data.get()
        self.assertItemsEqual(input_data, result_data)

    def test_partition(self):
        """
        Test partition output
        """

        import os
        try:
            p = self._pipeline.parallelize(["1", "2", "3"])
            self._pipeline.write(p, output.TextFile('./output-1').partition(5))
            self._pipeline.write(
                    p,
                    output.SequenceFile('./output-2').partition(2, lambda x, n : int(x) % n))
            self._pipeline.run()

            o1 = self._pipeline.read(input.SequenceFile('./output-2/part-00000'))
            o1.cache()
            o2 = self._pipeline.read(input.SequenceFile('./output-2/part-00001'))
            o2.cache()
            self.assertEqual(["2"], o1.get())
            self.assertItemsEqual(["1", "3"], o2.get())

            n = os.popen('ls output-1/[^_]* | wc -l').read()
            self.assertEqual(5, int(n))
            o = self._pipeline.read(input.TextFile('output-1')).get()
            self.assertItemsEqual(["1", "2", "3"], o)
        finally:
            os.system("rm output-1 output-2 -r")

    def test_gzip_file(self):
        """
        Test read/write gzip files
        """
        import os
        try:
            mem_testdata = ['1', '2', '3']
            p = self._pipeline.parallelize(mem_testdata)
            target = output.TextFile('output-gzip').with_compression("gzip").partition(2)
            self._pipeline.write(p, target)
            self._pipeline.run()
            self.assertTrue(os.path.isdir('output-gzip'))

            read = os.popen('gzip -cd output-gzip/*').read()
            self.assertItemsEqual(mem_testdata, read.rstrip('\n').split('\n'))

        finally:
            os.system("rm output-gzip -r")

    def test_gzip_file_case_2(self):
        expect = ['wan', 'cheng', 'Hello, world!', 'Hello, toft!', 'Hello, flume!']

        p = self._pipeline.read(input.TextFile('testdata/gzip'))
        self.passertEqual(expect, p)

        p = self._pipeline.read(input.TextFile('testdata/gzip/part-00000.gz',
                                               'testdata/gzip/part-00001.gz'))
        self.passertEqual(expect, p)

        p = self._pipeline.read(input.TextFile('testdata/gzip/*'))
        self.passertEqual(expect, p)

    def check_parallelize(self, data):
        """
        no comment
        """
        def deep_sort(obj):
            """
            Recursively sort list or dict nested lists
            """

            if isinstance(obj, dict):
                _sorted = {}
                for key in sorted(obj):
                    _sorted[key] = deep_sort(obj[key])

            elif isinstance(obj, list):
                new_list = []
                for val in obj:
                    new_list.append(deep_sort(val))
                _sorted = sorted(new_list)

            else:
                _sorted = obj
            return _sorted

        datasets = map(lambda d: d["dataset"], data)
        pcollections = map(lambda d: self._pipeline.parallelize(**d), data)
        for plist in pcollections:
            plist.cache()
        for d, plist in zip(datasets, pcollections):
            self.assertEqual(deep_sort(d), deep_sort(plist.get()))

    def run_check_parallelize_case(self):
        """
        no comment
        """

        self.check_parallelize([
            {"dataset": None},
            {"dataset": 1},
            {"dataset": []},
            {"dataset": [1]},
            {"dataset": [1, 2, 'str']},
            {"dataset": [1, 2, None]},
            {"dataset": {1: 2, 3: 4}},
            {"dataset": {1: 2}},
            {"dataset": {None: None}},
            {"dataset": {'1': 1, '2': 2}},
            {"dataset": {'1': [1, 2, 3], '2': [1, 2], '3': [None]}},
            {"dataset": {1: {2: {'3': [1, 2, '3'], '4': [None]}, 3: {'3': [1, 2]}}, 2: {1: {'3': [1, 2]}}}},
        ])

    def run_check_parallelize_case_with_serde(self):
        """
        set parallelize serde
        """
        self.check_parallelize([
            {"dataset": None, "serde": self._pipeline.default_objector()},
            {"dataset": 1, "serde": serde.IntSerde()},
            {"dataset": [], "serde": serde.StrSerde()},
            {"dataset": [1], "serde": serde.IntSerde()},
            {"dataset": [1, 2, 'str'], "serde": self._pipeline.default_objector()},
            {"dataset": [1, 2, None], "serde": serde.Optional(serde.IntSerde())},
            {"dataset": {1: 2, 3: 4}, "serde": self._pipeline.default_objector()},
            {"dataset": [{1: 2, 3: 4}], "serde": serde.DictSerde(int, int)},
            {"dataset": {1: 2, 3: 4}, "serde": self._pipeline.default_objector()},
            {"dataset": [{"name": "bob", "age": 14}],
             "serde": FieldsDictSerde({"name": str, "age": int})},
            {"dataset": {1: 2}, "serde": self._pipeline.default_objector()},
            {"dataset": {None: None}, "serde": self._pipeline.default_objector()},
            {"dataset": {'1': 1, '2': 2}, "serde": self._pipeline.default_objector()},
            {"dataset": {'1': [1, 2, 3], '2': [1, 2], '3': [None]},
             "serde": self._pipeline.default_objector()},
            {"dataset": {1: {2: {'3': [1, 2, '3'], '4': [None]}, 3: {'3': [1, 2]}}, 2: {1: {'3': [1, 2]}}},
             "serde": self._pipeline.default_objector()},
            {"dataset": [{"name": "bob", "age": 14, "date": datetime.date(2017,1,1)}],
             "serde": FieldsDictSerde({"name": str, "age": int, "date": serde.CPickleSerde()})},
        ])

    def test_parallelize_with_pipeline_default_serde(self):
        """
        no comment
        """

        from bigflow import serde
        import cPickle
        serde.USE_DEFAULT_SERDE = False

        self.setConfig(default_serde=serde.CPickleSerde())
        self.run_check_parallelize_case()

    def test_parallelize(self):
        """
        no comment
        """

        self.run_check_parallelize_case()

    def test_parallelize_with_serde(self):
        """
        """
        self.run_check_parallelize_case_with_serde()


    def test_pickle(self):
        """
        no comment
        """
        class _A(object):
            def __init__(self, pipeline):
                self.p = pipeline

            def _map_fn(self, record):
                return record + 1

        test_obj = _A(self._pipeline)
        _input = self._pipeline.parallelize([2, 3, 4])
        _output = _input.map(test_obj._map_fn)

        self.assertItemsEqual([3, 4, 5], _output.get())

    @test_base.run_mode(mode=["local", "hadoop"], fs=['local', 'hdfs'])
    def test_add_remote_file_for_local(self):
        """test add remote file"""
        import subprocess
        prog_dir = self.generate_tmp_path()
        data_dir = self.generate_tmp_path()
        prog_list = ["#!/bin/env python", "#-*- coding:utf-8 -*-"]
        prog_list += ["def map(x):", "    return (x, 1)"]
        words = ["zhu", "xi", "da", "fa", "hao"]
        prog = self._pipeline.parallelize(prog_list)
        data = self._pipeline.parallelize(words)
        # set partition number to 1
        # for mv * can only handle one file if target is a file
        self._pipeline.write(prog, output.TextFile(prog_dir).partition(n=1))
        self._pipeline.write(data, output.TextFile(data_dir).partition(n=1))
        self._pipeline.run()

        # rename
        file_prog = os.path.join(prog_dir, "*")
        file_data = os.path.join(data_dir, "*")
        target_prog = os.path.join(prog_dir, "remote_map.py")
        target_data = os.path.join(data_dir, "words.txt")

        if self.running_on_filesystem == "local":
            # only file in the folder
            subprocess.Popen("mv %s %s" % (file_prog, target_prog), shell=True).wait()
            subprocess.Popen("mv %s %s" % (file_data, target_data), shell=True).wait()
        else:
            hadoop = "{bin} fs -conf {conf_path} -mv".format(
                    bin=self._pipeline._config['hadoop_client_path'],
                    conf_path=self._pipeline._config['hadoop_config_path'])
            subprocess.Popen("{hadoop} {source} {target}".format(
                hadoop=hadoop, source=file_prog, target=target_prog), shell=True).wait()
            subprocess.Popen("{hadoop} {source} {target}".format(
                hadoop=hadoop, source=file_data, target=target_data), shell=True).wait()

        self._pipeline.add_file(target_prog, "remote_map.py")
        self._pipeline.add_file(target_data, "words.txt")

        def _remote_map(x):
            """inner map"""
            local_words = []
            with open("words.txt") as fd:
                for line in fd:
                    local_words.append(line.strip())

            assert local_words == words, "local words and remote words not equal"
            import remote_map
            return remote_map.map(x)

        p_words = self._pipeline.parallelize(words)
        p_words_map = p_words.map(_remote_map)
        p_res = p_words_map.get()
        p_ori = map(lambda w:(w, 1), words)
        self.assertItemsEqual(p_ori, p_res)

    def test_init_hooks(self):
        """test settings init hooks"""
        from bigflow.pipeline import pipeline_base

        def _init_a_val():
            pipeline_base.test_dict = {'a': 1}

        def _inc_a_val():
            pipeline_base.test_dict['a'] += 1

        def _get_a_val(record):
            return pipeline_base.test_dict['a']

        self._pipeline.set_init_hook("0_init", _init_a_val)
        self._pipeline.set_init_hook("1_init", _inc_a_val)

        p_test = self._pipeline.parallelize(None)

        p_result = p_test.map(_get_a_val)
        result = p_result.get()
        self.assertEqual(result, 2)
        # todo(yexianjin): remove this hack. Spark pipeline cannot add new resource(init_hooks)
        # after first run
        if self.pipeline_type == "spark":
            self.setConfig()
            self._pipeline.set_init_hook("0_init", _init_a_val)
            self._pipeline.set_init_hook("1_init", _inc_a_val)
            p_test = self._pipeline.parallelize(None)

        self._pipeline.set_init_hook("2_init", _inc_a_val)

        p_result2 = p_test.map(_get_a_val)
        result2 = p_result2.get()
        self.assertEqual(result2, 3)

    def test_before_run_hooks(self):
        """test before run hooks"""

        class b_class:
            a = 3

            @staticmethod
            def increase():
                b_class.a += 1

        self._pipeline._set_before_run_hook("001", b_class.increase)
        p_test = self._pipeline.parallelize(None)
        p_result = p_test.map(lambda x: b_class.a)
        p_result2 = p_test.map(lambda x: b_class.a)
        self.assertEqual(1, len(self._pipeline._before_run_hooks))
        # first run
        self.assertEqual(p_result.get(), 3)
        self.assertEqual(0, len(self._pipeline._before_run_hooks))
        # second run, but the lambda is captured in the map operation, which is ran before
        # pipeline.run
        self.assertEqual(p_result2.get(), 3)
        # third run, b_class.a is increased to 4
        self.assertEqual(p_test.map(lambda x: b_class.a).get(), 4)
        # fourth run
        self.assertEqual(p_test.map(lambda x: b_class.a).get(), 4)

    def test_after_run_hooks(self):
        """test after run hooks"""

        # need a class to correctly deal with method scope
        class a_class:
            a = 3

            @staticmethod
            def decrease():
                a_class.a -= 1

        self._pipeline._set_after_run_hook("001", a_class.decrease)
        p_test = self._pipeline.parallelize(None)
        p_result = p_test.map(lambda x: a_class.a)
        self.assertEqual(1, len(self._pipeline._after_run_hooks))
        # first run
        self.assertEqual(p_result.get(), 3)

        self.assertEqual(0, len(self._pipeline._after_run_hooks))
        # second run, cannot use p_result directly
        self.assertEqual(p_test.map(lambda x: a_class.a).get(), 2)
        self.assertEqual(p_test.map(lambda x: a_class.a).get(), 2)

    def test_fini_hooks(self):
        """test settings fini hooks"""
        from bigflow.pipeline import pipeline_base
        def _init_a_val():
            pipeline_base.test_dict = {'a': 1}

        def _dec_a_val():
            pipeline_base.test_dict['a'] -= 1

        def _check_a_val():
            if pipeline_base.test_dict['a'] != 0:
                raise error.BigflowRuntimeException("Expected check_a_val")

        self._pipeline.set_init_hook("0_init", _init_a_val)
        self._pipeline.set_fini_hook("0_dec", _dec_a_val)
        self._pipeline.set_fini_hook("1_check", _check_a_val)

        p_test = self._pipeline.parallelize(None)

        p_result = p_test.map(lambda x: pipeline_base.test_dict['a'])
        self.assertEqual(p_result.get(), 1)

    @test_base.run_mode(mode = ['hadoop', 'spark'])
    def test_default_job_name(self):
        expect_job_name = 'pipeline_test.py'

        self.assertIn(expect_job_name, self._pipeline._default_job_name)

    @test_base.run_mode(fs=['local'])
    def test_hadoop_conf(self):

        expected_config = {'mapred.job.priority': 'VERY_HIGH', 'mapred.job.map.capacity': '1000', 'abaci.dag.enable.auto.parallel': 'false'}
        self.setConfig(hadoop_conf=expected_config)
        self.assertEqual(expected_config['mapred.job.priority'], self._pipeline._hadoop_config['mapred.job.priority'])
        self.assertEqual(expected_config['mapred.job.map.capacity'], self._pipeline._hadoop_config['mapred.job.map.capacity'])
        self.assertEqual(expected_config['abaci.dag.enable.auto.parallel'], self._pipeline._hadoop_config['abaci.dag.enable.auto.parallel'])

        self.setConfig(hadoop_job_conf=expected_config)
        self.assertEqual(expected_config['mapred.job.priority'], self._pipeline._hadoop_config['mapred.job.priority'])
        self.assertEqual(expected_config['mapred.job.map.capacity'], self._pipeline._hadoop_config['mapred.job.map.capacity'])
        self.assertEqual(expected_config['abaci.dag.enable.auto.parallel'], self._pipeline._hadoop_config['abaci.dag.enable.auto.parallel'])


    def default_encoding_check(self, encoding):
        import sys
        reload(sys)
        sys.setdefaultencoding(encoding)
        unicode_welcome_string = u'\u4F60\u597D'
        self.setConfig() # Must setdefaultencoding before pipeline being created.
        data = self._pipeline.parallelize(unicode_welcome_string)
        data = data.map(lambda record: str(record))
        self.passertEqual(unicode_welcome_string.encode(encoding), data)

    @test_base.run_mode(mode=['hadoop', 'spark', 'local'])
    def test_default_encoding_utf8(self):
        self.default_encoding_check('utf-8')

    @test_base.run_mode(mode=['hadoop', 'spark', 'local'])
    def test_default_encoding_gbk(self):
        self.default_encoding_check('gbk')


if __name__ == "__main__":
    unittest.main()
