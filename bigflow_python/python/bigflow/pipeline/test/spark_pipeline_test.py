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
Spark pipeline specialized tests

Author: Ye, Xianjin(bigflow-opensource@baidu.com)
"""

import os
import shutil
import unittest

from bigflow import output
from bigflow import serde
from bigflow.test import test_base


serde.USE_DEFAULT_SERDE=False


class TestCase(test_base.PipelineBasedTest):

    @test_base.run_mode(mode='spark', fs=['local', 'hdfs'])
    def test_add_remote_file(self):
        """test add remote file"""
        prog_dir = self.generate_tmp_path()
        data_dir = self.generate_tmp_path()
        prog_list = ["#!/bin/env python", "#-*- coding:utf-8 -*-"]
        prog_list += ["def map(x):", "    return (x, 1)"]
        words = ["zhu", "xi", "da", "fa", "hao"]
        prog = self._pipeline.parallelize(prog_list)
        data = self._pipeline.parallelize(words)
        # set partition number to 1 so we can make sure all the data is in the part-00000
        self._pipeline.write(prog, output.TextFile(prog_dir).partition(n=1))
        self._pipeline.write(data, output.TextFile(data_dir).partition(n=1))
        self._pipeline.run()

        target_prog = os.path.join(prog_dir, "part-00000")
        # Currently spark fails to deal with cache files with same file name.
        # In this test, move part-00000 to part-data temporarily. Should revert this change when
        # spark can handle same file name cache files.
        origin_target_data = os.path.join(data_dir, "part-00000")
        if self.running_on_filesystem == "local":
            target_data = origin_target_data
        else:
            target_data = os.path.join(data_dir, "part-data")
            self._pipeline._client.fs_mv(origin_target_data, target_data)

        # reset the pipeline, so this can work on spark pipeline.
        # todo: when spark_pipeline provides addFile, use addFile to handle add_file request after
        # todo: first run.
        self.setConfig()
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
        p_ori = p_words.map(lambda x: (x, 1))
        self.passertEqual(0, p_words_map.diff(p_ori).count())

    @test_base.run_mode(mode='spark')
    def test_add_archive(self):
        """ Test add_archive
        """
        # create gzip.tgz
        import tarfile
        gzip_tar = tarfile.open("gzip.tgz", "w:gz")
        gzip_tar.add("testdata/gzip")
        gzip_tar.close()

        # get temp path and move gzip.tgz to tmp location
        tmp_location = self.generate_tmp_path()
        archive_path = os.path.join(tmp_location, "gzip.tgz")
        if self.running_on_filesystem == "local":
            os.makedirs(tmp_location)
            shutil.move("gzip.tgz", archive_path)
        else:
            # use hadoop client to upload cache archive to hdfs
            local_path = os.path.abspath("gzip.tgz")
            self._pipeline._client.fs_put(local_path, archive_path)
            os.remove(local_path)

        # use cache archive.
        self._pipeline.add_archive(archive_path, "testdata/gzip")
        in_pc = self._pipeline.parallelize([1, 2])
        # first occurrence of testdata/gzip: cache archive target name
        # second occurrence of testdata/gzip: original directory layout
        files = in_pc.flat_map(lambda _: os.listdir("testdata/gzip/testdata/gzip"))
        self.passertEqual(["part-00000.gz", "part-00001.gz"] * 2, files)

    @test_base.run_mode(mode="spark")
    def test_add_egg_file(self):

        def _remote_map(x):
            import columns
            return x

        self._pipeline.add_egg_file("testdata/columns/dist/columns-0.1.0.0-py2.7.egg")
        result = self._pipeline.parallelize([1, 2, 3, 4, 5]) \
                     .map(_remote_map)

        self.passertEqual([1, 2, 3, 4, 5], result)

    @test_base.run_mode(mode="spark")
    def test_spark_config_merge(self):
        executor_extra_library_path = "spark.executor.extraLibraryPath"
        self.setConfig(spark_conf={
            executor_extra_library_path: "/path/to/anywhere",
        })

        extra_lib_list = \
            self._pipeline._config["spark_conf"][executor_extra_library_path].split(":")
        self.assertIn(".", extra_lib_list)
        self.assertEqual(4, len(extra_lib_list))
        self.assertEqual("/path/to/anywhere", extra_lib_list[-1])



if __name__ == "__main__":
    unittest.main()
