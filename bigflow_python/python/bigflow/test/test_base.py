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
A some basic classes for unit test

Author: Wang, Cong(wangcong09@baidu.com)
"""

import unittest
import shutil
import os
import sys
import uuid

from bigflow import base
from bigflow import input
from bigflow import output
from bigflow import serde
from bigflow.util.log import logger


class PipelineBasedTest(unittest.TestCase):
    """
    A unit test that is based on Pipeline, it will clean all temp files after Pipeline runs
    """
    def setUp(self):
        """ no comments """
        if self.shortDescription() is None:
            case_msg = self.id()
        else:
            case_msg = "%s - %s" % (self.shortDescription(), self.id())

        logger.info('setUp for case: %s' % case_msg)
        self._tmp_path = []

        self.pipeline_type = os.environ.get('PIPELINE_TYPE', 'local').lower()
        if self.pipeline_type == 'hadoop':
            self.pipeline_type = 'dagmr'

        if not hasattr(self, "running_on_filesystem"):
            if self.pipeline_type == "local":
                self.running_on_filesystem = "local"
            else:
                self.running_on_filesystem = "hdfs"

        if self.pipeline_type not in self._supported_pipeline_type():
             self.skipTest("pipeline type is not supported in this case")

        hdfs_root_path = os.environ.get('HDFS_ROOT_PATH', '').strip()

        self.support_file_system = ['local']
        if hdfs_root_path:
            self.support_file_system.append("hdfs")

        self.root_path_dict = {'local': '.', 'hdfs': hdfs_root_path}
        self.root_path = self.root_path_dict[self.running_on_filesystem]

        self.setConfig()

        self._conditions = []

    def tearDown(self):
        try:
            if self._conditions:
                self.run_and_check_passert()
        finally:
            for local_path in self._tmp_path:
                self._pipeline._force_delete_file(local_path)
            if hasattr(self, "_pipeline"):
                del(self._pipeline)
            self._conditions = []
            self._tmp_path = []

    def run_and_check_passert(self):

        for idx, condition in enumerate(self._conditions):
            from bigflow import pobject
            from bigflow import pcollection
            from bigflow import ptable

            cond = list(condition)
            p_type = type(cond[1])
            cond[1] = cond[1].get()
            cond[2] = cond[2] + '\nexpect is:\n%s\nbut real is:\n%s\n' %(cond[0], cond[1])

            if issubclass(p_type, pobject.PObject):
                self.assertEqual(*cond)
            elif issubclass(p_type, pcollection.PCollection):
                self.assertItemsEqual(*cond)
            elif issubclass(p_type, ptable.PTable):
                # use self.assertItemsEqual directly will not compare dict as you wish
                from bigflow.util import utils
                cond[0] = utils.flatten_runtime_value(cond[0])
                cond[1] = utils.flatten_runtime_value(cond[1])
                self.assertItemsEqual(*cond)
            else:
                raise Exception("passertEqual's second param must be a PObject/PCollection/PTable, "
                    "but you put a %s in this position" % str(p_type) + '\n' + msg)
        self._conditions = []


    def _supported_pipeline_type(self):
        return ['local', 'dagmr', 'spark']

    def _assertDictItemsEqual(self, dict1, dict2):
        import collections
        self.assertItemsEqual(dict1, dict2)
        for key in dict1.keys():
            if isinstance(dict1[key], list):
                self.assertItemsEqual(dict1[key], dict2[key])
            elif isinstance(dict1[key], collections.MutableMapping):
                self._assertDictItemsEqual(dict1[key], dict2[key])
            else:
                self.assertEqual(dict1[key], dict2[key])

    def generate_tmp_path(self):
        """ generate a tmp path """
        path = self.root_path
        if self.running_on_filesystem == 'local':
            path = path + '/.tmp_' + str(uuid.uuid4()) # in case of being added by other cases
        else:
            path = path + '/tmp_' + str(uuid.uuid4()) # hdfs path start with dot will be ignored in some situation
        self._tmp_path.append(path)
        return path

    def _get_run_fs(self):
        return {v: k for k, v in self.root_path_dict.iteritems()}[self.root_path]

    def passertEqual(self, expect, real, msg=None):
        """
        no comment
        """
        if msg is None:
            msg = ""

        def _info():
            import inspect
            call_frame = inspect.getframeinfo(inspect.currentframe().f_back.f_back)
            return '%s:%d %s [pipline=%s][fs=%s]\n %s' % \
                   (call_frame.filename, call_frame.lineno,
                    call_frame.function, self.pipeline_type, self.running_on_filesystem,
                    ''.join(call_frame.code_context))

        msg = _info() + '\n' + msg

        real.cache()
        self._conditions.append((expect, real, msg))

    def passertTrue(self, cond, msg=None):
        """
        no comment
        """

        return self.passertEqual(True, cond, msg)

    def _get_tmp_data_path_for_unit_test(self):
        tmp_data_path = None
        if  self.running_on_filesystem == "local" or\
           self.running_on_filesystem == "hdfs":
            tmp_data_path = os.environ.get('HDFS_ROOT_PATH', 'hdfs:///tmp') + \
                '/tmp_data_path'
        else:
            logger.error('!!!!! unknowed filesystem: %s !!!!!' % \
                    str(self.running_on_filesystem))
            exit(1)

        return tmp_data_path

    def _get_hadoop_config_path_for_unit_test(self):
        hadoop_config_path = None
        if  self.running_on_filesystem == "local" or\
           self.running_on_filesystem == "hdfs":
            hadoop_config_path = os.environ.get("HDFS_CONF_PATH", "")
        else:
            logger.error('!!!!! unknowed filesystem: %s !!!!!' % \
                    str(self.running_on_filesystem))
            exit(1)

        if not hadoop_config_path:
            hadoop_config_path = os.environ.get("HADOOP_HOME", "") + "/etc/hadoop/core-site.xml"

        return hadoop_config_path

    def create(self, **kargs):
        """ no comments """
        kargs["default_concurrency"] = 10

        tmp_data_path = self._get_tmp_data_path_for_unit_test()
        hadoop_config_path = self._get_hadoop_config_path_for_unit_test()

        self._hadoop_config_path = hadoop_config_path

        if self.pipeline_type != "spark":
            return base.Pipeline.create(self.pipeline_type,
                                        tmp_data_path = tmp_data_path,
                                        hadoop_config_path = hadoop_config_path,
                                        **kargs)
        else:
            spark_conf = {
                "spark.yarn.stage.dir": "hdfs:///app/dc/bigflow/spark/spark-stages",
            }
            if 'spark_conf' in kargs:
                spark_conf.update(kargs['spark_conf'])
                del kargs['spark_conf']


            return base.Pipeline.create(self.pipeline_type,
                                        tmp_data_path = tmp_data_path,
                                        spark_conf = spark_conf,
                                        hadoop_config_path = hadoop_config_path,
                                        **kargs)

    def setConfig(self, **kargs):
        """
        no comment
        """
        kargs.update({'job_name': ' '.join(sys.argv) + '|' + self.id()})
        self._pipeline = self.create(**kargs)


class StreamingPipelineBasedTest(PipelineBasedTest):
    """
    use for test streaming pipeline
    """
    def tearDown(self):
        self._pipeline.kill()
        super(StreamingPipelineBasedTest, self).tearDown()

    def _supported_pipeline_type(self):
        """ no comments """
        return ['tm']

    def setConfig(self, **kargs):
        """ no comments """
        kargs.update({
            'pagoda_job_conf': {
                'cluster': 'pagoda://szwg-inf-szwg-dstream17.szwg01.baidu.com:40379,szwg-inf-szwg-dstream16.szwg01.baidu.com:40379/pagoda_sandbox_szwg',
                'ugi': 'pagoda_user,pagoda',
                'tm_name': 'xuyao_tm_server',
                'deleter_archive_path': 'hdfs:///bigflow/streaming/deleter_2.tar.gz#deleter',
                'physical_name': 'pagoda_phy',
                'logical_name': 'pagoda',
            }})
        self._pipeline = self.create(**kargs)

    def getResultWithText(self, pipeline_status, path):
        """ no comments """
        pipeline_status.wait_status("APP_RUN")
        import time
        time.sleep(300)

        local_pipeline = base.Pipeline.create('local')
        result = local_pipeline.read(input.TextFile(path))
        return result.get()

    def getResultWithSequence(self, pipeline_status, path):
        """ no comments """
        pipeline_status.wait_status("APP_RUN")
        import time
        time.sleep(300)

        local_pipeline = base.Pipeline.create('local')
        result = local_pipeline.read(input.SequenceFile(path))
        return result.get()


class MRTestCaseWithMockClient(unittest.TestCase):
    """ use for test with mocked hadoop client """

    #_cmds_path = os.path.dirname(os.path.abspath(__file__)) + '/dataflow-hadoop-cmds'

    def setUp(self):
        """ override """
        if os.environ.get('PIPELINE_TYPE', 'local').lower() != 'local':
            self.skipTest("pipeline type is not support for this case")

        # try:
        #     #os.remove(self._cmds_path)
        #     #self.reset_cmds_path()
        # except:
        #     pass

        from bigflow.test import mock_hadoop_client
        self._mock_client = mock_hadoop_client.MockHadoopClient()
        self._hadoop_client_path = self._mock_client.mock_hadoop_client_path
        os.system('chmod a+x %s' % self._hadoop_client_path)

    def create_pipeline(self, **kargs):
        """ used to create a pipeline """
        self._hadoop_config_path = os.environ.get("HDFS_CONF_PATH", "")
        if not self._hadoop_config_path:
            self._hadoop_config_path = os.environ.get("HADOOP_HOME", "") \
                    + "/etc/hadoop/core-site.xml"
        return base.Pipeline.create('dagmr',
            hadoop_client_path = self._hadoop_client_path,
            hadoop_config_path = self._hadoop_config_path,
            **kargs)

    def get_mock_client(self):
        """ used to get mock client """
        return self._mock_client


def run_mode(mode=None, fs=None):
    """ decorator """
    if not mode:
        mode = ['local', 'dagmr', 'hadoop', 'spark']
    if not fs:
        fs = ['local', 'hdfs']

    if isinstance(mode, tuple) or isinstance(mode, list):
        modes = mode
    else:
        modes = [mode]
    if 'dagmr' in modes or 'hadoop' in modes:
        modes.extend(['dagmr', 'hadoop'])

    if fs is None:
        fs = 'hdfs' if os.environ.get('HDFS_ROOT_PATH', '').strip() else 'local'
        expect_filesystems = ["local"]
        hdfs_root_path = os.environ.get("HDFS_ROOT_PATH", "").strip()
        if hdfs_root_path:
            expect_filesystems.append("hdfs")

    if isinstance(fs, tuple) or isinstance(fs, list):
        expect_filesystems = fs
    else:
        expect_filesystems = [fs]

    def dec(fn):
        """ inner """
        def _skip_filesystem_test(filesystem):
            env = ["SKIP_LOCAL_TEST", "SKIP_HDFS_TEST"]
            target = ["local", "hdfs"]

            for idx, e in enumerate(env):
                fs = target[idx]
                flag = os.environ.get(e)
                if flag is not None and filesystem == fs:
                    return True
            return False

        def wrapper(test_class_obj):
            """ inner """
            _first_run = True
            if test_class_obj.pipeline_type in modes:
                for filesystem in expect_filesystems:
                    if filesystem in test_class_obj.support_file_system:
                        if _skip_filesystem_test(filesystem):
                            continue
                        test_class_obj.root_path = test_class_obj.root_path_dict[filesystem]
                        test_class_obj.running_on_filesystem = filesystem
                        logger.info("running case [%s.%s] root_path=[%s], filesystem=[%s]" %
                                    (type(test_class_obj).__name__, fn.func_name, test_class_obj.root_path,
                                     test_class_obj.running_on_filesystem))

                        if not _first_run:
                            test_class_obj.tearDown()
                        test_class_obj.setUp()
                        fn(test_class_obj)
                        _first_run = False
                    else:
                        logger.warn('\033[01;31mWarning!!! %s not executed,' \
                                ' because filesystem is %s.\033[00m' \
                                %(fn.__name__, filesystem))

        return wrapper

    return dec

if __name__ == "__main__":
    unittest.main()
