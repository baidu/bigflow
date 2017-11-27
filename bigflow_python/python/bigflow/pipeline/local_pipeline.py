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
LocalPipeline定义

"""
import os

from bigflow import error
from bigflow.pipeline import pipeline_base
from bigflow_python.proto import pipeline_pb2
from bigflow.rpc import requests
from bigflow.util.log import logger
from flume.proto import config_pb2
from flume.proto import entity_pb2
from bigflow.util import path_util
import tempfile
import pwd


class LocalPipeline(pipeline_base.PipelineBase):
    """
    在本地引擎单机执行的Pipeline

    Args:
      **job_config:  一些引擎相关的参数
    """
    def __init__(self, **job_config):
        super(LocalPipeline, self).__init__(**job_config)
        self._type_str = "LOCAL"
        self._temp_file_dict = {}

        if "hadoop_config_path" in job_config:
            job_config["hadoop_config_path"] = os.path.abspath(job_config["hadoop_config_path"])
        if "hadoop_client_path" in job_config:
            job_config["hadoop_client_path"] = os.path.abspath(job_config["hadoop_client_path"])

        class _DelayParam(object):
            """ inner function"""
            def __init__(self, fn):
                """ inner function"""
                self.__fn = fn

            def get(self):
                """ inner function"""
                return self.__fn()

        self._job_config = config_pb2.PbLocalConfig()

        from bigflow import serde
        self._config = {
            'hadoop_config_path': _DelayParam(requests.default_hadoop_config_path),
            'hadoop_client_path': _DelayParam(requests.default_hadoop_client_path),
            'default_serde': serde.DefaultSerde(),
            'round_times' : -1,
            'status_as_meta' : True,
            'status_meta_path' : './flume/status_meta',
            'status_table_path' : './flume/status_table',
            'status_cache_size' : None,
            'use_leveldb_storage': False,
        }

        self._config.update(job_config)

        for key in self._config.keys():
            if isinstance(self._config[key], _DelayParam):
                self._config[key] = self._config[key].get()

        for (k, v) in self._hadoop_conf.items():
            kv = self._job_config.hadoop_conf.add()
            kv.key = k
            kv.value = v

        self._job_config.hadoop_config_path = self._config['hadoop_config_path']
        self._job_config.hadoop_client_path = self._config['hadoop_client_path']
        self._job_config.round_times =  self._config['round_times']
        self._job_config.status_as_meta = self._config['status_as_meta']
        if self._config['status_meta_path']:
            self._job_config.status_meta_path = self._config['status_meta_path']
        if self._config['status_table_path']:
            self._job_config.status_table_path = self._config['status_table_path']
        if self._config['status_cache_size']:
            self._job_config.status_cache_size = self._config['status_cache_size']

        self._job_config.use_leveldb_storage = self._config['use_leveldb_storage']

        pb = pipeline_pb2.PbPipeline()
        pb.type = pipeline_pb2.PbPipeline.LOCAL
        pb.local_config.CopyFrom(self._job_config)
        requests.register_pipeline(pb, self.id())
        logger.debug("Register Pipeline %s OK" % self.id())

        exception_dir = os.path.join('.tmp', self.id(), 'exception_dir')
        self._exception_path = os.path.join(exception_dir, 'exception')

        self._set_exception_path()

    @property
    def _hadoop_config(self):
        if not self._hadoop_conf.get('hadoop.job.ugi'):
            self._hadoop_conf['hadoop.job.ugi'] = pwd.getpwuid(os.getuid())[0]
        return self._hadoop_conf

    def __del__(self):
        requests.unregister_pipeline(self.id())
        logger.debug("Unregister Pipeline %s OK" % self.id())
        super(LocalPipeline, self).__del__()
        # files are created using TemporaryFile, will be deleted automatically
        # when closed
        for key, file in self._temp_file_dict.iteritems():
            file.close()

    # Override
    def _toft_path(self, uri, ugi=None):
        """toft style path"""
        fs_default_name, hadoop_job_ugi = self._get_fs_conf()
        if ugi:
            hadoop_job_ugi = ugi
        return requests.get_toft_style_path(uri, self._job_config.hadoop_config_path,
                                            fs_default_name, hadoop_job_ugi)

    # Override
    def _transform_uri(self, uri, format_type, ugi=None):
        import glob
        from bigflow.util import path_util
        if format_type == "TextInputFormat" or\
                format_type == "SequenceFileAsBinaryInputFormat" or\
                format_type == "OrcInputFormat" or\
                format_type == "ParquetInputFormat" or \
                format_type == "TextStreamInputFormat" or\
                format_type == "SequenceStreamInputFormat":
            uri = path_util.to_abs_local_path(uri)
            uri = self._toft_path(uri)
            if not path_util.is_toft_style_dfs_path(uri):
                # support local file glob
                uri = glob.glob(uri)
            return uri

        if format_type in ["TextInputFormatWithUgi", "SequenceStreamInputFormatWithUgi"]:
            uri = path_util.to_abs_local_path(uri)
            ret = self._toft_path(uri, ugi)
            return ret
        return uri

    # Override
    def _transform_output_format(self, pcollection, output_format):
        from bigflow.util import path_util
        from bigflow.util import utils

        format_type = output_format.get_entity_name()
        ugi = output_format.ugi if hasattr(output_format, "ugi") else None
        if format_type == "TextOutputFormat" or \
                format_type == "SequenceFileAsBinaryOutputFormat" or \
                format_type == "ParquetOutputFormat" or \
                format_type == "PartitionedParquetOutputFormat":
            uri = path_util.to_abs_local_path(output_format.path)
            if utils.is_infinite(pcollection):
                output_format.path = self._toft_path(uri, ugi)
            else:
                output_format.path = self._toft_path(self._tmp_output_path(uri), ugi)
                output_format.commit_path = self._toft_path(uri, ugi)

        return output_format

    # Override
    def add_file(self, file_path, resource_path=None, executable=False):
        """
        向Pipeline添加单个文件，使得该文件能够在运行期被访问

        Args:
          file_path(str): 需要添加的文件路径，可以是本地路径或者 HDFS 路径.
          resource_path(str): local 引擎运行时访问该文件的路径, 应是相对路径. 也即本地引擎在执行时,
                              file_path 将会被映射到该 resource_path, 用户程序可以以该路径访问
          executable (bool): 若为True，则该文件在运行期会被添加可执行属性
        """
        if path_util.is_hdfs_path(file_path.lower()):
            self._add_remote_file(file_path, resource_path, executable)
        else:
            self._resource.add_file(file_path, resource_path, executable)

    def _add_remote_file(self, file_path, resource_path, executable=False):
        """add remote file for local pipeline
        生成hadoop命令来下载文件

        Args:
          file_path (str): 文件路径
          resource_path (str): 运行期访问该文件的路径
        """

        # add script file in BIGFLOW_PYTHON_HOME/prepare/,
        # ended with .sh
        script = tempfile.NamedTemporaryFile("w", suffix=".sh")
        assert(script.name not in self._temp_file_dict)
        self._temp_file_dict[script.name] = script
        header = "#!/bin/sh\n"
        header += "#This is generated automatically\n"
        script.write(header)

        # generate script contents
        hadoop_bin = self._config['hadoop_client_path']
        hadoop_conf = self._config['hadoop_config_path']
        job_conf = ''
        for (k, v) in self._hadoop_config.items():
            job_conf += ' -D "%s"="%s" ' % (k, v)

        download_cmd = "\n# download file: %s\n" % (os.path.basename(file_path))
        download_cmd += '"{bin}" fs --conf "{conf}" {job_conf} -copyToLocal '.\
                format(bin=hadoop_bin, conf=hadoop_conf, job_conf=job_conf)
        download_cmd += ' "%s" ' % file_path
        download_cmd += ' "%s" ' % resource_path
        download_cmd += "\n\n"
        script.write(download_cmd)
        script.flush()

        target_path = os.path.join("prepare", os.path.basename(script.name))
        self._resource.add_file(script.name, target_path, executable)
        # for test
        return script.name

    def _handle_serialized_exception(self):
        if self._path_exists(self._exception_path):
            import cPickle
            e = cPickle.load(open(self._exception_path))
            if isinstance(e, str):
                raise error.BigflowRuntimeException(e) #>>> The stack uppon this is useless >>>
            elif isinstance(e, Exception):
                raise e #>>> The stack uppon this is useless, please see the error below: >>>
            else:
                raise error.BigflowRuntimeException(
                    "Unknow error, please contact us. bigflow@baidu.com.\nerr_msg = %s" % str(e)
                )
        return True

    # Override
    def run(self):
        """
        立刻运行Pipeline并等待结束

        Raises:
          BigflowRuntimeException:  若运行期出错抛出此异常
        """
        self._before_run()
        try:
            commit_args = []
            for key, value in self._hadoop_config.iteritems():
                commit_args.extend(["-D", key + "=" + value])
            requests.launch(self._id, self._plan_message, self._resource_message, commit_args)
            logger.info("Job ran successfully")
        except Exception as e:
            self._handle_serialized_exception()
            raise
        self._after_run()

    # Override
    def _before_run(self):

        exception_dir = os.path.dirname(self._exception_path)
        os.system('rm -rf {dir};mkdir -p {dir}'.format(dir = exception_dir))

        super(LocalPipeline, self)._before_run()

