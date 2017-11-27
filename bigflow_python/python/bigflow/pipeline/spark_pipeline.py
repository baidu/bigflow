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
SparkPipeline定义

"""
import uuid
import os
import glob

import bigflow
from bigflow import version as bigflow_version
from bigflow import error
from bigflow.pipeline import pipeline_base
from bigflow_python.proto import pipeline_pb2
from bigflow.rpc import requests
from bigflow.util.log import logger
from flume.proto import config_pb2
from bigflow.util import path_util
from bigflow.util import hadoop_client
import tempfile
import pwd


class SparkPipeline(pipeline_base.PipelineBase):
    """
    基于Spark执行的Pipeline

    Args:
      **pipeline_options:  引擎相关的参数
    """

    # todo: refactor the __init__ function, it's too long

    cache_archive_file_name = "bigflow_python_on_spark.tar.gz"
    output_dir_conf_key = "bigflow.output.dir"

    def __init__(self, **pipeline_options):
        super(SparkPipeline, self).__init__(**pipeline_options)
        self._type_str = "SPARK"
        self._local_uri_infos = []
        self._default_job_name = self._get_default_job_name()

        if "hadoop_config_path" in pipeline_options:
            pipeline_options["hadoop_config_path"] = os.path.abspath(pipeline_options["hadoop_config_path"])
        if "hadoop_client_path" in pipeline_options:
            pipeline_options["hadoop_client_path"] = os.path.abspath(pipeline_options["hadoop_client_path"])
        if "spark_home_path" in pipeline_options:
            pipeline_options["spark_home_path"] = os.path.abspath(pipeline_options["spark_home_path"])

        class _DelayParam(object):
            """ inner function"""
            def __init__(self, fn):
                """ inner function"""
                self.__fn = fn

            def get(self):
                """ inner function"""
                return self.__fn()

        # config as pb message
        self._job_config = config_pb2.PbSparkConfig()

        def _get_reprepare_cache_archive():
            reprepare = os.getenv('BIGFLOW_REPREPARE_CACHE_ARCHIVE')
            if not reprepare:
                reprepare = False
            elif 'true' == reprepare.lower():
                reprepare = True
            else:
                reprepare = False
            return reprepare

        from bigflow import serde
        # config as dict
        self._default_spark_conf = {
            "spark.app.name": self._default_job_name,
            "spark.master": "yarn",
            "spark.local.dir": ".bigflow.on.spark",
            "spark.executor.extraClassPath": "spark_launcher.jar",
			"spark.executorEnv.PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION":"python",
            "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.LocalFileSystem",
            #"spark.hadoop.fs.hdfs.impl": "org.apache.hadoop.fs.DFileSystem",
        }

        default_merging_spark_conf = {
            # (key, value, separator, prepend)
            ("spark.executor.extraLibraryPath",
             ".:__bigflow_on_spark__/flume:__bigflow_on_spark__/python_runtime/lib", ":", True)
        }

        self._config = {
            'hadoop_config_path': _DelayParam(requests.default_hadoop_config_path),
            'hadoop_client_path': _DelayParam(requests.default_hadoop_client_path),
            'spark_home_path': _DelayParam(requests.default_spark_home_path),
            'default_serde': serde.DefaultSerde(),
            'spark_conf': {},
            'reprepare_cache_archive': _DelayParam(_get_reprepare_cache_archive),
            'bigflow_version': bigflow_version.bigflow_version,
            # now only support cpu profiling
            'cpu_profile': False,
            'heap_profile': False,
        }
        # update config by pipeline options
        self._config.update(pipeline_options)
        self._default_spark_conf.update(pipeline_options.get("spark_conf", {}))
        # merge spark configs which should not be simply replaced.
        for (k, v, sep, prepend) in default_merging_spark_conf:
            original_v = self._default_spark_conf.get(k)
            if original_v is None:
                self._default_spark_conf[k] = v
            else:
                merged_v = v + sep + original_v if prepend else original_v + sep + v
                self._default_spark_conf[k] = merged_v

        # Accept job_name as spark application name
        if self._config.get("job_name"):
            self._default_spark_conf["spark.app.name"] = self._config["job_name"]
        self._config["spark_conf"] = self._default_spark_conf
        for key in self._config.keys():
            if isinstance(self._config[key], _DelayParam):
                self._config[key] = self._config[key].get()

        # check spark_home is set and valid
        spark_home_path = self._config["spark_home_path"]
        assert spark_home_path, "Spark home is not set, please specify spark home by " \
                                "Pipeline.create or by setting SPARK_HOME environment variable"
        assert os.path.isdir(spark_home_path), "Specified spark_home: %s is not a valid path, " \
                                               "" % spark_home_path
        # insert spark's core-site.xml over default hadoop client's config path unless explicitly
        # specified. I still think this's debatable,
        if not ("hadoop_config_path" in pipeline_options or "HADOOP_CONF_PATH" in os.environ):
            self._config["hadoop_config_path"] = os.path.join(spark_home_path, "conf/core-site.xml")

        for (k, v) in self._config['spark_conf'].items():
            kv = self._job_config.kv_config.add()
            kv.key = k
            kv.value = v

        # set cpu and heap profiling switch.
        self._job_config.cpu_profile = self._config.get("cpu_profile", False)
        self._job_config.heap_profile = self._config.get("heap_profile", False)

        self._job_config.hadoop_config_path = self._config['hadoop_config_path']
        self._job_config.hadoop_client_path = self._config['hadoop_client_path']
        self._job_config.spark_home_path = self._config['spark_home_path']

        if not 'tmp_data_path' in self._config.keys():
            err_msg = "Please set tmp_data_path to a writable HDFS dir" \
                + " when you use hadoop/dagmr pipeline to run Bigflow."
            logger.warn(err_msg)
            raise error.InvalidConfException(err_msg)
        if not self._config['tmp_data_path'].startswith('hdfs://'):
            self._config['tmp_data_path'] = "hdfs://" + self._config['tmp_data_path']
            err_msg = "!!!!! Your tmp_data_path is not start with hdfs://, " \
                    + "so Bigflow set `hdfs://` by default. !!!!!"
            logger.warn(err_msg)
        self._config['tmp_data_path'] = os.path.join(
            self._config['tmp_data_path'],
            self._config['bigflow_version']
        )
        self._job_config.tmp_data_path = self._config['tmp_data_path']
        self.prepared_archive_path = self._config['tmp_data_path'] \
            + "/" + SparkPipeline.cache_archive_file_name
        self._job_config.prepared_archive_path = self.prepared_archive_path

        if 'default_concurrency' in self._config:
            self._job_config.default_concurrency = self._config['default_concurrency']

        pb = pipeline_pb2.PbPipeline()
        pb.type = pipeline_pb2.PbPipeline.SPARK
        pb.spark_config.CopyFrom(self._job_config)

        requests.register_pipeline(pb, self.id())
        logger.debug("Register Pipeline %s OK" % self.id())

        self._pipeline_tmp_dir = os.path.join(self._job_config.tmp_data_path, 'pipeline', self.id())
        self._local_exception_path = os.path.join('.tmp', self.id(), 'exception')
        self._exception_path = os.path.join(self._pipeline_tmp_dir, 'exception_dir', 'exception')
        self._set_python_path_in_init_hooks()

        self._is_first_run = True
        self._client = hadoop_client.HadoopClient(self._job_config.hadoop_client_path,
                                                  self._job_config.hadoop_config_path)

    @property
    def _hadoop_config(self):

        provided_hadoop_config = self._hadoop_conf
        for key, value in self._config.get('spark_conf', {}).iteritems():
            if not key.startswith('spark.'):
                provided_hadoop_config[key] = value

        if not provided_hadoop_config.get('hadoop.job.ugi'):
            provided_hadoop_config['hadoop.job.ugi'] = pwd.getpwuid(os.getuid())[0]

        return provided_hadoop_config

    # Override
    def _toft_path(self, uri, ugi=None):
        fs_defaultfs_key = "fs.defaultFS"
        hadoop_job_ugi_key = "hadoop.job.ugi"
        user_provided_config = self._hadoop_config

        fs_defaultfs = None if fs_defaultfs_key not in user_provided_config \
            else user_provided_config.get(fs_defaultfs_key)
        hadoop_job_ugi = None if hadoop_job_ugi_key not in user_provided_config \
            else user_provided_config.get(hadoop_job_ugi_key)

        return requests.get_toft_style_path(
                uri,
                self._job_config.hadoop_config_path,
                fs_defaultfs,
                hadoop_job_ugi)

    def _upload_file(self, local_path):
        user_provided_config = self._hadoop_config
        hdfs_path = []
        tmp_data_path = self._job_config.tmp_data_path

        for path in glob.glob(local_path):

            if os.path.isdir(path):
                target = os.path.join(tmp_data_path, "local_input", str(uuid.uuid4()))
                logger.info("Uploading input directory [%s] to [%s]" % (path, target))
            elif os.path.isfile(path):
                # do not change the basename of the input file to keep the suffix.
                file_name = os.path.basename(path)
                target = os.path.join(tmp_data_path, "local_input", str(uuid.uuid4()), file_name)
                logger.info("Uploading input file [%s] to [%s]" % (path, target))
            else:
                raise error.BigflowRuntimeException("file [%s] (matched by pattern [%s]) "
                                                    "is neither a dir nor a regular file" % (path, local_path))

            hdfs_path.append(target)
            self._client.fs_put(path, target, user_provided_config)
            self._remote_temp_files.append(target)

        return hdfs_path

    # Override
    def _transform_uri(self, uri, format_type, ugi=None):
        from bigflow.util import path_util

        if format_type == "TextInputFormat" or \
                format_type == "SequenceFileAsBinaryInputFormat":
            uri = path_util.to_abs_local_path(uri)

            if not path_util.is_hdfs_path(uri):
                return self._upload_file(uri)
            else:
                return uri
        # todo: support multiple clusters and ugis for spark_pipeline
        return uri

    def _tmp_hdfs_path(self, path):
        return self._job_config.tmp_data_path + "/local_output/" + str(uuid.uuid4())

    def _transform_output_format(self, pcollection, output_format):
        from bigflow.util import path_util
        from bigflow.util import utils

        format_type = output_format.get_entity_name()
        # todo: extract ugi from output_format, support multiple clusters and ugis
        if format_type == "TextOutputFormat" or \
                format_type == "SequenceFileAsBinaryOutputFormat":
            uri = path_util.to_abs_local_path(output_format.path)

            if utils.is_infinite(pcollection):
                if not path_util.is_hdfs_path(uri):
                    raise ValueError("That write infinite PType to local file "
                            "is not supported in MRPipeline")
                else:
                    output_format.path = self._toft_path(uri)
            else:
                if not path_util.is_hdfs_path(uri):
                    # User try to use MRPipeline to write local file, we replace original uri
                    # to a temp path on HDFS and dump the output for local FS after job is done.
                    hdfs_uri = self._tmp_hdfs_path(uri)
                    output_format.path = self._toft_path(hdfs_uri)
                    self._local_uri_infos.append({
                        'local_uri': uri,
                        'hdfs_uri': hdfs_uri,
                        'overwrite': output_format.overwrite
                    })
                    logger.debug(
                            "Write file to HDFS path: %s and dump it after job done" % hdfs_uri)
                    self._remote_temp_files.append(hdfs_uri)
                else:
                    output_format.path = self._toft_path(self._tmp_output_path(uri))
                    output_format.commit_path = self._toft_path(uri)

        return output_format

    # Override
    def add_file(self, file_path, resource_path=None, executable=False):
        """
        向Pipeline添加单个文件，使得该文件能够在运行期被访问

        Args:
          file_path(str): 需要添加的文件路径，支持本地, HDFS 路径
          resource_path (str): 远端运行时访问该文件的本地路径, 应是相对路径. 也即在远端, file_path 将会被映射
                               成该 resource_path 路径, 用户程序可以直接用该路径访问到 file_path 对应的文件
          executable (bool): 若为True，则该文件在运行期会被添加可执行属性
        """
        if path_util.is_hdfs_path(file_path.lower()):
            if executable:
                logger.warn("Set executable for cache file is not supported yet, "
                            "ignore executable property")
            self.__append_cache_file(file_path, resource_path, executable)
        else:
            self._resource.add_file(file_path, resource_path, executable)

    # Override
    def add_archive(self, file_path, resource_path=None):
        """
        向 Pipeline 添加一个 Archive 文件，使得该文件在运行期自动被解包

        Args:
        file_path (str):  需要添加的 archive 文件路径, 可以是本地文件系统或者远端 HDFS 路径
        resource_path (str): 远端运行时访问该文件解压后的路径, 该路径应为相对路径. 在远端, 用户程序可以直接用该
                             路径访问对应 archive 解压缩后的内容.
                             如果为 None, 则 resource_path 为 basename(file_path)
        """
        if not path_util.can_be_archive_path(file_path):
            raise ValueError("Only tar/jar/zip/tgz/qp like cache archive is supported")
        resource_path = os.path.basename(file_path) if resource_path is None else resource_path
        self.__append_cache_archive(file_path, resource_path)

    def __append_cache_file(self, file_path, resource_path, executable=False):
        if resource_path is None:
            resource_path = os.path.basename(file_path)

        cache_files = self._resource.cache_file_list
        if cache_files is None:
            cache_files = "%s#%s" % (file_path, resource_path)
        else:
            cache_files = "%s,%s#%s" % (cache_files, file_path, resource_path)
        # todo(yexianjin): to support executable cache file, we need to add a property in
        # spark_conf, such as __bigflow_on_spark__.cache.executable_files. Pass this property to
        # spark config in Spark Application and add executable permission during workspace
        # initialization.
        self._resource.cache_file_list = cache_files

    def __append_cache_archive(self, file_path, resource_path):
        resource_path = resource_path.rstrip("/")

        if not path_util.is_hdfs_path(file_path):
            file_path = os.path.abspath(file_path)

        cache_archives = self._resource.cache_archive_list
        if cache_archives is None:
            cache_archives = "%s#%s" % (file_path, resource_path)
        else:
            cache_archives = "%s,%s#%s" % (cache_archives, file_path, resource_path)
        self._resource.cache_archive_list = cache_archives

    def _set_python_path_in_init_hooks(self):
        """ Add egg file to executor/flume_worker's python path

        :return: None
        """

        def _set_python_path():
            import sys
            import os
            old_sys_path = set(sys.path)
            base_dir = "__bigflow_on_spark_application__/pythonlib/"
            if os.path.isdir(base_dir):
                egg_paths = [os.path.abspath(os.path.join(base_dir, path))
                             for path in os.listdir(base_dir) if path.endswith(".egg")]
                # filter none file path and already in the sys.path
                egg_paths = \
                    filter(lambda p: os.path.isfile(p) and (p not in old_sys_path), egg_paths)
                print >>sys.stderr, "Added egg paths: %s" % ":".join(egg_paths)
                # Don't inference with path[0] as it's normally current path
                sys.path[1:] = egg_paths + sys.path[1:]
                print >>sys.stderr, "Now sys path: %s" % ":".join(sys.path)

        # \0 to make sure this hook is loaded first.
        self.set_init_hook("\0python_path_hook", _set_python_path)

    # Override
    def _before_run(self):
        os.system('rm -f %s > /dev/null' % self._local_exception_path)

        if self._is_first_run:
            self._prepare_cache_archive()
            # delay the exception path setup, so we don't have to test/delete the exception if
            # pipeline never triggers computing.
            self._remote_temp_files.append(self._exception_path)
            self._set_exception_path()
            self._is_first_run = False
        super(SparkPipeline, self)._before_run()

    def _get_bigflow_python_home(self):
        bigflow_home = os.getenv("BIGFLOW_PYTHON_HOME")
        if bigflow_home is None:
            raise error.BigflowPlanningException("BIGFLOW_PYTHON_HOME is not set!")
        return bigflow_home

    def _prepare_cache_archive(self):
        logger.info("Checking PreparedArchive for Spark Pipeline...")
        existed = self._client.fs_test(self.prepared_archive_path, self._hadoop_config)
        tmp_path = self.prepared_archive_path + '-' + str(uuid.uuid4())
        self._job_config.prepared_archive_path = self.prepared_archive_path
        self._job_config.tmp_data_path = tmp_path

        if self._config['reprepare_cache_archive'] or not existed:
            if self._config['reprepare_cache_archive']:
                if not existed:
                    logger.info("Bigflow PreparedArchive does not exist")
                else:
                    logger.info("Re-prepare Bigflow PreparedArchive")
                    self._client.fs_rmr(self.prepared_archive_path, self._hadoop_config)
            import subprocess

            bigflow_home = self._get_bigflow_python_home()
            local_cache_archive = "bigflow_python_%s.tar.gz" % (str(uuid.uuid4()))
            cmd = "tar czf %s -C %s --exclude=flume/worker python_runtime flume" % (local_cache_archive, bigflow_home)
            ret = subprocess.call(cmd, shell=True)
            if ret != 0:
                raise error.BigflowPlanningException("Cannot make PreparedArchive file")
            try:
                self._client.fs_put(
                        local_cache_archive, tmp_path, self._hadoop_config)
                self._client.fs_mv(
                        tmp_path, self.prepared_archive_path, self._hadoop_config)
            except error.BigflowHDFSException:
                # only need to delete archive path when exception occurs.
                self._remote_temp_files.append(tmp_path)
                if not self._client.fs_test(self.prepared_archive_path, self._hadoop_config):
                    msg = "Unable to upload Bigflow PreparedArchive, please " \
                          "make sure you have write permission to " \
                          "tmp_data_path['%s']" % self._config['tmp_data_path']
                    raise error.BigflowHDFSException(msg)
            finally:
                ret = subprocess.call("rm %s" % local_cache_archive, shell=True)
                self._client.fs_rmr(tmp_path, self._hadoop_config)
        else:
            logger.info("Bigflow PreparedArchive exists already")

    # Override
    def _after_run(self):
        super(SparkPipeline, self)._after_run()
        for local_uri_info in self._local_uri_infos:
            local_uri = local_uri_info['local_uri']
            hdfs_uri = local_uri_info['hdfs_uri']
            if local_uri_info['overwrite']:
                logger.info("Preparing local directory: %s" % local_uri)
            if not self._force_delete_file(local_uri):
                raise error.BigflowHDFSException("Failed to remove target path: %s" % local_uri)
            else:
                if self._path_exists(local_uri):
                    raise error.BigflowHDFSException(
                        "Failed to output target path: %s, target path is existed" % local_uri)
            os.makedirs(local_uri)
            self._client.fs_get(hdfs_uri + "/*", local_uri, self._hadoop_config)
        self._local_uri_infos = []
        if SparkPipeline.output_dir_conf_key in self._config["spark_conf"]:
            del self._config["spark_conf"][SparkPipeline.output_dir_conf_key]


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

    def _handle_serialized_exception(self):
        if self._path_exists(self._exception_path):
            local_path = self._local_exception_path
            self._client.fs_get(self._exception_path, local_path, self._hadoop_config)
            import cPickle
            e = cPickle.load(open(local_path))
            if isinstance(e, str):
                raise error.BigflowRuntimeException(e) #>>> The stack uppon this is useless >>>
            elif isinstance(e, Exception):
                raise e #>>> The stack uppon this is useless, please see the error below: >>>
            else:
                raise error.BigflowRuntimeException(
                    "Unknow error, please contact us. bigflow@baidu.com.\nerr_msg = %s" % str(e)
                )
        return True

    def __del__(self):

        requests.unregister_pipeline(self.id())
        logger.debug("Unregister Pipeline %s OK" % self.id())
        self._delete_remote_temp_files()
        super(SparkPipeline, self).__del__()

    def _delete_remote_temp_files(self):
        """
        force delete the remote temp files
        """
        user_provided_config = self._hadoop_config

        for f in self._remote_temp_files:
            self._client.fs_rmr(f, user_provided_config)

