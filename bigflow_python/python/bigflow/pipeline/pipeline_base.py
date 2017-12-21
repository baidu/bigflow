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
Pipeline基类定义

"""

import os
import shutil
import uuid

import bigflow
from bigflow import error, input, ptable
from bigflow import counter
from bigflow.core import logical_plan
from bigflow.core import entity
from bigflow.rpc import requests
from bigflow.runtime import python_resource
from bigflow.util import hadoop_client
from bigflow.util import path_util
from bigflow.util import utils
from bigflow.util.log import logger


class PipelineBase(object):
    """
    Pipeline基类
    """
    def __init__(self, **options):
        self._hadoop_client_instance = None
        self._id = str(uuid.uuid4())
        self._plan = logical_plan.LogicalPlan()
        self._plan.set_environment(entity.PythonEnvironment())
        self._resource = python_resource.Resource()
        self._type_str = "RawPipeline"

        self.estimate_concurrency = False
        self._job_config = None

        # todo(yexianjin): determine to use list or set
        self._local_temp_files = list()
        self._remote_temp_files = list()
        self._cache_node_ids = list()
        self._uri_to_write = list()

        # pipeline before and after run hooks
        self._before_run_hooks = dict()
        self._after_run_hooks = dict()

        self._init_hooks = dict()
        self._fini_hooks = dict()

        self._hadoop_conf = {}
        if options.has_key('hadoop_conf'):
            self._hadoop_conf = options.get('hadoop_conf')
        elif options.has_key('hadoop_job_conf'):
            self._hadoop_conf = options.get('hadoop_job_conf')

        # registered extensions
        self._extensions = []

        self._add_bigflow_path()

        self._set_sys_defaultencoding(options.get('pass_default_encoding_to_remote', None))


    @property
    def _hadoop_config(self):
        return self._hadoop_conf

    def _get_default_job_name(self):
        """ inner function """
        import sys
        if len(sys.argv) < 1 or len(sys.argv[0].strip()) == 0:
            return "bigflow_python_interactive_at_" + os.getcwd()
        else:
            return ' '.join(sys.argv)

    def plan(self, cache_id):
        return self._plan

    def add_cache_id(self, cache_id):
        """
        save the ptype cache node id for use
        """
        if not isinstance(cache_id, str):
            raise error.BigflowPlanningException("be added cache id should be str")
        self._cache_node_ids.append(cache_id)

    def add_file(self, source, resource_path=None, executable=False, from_bytes=False):
        """
        向Pipeline添加单个文件，使得该文件能够在运行期被访问

        Args:
          source (str):  需要添加的文件路径或者比特数据流
          resource_path (str): 计算引擎运行时访问该文件的本地路径, 应是相对路径. 也即在远端, source 将会被映射
                               成该 resource_path 路径, 用户程序可以直接用该路径访问.
          executable (bool): 若为True，则该文件在运行期会被添加可执行属性
          from_bytes (bool): 若为True, 则表示远端文件的内容即为 source 的比特数据流, 默认为 False, source
                             应为一个文件路径

        ::

            >> pipeline.add_file('/path/to/local/or/hdfs/file', './target_path_a')
            >> def remote_filter(self):
            ..     # This is a dumb example, don't do this in production,
            ..     import os
            ..     return os.path.isfile('./target_path_a')
            >> pc = pipeline.parallelize([1,2,3])
            >> filtered_pc = pc.filter(remote_filter)
            >> print pc.diff(filtered_pc).count().get()
            .. 0
        """
        if from_bytes:
            self._resource.add_file_from_bytes(source, resource_path, executable)
        else:
            self._resource.add_file(source, resource_path, executable)

    def add_egg_file(self, file_path):
        """
        向Pipeline添加一个egg文件，使得该文件会在运行期自动被添加到PYTHONPATH中

        Args:
          file_path (str):  egg文件路径
        """
        self._resource.add_egg_file(file_path)

    def add_directory(self, dir_path, resource_path="", is_only_python_files_accepted=False):
        """
        向Pipeline添加一个目录，使得该目录下的文件/子目录能够在运行期被访问

        Args:
          dir_path (str):  需要添加的本地目录路径
          resource_path (str):  计算引擎运行时访问该目录的路径, 应是相对路径. 如未提供, 则 dir_path 下的所有
                                文件和子目录将会在远端的当前目录.
          is_only_python_files_accepted (bool):  是否仅添加目录下的.py/.pyc/.egg文件，默认为False

        .. note:: 如 resource_path 未提供, 且调用了多次 add_directory, 各个目录下的文件或者子目录如果存在重
                  名, 则为未定义行为. 有可能在添加时就出错, 也有可能在远端启动出错. 为避免该情况, 可以为每个要添
                  加的 dir_path 设置惟一的 resource_path.
        """
        self._resource.add_directory(dir_path, resource_path, is_only_python_files_accepted)

    def add_archive(self, file_path, resource_path):
        """
        向Pipeline添加一个压缩文件，使得该文件在运行期自动被解包

        Args:
          file_path (str):  文件路径，目前仅支持HDFS
          resource_path (str):  运行期访问该文件解压后的路径
        """
        raise NotImplementedError("Not supported for this Pipeline type")

    def set_init_hook(self, name, fn):
        """
        向Pipeline设置一个初始化钩子，使得Pipeline能够在进程启动时依次执行(按钩子名字排序)
        Args:
          name (str):  钩子名称, 用户应使用 str 作为钩子名字, 同时钩子应使用 ascii 码中字符.
          fn (callable):  方法名称
        """
        self._init_hooks[name] = (fn, )

    def set_fini_hook(self, name, fn):
        """
        向Pipeline设置一个结束钩子，使得Pipeline能够在进程结束前依次执行
        Args:
          name (str):  钩子名称
          fn (callable):  方法名称
        """
        self._fini_hooks[name] = (fn, )

    def config(self):
        """
        获得job config

        Returns:
          JobConfig:  用户不应当修改此Pipeline job config
        """
        return self._job_config

    def default_objector(self):
        """
        返回该Pipeline的默认序列化/反序列化器

        Returns:
          Objector:  序列化/反序列化器，用户不应当修改此objector
        """
        return self._config['default_serde']

    def read(self, source):
        """
        将外部存储的数据映射为一个PCollection，并在运行时读取数据

        Args:
          source (Source):  表示外部存储的Source实例

        Returns:
          PCollection:  读取结果
        """
        from bigflow import input
        #from bigflow.io import ComplexInputFormat
        if isinstance(source, input.UserInputBase):
            source = input.user_define_format(source)
        #elif isinstance(source, ComplexInputFormat):
        #    return source.load_by(self)

        from bigflow.core import entity
        input_format_entity = entity.Entity.of(entity.Entity.loader, source.input_format)
        ugi = source.ugi if hasattr(source, "ugi") else None
        uris = map(lambda uri: self._transform_uri(uri, input_format_entity.name, ugi),
                   source.uris)

        def _sum(arr, b):
            if not isinstance(b, list):
                arr.append(b)
            else:
                arr.extend(b)
            return arr
        uris = reduce(_sum, uris, [])

        load_node = self._plan.load(uris) \
            .by(source.input_format) \
            .as_type(source.objector)

        if self.estimate_concurrency:
            load_node = load_node.set_size(source.get_size(self))

        return source.transform_from_node(load_node, self)

    def _register_extension(self, extension):
        """ Experimental interface to register an extension

        :param extension:
        :return:
        """
        from bigflow.extension import BigFlowExtension
        if not isinstance(extension, BigFlowExtension):
            raise error.BigflowRuntimeException("Extension must be subclass of "
                                                "bigflow.extension.BigFlowExtension")
        else:
            extension.setup(self)  # setup the extension
            self._extensions.append(extension)

    def run(self):
        """
        立刻运行Pipeline并等待结束

        Raises:
          BigflowRuntimeException:  若运行期出错抛出此异常
        """
        raise error.BigflowRuntimeException("run is not supported in this pipeline")

    def async_run(self):
        """
        立刻运行Pipeline并等待作业提交完成

        Raises:
            BigflowRuntimeException:  若运行期出错抛出此异常
        """
        raise error.BigflowRuntimeException("async_run isn't be supported in this pipeline")

    def write(self, pcollection, target):
        """
        将一个PCollection映射为外部存储数据，并在运行期写到该外部存储

        Args:
          pcollection (PCollection):  要写出的PCollection
          target (Target):  表示外部存储的Target实例
        """
        from bigflow import output
        #from bigflow.io import ComplexOutputFormat

        if isinstance(target, output.UserOutputBase):
            target = output.user_define_format(target)
        elif isinstance(target, output.FileBase):
            self._uri_to_write.append(target.output_format.path)
            target.output_format = self._transform_output_format(pcollection, target.output_format)
        #elif isinstance(target, ComplexOutputFormat):
        #    return target.write(self, pcollection)

        output_node = target.transform_to_node(pcollection)
        return output_node.sink_by(target.output_format)

    def parallelize(self, dataset, **options):
        """
        将一段内存变量映射为一个P类型实例

        Args:
          dataset (object):  任意类型的内存变量
          options:
                serde: 设置dataset的serde对象

        Returns:
          PType:  表示该内存变量的P类型
        """
        objector = options.get("serde", self.default_objector())

        local_input_path = "./.local_input"
        if os.path.isfile(local_input_path):
            raise error.BigflowPlanningException("file ./.local_input exist, "
                                                  "cannot use it as temp directory")
        if not os.path.exists(local_input_path):
            os.makedirs(local_input_path)

        file_name = os.path.abspath(local_input_path + "/" + str(uuid.uuid4()))
        requests.write_record(file_name,
                              utils.flatten_runtime_value(dataset),
                              objector)

        self._local_temp_files.append(file_name)

        node = self.read(input.SequenceFile(file_name, **options)).node()

        nested_level, ptype = utils.detect_ptype(dataset)

        if nested_level < 0:
            return utils.construct(self, node, ptype)
        else:
            from bigflow.transform_impls import group_by

            for i in range(0, nested_level + 1):
                node = group_by.node_group_by(
                        node,
                        lambda x: x[0],
                        lambda x: x[1] if len(x) == 2 else x[1:len(x)],
                        key_serde=self.default_objector(),
                        value_serde=self.default_objector())

            return utils.construct(self, node, ptable.PTable, nested_level, ptype)

    def get(self, pvalue):
        """
        将一个P类型表示的数据汇聚为内存变量，相当于调用pvalue.get()。改方法隐式调用pvalue.cache()
        并立即触发Pipeline.run()

        Args:
          pvalue (PType):  P类型实例

        Returns:
          object:  内存变量
        """
        return pvalue.get()

    def id(self):
        """
        得到表示该Pipeline的唯一ID

        Returns:
          int:  Pipeline ID
        """
        return self._id

    def reset_counter(self, name):
        """
        将一个counter清零, 若 name 中不包含 group 部分, 则默认将 Flume group 下面对应的 counter 清零

        Args:
          name (str):  counter名称，其说明请参考 :mod:`counter模块<bigflow.counter>`

        Raises:
          error.BigflowRuntimeException:  此方法不允许在 :mod:`Bigflow变换<bigflow.transforms>` 的用户自定义方法(UDF)中调用，否则抛出此异常
        """
        if os.getenv("__PYTHON_IN_REMOTE_SIDE", None) is not None:
            raise error.BigflowRuntimeException("reset_counter cannot be called at runtime")
        requests.reset_counter(name)

    def reset_all_counters(self):
        """
        将所有counter清零

        Raises:
          error.BigflowRuntimeException:  此方法不允许在 :mod:`Bigflow变换<bigflow.transforms>` 的用户自定义方法(UDF)中调用，否则抛出此异常
        """
        if os.getenv("__PYTHON_IN_REMOTE_SIDE", None) is not None:
            raise error.BigflowRuntimeException("reset_all_counters cannot be called at runtime")
        requests.reset_all_counters()

    def _add_bigflow_path(self):
        base_path = os.path.abspath(os.path.dirname(bigflow.__file__))
        self.add_egg_file(base_path + "/depend/protobuf-2.5.0-py2.7.egg")
        self.add_directory(base_path, "bigflow", True)
        self.add_directory(base_path + "_python", "bigflow_python", True)
        flume_proto_path = os.path.join(os.path.dirname(base_path), 'flume/proto')
        self.add_directory(flume_proto_path, "flume/proto", True)
        flume_init_py = os.path.join(os.path.dirname(base_path), 'flume/__init__.py')
        self.add_file(flume_init_py, "flume/__init__.py")

    def _set_before_run_hook(self, name, callback):
        """ 注册一个在 pipeline.run() 执行之前的 hook.
            hook 执行顺序: 注册的 name 进行 sorted 排序结果

        todo: deal with callback with parameters. Users can always use closure to convert a callback
              with parameters to a zero-parameter callback
        :param name:  钩子名称
        :param callback: 无参的 callback
        :return: None

        ..Note: This function is provided for advanced usage, please make sure you know what you are
                doing.
        """
        if callable(callback):
            self._before_run_hooks[name] = (callback, )
        else:
            raise error.BigflowPlanningException("Cannot register a non-callable object: %s" %
                                                 str(callback))

    def _before_run(self):
        for key in sorted(self._before_run_hooks.keys()):
            callback = self._before_run_hooks[key][0]
            callback()
        self._before_run_hooks = dict()
        # register hooks in extensions
        for extension in self._extensions:
            extension.register_hooks(self)
        # generate message
        self._generate_plan_message()
        self._generate_resource_message()

    def _set_sys_defaultencoding(self, will_pass_encoding=None):
        """
            Pass sys default encoding to remote side.
        """
        import sys
        if will_pass_encoding is None:
            """
            By default,
            if user has reloaded module "sys", we should pass defaultencoding to the remote side.
            Because "reload(sys)" and "sys.setdefaultencoding(encoding)" are most probably related.

            TODO(zhangyuncong):
                We should have a further discussion about
                whether we should pass the encoding by default or not.
            """
            will_pass_encoding = hasattr(sys, 'setdefaultencoding')

        if will_pass_encoding:
            default_encoding = sys.getdefaultencoding()
            logger.warn("pass defaultencoding %s to the remote side" % default_encoding)
            def set_default_encoding():
                import sys
                reload(sys)
                sys.setdefaultencoding(default_encoding)

            # \0 makes sure this hook will run before user's
            self.set_init_hook('\0set_sys_defaultencoding', set_default_encoding)

    def _set_after_run_hook(self, name, callback):
        """ 注册一个在 pipeline.run() 执行之后的 hook.
            hook 执行顺序: 注册的 name 进行 sorted 排序结果

        todo: deal with callback with parameters. Users can always use closure to convert a callback
              with parameters to a zero-parameter callback
        :param name: 钩子名称
        :param callback: 无参的 callback
        :return: None

        ..Note: This function is provided for advanced usage, please make sure you know what you are
                doing.
        """
        if callable(callback):
            self._after_run_hooks[name] = (callback, )
        else:
            raise error.BigflowPlanningException("Cannot register a non-callable object: %s" %
                                                 str(callback))

    def _after_run(self):
        # do not clear entity folder cause get_cache_data require this folder
        # self._clear_entity_folder()
        #self._print_counters()
        self._handle_new_writtens()
        for key in sorted(self._after_run_hooks.keys()):
            callback = self._after_run_hooks[key][0]
            callback()
        self._after_run_hooks = dict()

    def _print_counters(self):
        # print counters after run
        c_dict = counter._get_all(grouped=True)
        if len(c_dict) > 0:
            logger.info("=========================================================")
            logger.info("all counters:")
            for group in sorted(c_dict.iterkeys()):
                logger.info("\t%s:" % group)
                for k, v in c_dict[group].iteritems():
                    logger.info("\t\t%s=%d" % (k, v))

    def _handle_new_writtens(self):
        if len(self._uri_to_write) > 0:
            logger.info("=========================================================")
            logger.info("all outputs:")
            for uri in self._uri_to_write:
                logger.info("\t%s" % uri)
            self._uri_to_write[:] = []

    def _add_dynamic_library_in_path(self, lib_dir):
        for f in os.listdir(lib_dir):
            file_path = os.path.join(lib_dir, f)
            if os.path.isfile(file_path):
                logger.debug("LIB: %s" % file_path)
                self._resource.add_dynamic_library(file_path)

    def _add_python_library(self):
        python_home = os.getenv("PYTHONHOME")
        if python_home is None:
            raise ValueError("PYTHONHOME is not set")

        # todo: /solib should be sufficient, review this setting before releasing
        self._add_dynamic_library_in_path(python_home + "/solib")
        self._add_dynamic_library_in_path(python_home + "/lib")

    def _toft_path(self, path):
        return requests.get_toft_style_path(path, self._job_config.hadoop_config_path)

    def _tmp_output_path(self, uri):
        parent = os.path.dirname(uri)
        filename = os.path.basename(uri)
        return parent + "/_writting_for_" + filename + "_" + str(uuid.uuid4())

    def _hadoop_client(self):
        if self._hadoop_client_instance == None:
            self._hadoop_client_instance = hadoop_client.HadoopClient(
                    self._job_config.hadoop_client_path,
                    self._job_config.hadoop_config_path)
        return self._hadoop_client_instance

    def _force_delete_file(self, path):
        toft_path = self._toft_path(path)
        if toft_path.startswith('/hdfs/'):
            self._hadoop_client().fs_rmr(path, self._hadoop_config)
            return True
        else:
            try:
                logger.debug('rmtree %s' % path)
                shutil.rmtree(path)
            except Exception as e:
                logger.warning('%s' % e)
            return not os.path.exists(path)

    def _path_exists(self, path):
        if path_util.is_hdfs_path(path):
            return self._hadoop_client().fs_test(path, self._hadoop_config)
        return os.path.exists(path)

    def _generate_plan_message(self):
        self._plan_message = self._plan.to_proto_message()

    def _generate_resource_message(self):
        folder = entity.ENTITY_FOLDER
        if os.path.exists(folder):
            for file_name in os.listdir(folder):
                src_file = os.path.join(folder, file_name)
                target_file = os.path.join(entity.FLUME_WORKER_ENTITY_FOLDER, file_name)
                self.add_file(src_file, target_file)

        def _sorted_hooks(hooks_dict):
            result = []
            for name in sorted(hooks_dict.keys()):
                result.append(hooks_dict[name])
            return result

        import copy
        resource = copy.deepcopy(self._resource)
        from bigflow.core.serde import cloudpickle
        resource.add_file_from_bytes(
                cloudpickle.dumps(_sorted_hooks(self._init_hooks)),
                ".init_hooks")
        resource.add_file_from_bytes(
                cloudpickle.dumps(_sorted_hooks(self._fini_hooks)),
                ".fini_hooks")
        self._resource_message = resource.to_proto_message()

    def _clear_entity_folder(self):
        """do the dirty job: clear files in entity folder"""
        import subprocess
        folder = entity.ENTITY_FOLDER
        subprocess.call("command rm -f %s/*" % folder, shell=True)

    def _get_fs_conf(self, uri=None):
        """get fs.defaultFS and hadoop.job.ugi as tuple"""

        fs_defaultfs_key = "fs.defaultFS"
        hadoop_job_ugi_key = "hadoop.job.ugi"
        user_provided_config = self._hadoop_config

        fs_defaultfs = None if fs_defaultfs_key not in user_provided_config \
            else user_provided_config.get(fs_defaultfs_key)
        hadoop_job_ugi = None if hadoop_job_ugi_key not in user_provided_config \
            else user_provided_config.get(hadoop_job_ugi_key)

        return fs_defaultfs, hadoop_job_ugi

    def __str__(self):
        return "%s: %s" % (self._type_str, self._id)

    def __del__(self):
        for extension in self._extensions:
            extension.cleanup()
        pass

        self._delete_local_temp_files()

    def _delete_local_temp_files(self):
        """
        force delete the local temp files
        """
        for f in self._local_temp_files:
            path_util.rm_rf(f)

    def _delete_remote_temp_files(self):
        """
        force delete the remote temp files, which involves hadoop client
        """
        raise NotImplementedError("Concrete pipeline should implement this function to delete "
                                  "remote temp files")

    def __getstate__(self):
        """
        避免参与序列化
        """
        pass

    def __setstate__(self, state):
        """
        避免参与反序列化
        """
        pass

    def _set_exception_path(self):
        # should have self._exception_path before use this function
        script_dir = os.path.join('./.tmp/', self.id(), 'scripts')
        script_path = os.path.join(script_dir, 'set_exception_path.sh')
        os.system('mkdir -p %s' % script_dir)
        toft_style_exception_path = self._toft_path(self._exception_path)

        with open(script_path, "w") as script:
            cmd = 'export BIGFLOW_PYTHON_EXCEPTION_PATH=' + self._exception_path + '\n'
            cmd += 'export BIGFLOW_PYTHON_EXCEPTION_TOFT_STYLE_PATH=' + toft_style_exception_path
            script.write(cmd)

        return self.add_file(script_path, 'prepare/set_exception_path.sh')

if __name__ == '__main__':
    pass
