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
定义用于创建 :mod:`bigflow.pipeline` 的工厂类

.. module:: bigflow.base
   :synopsis: base entry of pipeline

"""

from bigflow.pipeline import local_pipeline
from bigflow.pipeline import spark_pipeline


__all__ = ["Pipeline", "Options", "Transformer"]


class Options(object):
    """ for all options """

    class Explain(object):
        """
            for explain options in base.Pipeline.create

            DEFAULT: Default setting.
                     At most engine (Hadoop, Spark[not support yet]),
                     the explaining file will be shown at the log directory of the 1st task of every vertex.

            ON: Explain file will be shown at all task. This may be very expensive if the explaining file is too big.

            OFF: Do not show any explaining file.

            eg. ::

                base.Pipeline.create('dagmr', explain=base.Options.Explain.ON)

        """
        DEFAULT, ON, OFF = range(3)


pipeline_dict = {"local": local_pipeline.LocalPipeline,
                 "spark": spark_pipeline.SparkPipeline}

class Pipeline:
    """
    Pipeline是用户一个分布式计算任务的抽象
    """

    def __init__(self):
        pass

    @staticmethod
    def create(pipeline_type, **job_config):
        """
        根据用户所指定的后端引擎类型以及基本配置构造一个Pipeline实例

        Args:
          pipeline_type (str):  指定Pipeline类型，目前支持``"local"``和``"hadoop"``
          **job_config:  Pipeline配置

            hadoop模式：

              job_name: 用于指定Hadoop作业名

              tmp_data_path: 用于指定保存运行包等信息的HDFS路径，请确保ugi有操作权限

              hadoop_job_conf: 用于设置Hadoop作业相关配置，优先级关系：pipeline创建时指定的参数 > bigflow自动算出的参数 > hadoop-site.xml里的参数

              default_concurrency : 默认并发数。
                  如果某级reduce数据量过小，bigflow会利用hadoop相关feature在运行时自动调小并发。

            通用配置：

                pass_default_encoding_to_remote (bool/None): 是否传递defaultencoding配置值
                （默认编码）到远端。

                默认情况下（或配置为None)，仅在sys模块被reload过的情况下（通过sys.setdefaultencoding判断），
                会将本地的sys.getdefaultencoding()传递到remote端。
                设置为True，则强制将本地编码透传至远端。
                设置为False，则不透传。

        Returns:
          Pipeline:  Pipeline实例

        创建一个local作业：
        >>> base.Pipeline.create('local')

        创建一个hadoop作业：
        >>> base.Pipeline.create('hadoop',
                job_name="test_app",
                tmp_data_path="hdfs:///app/test/",
                hadoop_job_conf={"mapred.job.map.capacity": "1000"})
        """
        pipeline_type = pipeline_type.lower()
        if pipeline_type in pipeline_dict:
            return pipeline_dict[pipeline_type](**job_config)
        else:
            raise ValueError("Unknown pipeline type")


class Transformer(object):
    """
        Transformser基类

        用户在使用
        :func:`bigflow.transforms.transform(self, data, Transformer, *side_inputs, **options)
        <bigflow.transforms.transform>`
        时，需要实现一个本类的子类，并重写相关方法，此类相关样例也见前述变换文档页。

    """

    def begin_process(self, *side_inputs):
        """
        此方法在开始处理数据之前被调用，以通知用户要开始处理数据了。

        用户必须返回一个可迭代的对象，其中值将会被放入结果的PCollection中。
        """
        return []

    def process(self, record, *side_inputs):
        """
        此方法在处理数据之时被调用，以通知用户要开始处理数据了。
        其中record即为待处理的数据。

        用户必须返回一个可迭代的对象，其中值将会被放入结果的PCollection中。
        """
        return []

    def end_process(self, *side_inputs):
        """
        此方法在结束处理数据时被调用，以通知用户要开始处理数据了。

        用户必须返回一个可迭代的对象，其中值将会被放入结果的PCollection中。
        """
        return []


