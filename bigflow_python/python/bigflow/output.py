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
定义所有的数据输出抽象(Target)，用于Pipeline.write()方法

"""

from bigflow import error
from bigflow import pcollection
from bigflow import serde
from bigflow.core import entity
from bigflow.core.serde import cloudpickle
from bigflow.core.serde import record_objector
from bigflow.util import path_util
from flume.proto import entity_pb2


class _DefaultKeyReader(object):
    def __init__(self, key_read_fn=None, reverse=False):
        self.reverse = reverse
        self.read_key = key_read_fn
        self.serialize = None


class _TextOutputFormat(entity.EntitiedBySelf):
    def __init__(self, path, **options):
        self.path = path
        self.commit_path = None
        self.compression_type = entity_pb2.PbTextOutputFormatConfig.NONE
        self.async_mode = self._get_async_mode(**options)
        self.overwrite = options.get('overwrite', True)
        self.record_delimiter = options.get("record_delimiter", "\n")
        self.ugi = options.get("ugi", None)

    def _get_async_mode(self, **options):
        async_mode = options.get('async_mode', None)
        if async_mode is None:
            import os
            async_mode = os.getenv('BIGFLOW_FILE_OUTPUT_FORMAT_ASYNC_MODE')
            if async_mode is None:
                async_mode = True
            elif 'true' == async_mode.lower():
                async_mode = True
            else:
                async_mode = False
        return async_mode

    def get_entity_name(self):
        """ inner """
        return "TextOutputFormat"

    def get_entity_config(self):
        """ inner """
        pb = entity_pb2.PbOutputFormatEntityConfig()
        pb.async_mode = self.async_mode
        pb.overwrite = self.overwrite
        pb.path = self.path
        if self.commit_path:
            pb.commit_path = self.commit_path
        pb_output_format = entity_pb2.PbTextOutputFormatConfig()
        pb_output_format.compression_type = self.compression_type
        if self.record_delimiter:
            pb_output_format.record_delimiter = self.record_delimiter
        pb.output_format_config = pb_output_format.SerializeToString()
        return pb.SerializeToString()


class _SequenceFileAsBinaryOutputFormat(entity.EntitiedBySelf):
    def __init__(self, path, **options):
        self.path = path
        self.commit_path = None
        self.async_mode = self._get_async_mode(**options)
        self.overwrite = options.get('overwrite', True)
        self.ugi = options.get('ugi', None)

    def _get_async_mode(self, **options):
        async_mode = options.get('async_mode', None)
        if async_mode is None:
            import os
            async_mode = os.getenv('BIGFLOW_FILE_OUTPUT_FORMAT_ASYNC_MODE')
            if async_mode is None:
                async_mode = True
            elif 'true' == async_mode.lower():
                async_mode = True
            else:
                async_mode = False
        return async_mode

    def get_entity_name(self):
        """ inner """
        return "SequenceFileAsBinaryOutputFormat"

    def get_entity_config(self):
        """ inner """
        pb = entity_pb2.PbOutputFormatEntityConfig()
        pb.async_mode = self.async_mode
        pb.overwrite = self.overwrite
        pb.path = self.path
        if self.commit_path:
            pb.commit_path = self.commit_path
        return pb.SerializeToString()


class _EmptyOutputFormat(entity.EntitiedBySelf):
    def __init__(self, fake_path="fake"):
        self.path = fake_path

    def get_entity_name(self):
        return "EmptyOutputFormat"

    def get_entity_config(self):
        return self.path


class _ToRecord(entity.EntitiedBySelf):
    def __init__(self, is_kv=None):
        if is_kv is None:
            self.__entity_name = "PythonToRecordProcessor"
        else:
            self.__entity_name = "PythonKVToRecordProcessor"
        pass

    def get_entity_name(self):
        return self.__entity_name

    def get_entity_config(self):
        return cloudpickle.dumps(self)


class FileBase(object):
    """
    用于Pipeline.write()方法读取文件的基类

    Args:
      path (str):  写文件的path，必须为str类型

    >>> # 对数据按照tab分割的第一个元素进行hash，将结果分割成2000个文件，并且保证每个文件内有序。
    >>> # 代码示例：
    >>> pipeline.write(
    >>>         data.map(lambda x: x),
    >>>         output.TextFile("your output dir")
    >>>             .sort()
    >>>             .partition(n=2000, partition_fn=lambda x, n: hash(x.split("\\t", 1)[0]) % n)
    >>> )
    >>>


    """
    def __init__(self, path, **options):
        if not isinstance(path, str):
            raise ValueError("path should be str")
        self.partition_number = None
        self.partition_fn = None
        self.key_reader_obj = None

        self.transform_actions = {}
        self.ugi = options.get('ugi', None)

    def sort_by(self, key_read_fn=None, reverse=False):
        """
        通过key_read_fn获取key，并根据key对数据进行排序(默认为升序)

        Args:
          key_reader_fn (callable):  用户获取key的函数
          reverse (bool):  是否降序排序

        Returns:
          FileBase:  返回self
        """
        self.transform_actions['0_sort'] = lambda p: p.sort_by(key_read_fn, reverse)

        return self

    def sort(self, reverse=False):
        """
        根据数据实际值对数据进行排序(默认为升序)

        Args:
          reverse (bool):  是否降序排序

        Returns:
          FileBase:  返回self
        """
        self.transform_actions['0_sort'] = lambda p: p.sort(reverse)
        return self

    def partition(self, n=None, partition_fn=None):
        """
        对输出结果进行分组

        Args:
          n (int):  输出结果的分组个数，具体表现为产生n个输出文件，文件内容为各组数据
          partition_fn (callable):  用于指定分组方式的函数

        Returns:
          FileBase:  返回self
        """
        self.partition_number = n
        self.partition_fn = partition_fn
        return self


class TextFile(FileBase):
    """
    输出到文本文件的Target

    Args:
      path (str):  写文件的path，必须为str类型
      **options: 其中关键参数有：
        overwrite: 如果目标位置已经存在，是否进行覆盖写操作。默认为True。
        async_mode: 是否使用异步写。默认为True。

        record_delimiter: 输出文本的分隔符，默认'\n'；
                          若指定为None，则将所有数据按字节流连续输出
    """

    compression_types = {"gzip": entity_pb2.PbTextOutputFormatConfig.GZIP}

    def __init__(self, path, **options):
        super(TextFile, self).__init__(path, **options)

        self.output_format = _TextOutputFormat(
                path_util.to_abs_local_path(path.rstrip("/")), **options)

    def transform_to_node(self, ptype):
        """
        内部接口
        """
        from bigflow.core import entity
        node = ptype.node()
        plan = node.plan()

        shuffle_scope = plan.shuffle(plan.global_scope(), [node])
        node = shuffle_scope.node(0)
        if self.partition_fn is None:
            node = node.distribute_by_default()
        else:
            node = node.distribute_by(entity.Partitioner(self.partition_fn))

        pvalue = pcollection.PCollection(node, ptype.pipeline())

        for k, action in self.transform_actions.items():
            pvalue = action(pvalue)

        node = pvalue.node()

        if self.partition_number is not None:
            shuffle_scope.with_concurrency(self.partition_number)

        if self.key_reader_obj is not None:
            node = node.sort_by(self.key_reader_obj)

        node = node.process_by(_ToRecord())\
            .as_type(record_objector.RecordObjector()) \
            .set_effective_key_num(0) \
            .input(0) \
            .done() \
            .ignore_group()

        return node

    def with_compression(self, compression_type):
        """
        对输出文件进行压缩

        Args:
          compression_type (str):  压缩格式，目前仅支持"gzip"

        Returns:
          TextFile:  返回self
        """
        if compression_type in TextFile.compression_types:
            self.output_format.compression_type = TextFile.compression_types[compression_type]
        else:
            raise error.BigflowPlanningException("Unsupported compression types,"
                     " must be one of: %s" % TextFile.compression_types.keys())

        return self


class SchemaTextFile(TextFile):
    """
    读取文本文件生成支持字段操作的SchemaPCollection

    Args:
        path (str):  写文件的path，必须为str类型
        **options: Arbitrary keyword arguments, 其中关键参数，
            若SchemaPCollection的元素是dict, 必须指定columns(list)表示输出的字段名，
            若SchemaPCollection的元素是tuple, 可以直接输出所有数据
            separator(str)表示每行数据字段分隔符，默认分隔符是Tab("\t")

    Example:
        >>> from bigflow import schema
        >>> tps = _pipeline.parallelize([('XiaoA', 20), ('XiaoB', 21)])
        >>> dicts = tps.map(lambda (name, age): {'name': name, 'age': age})
        >>> dicts = dicts.map(lambda _: _, serde=schema.of(['name', 'age'])) # 下一个版本中这行可以省去
        >>> _pipeline.write(dicts, output.SchemaTextFile('./output', columns = ['name', 'age']))
        >>> _pipeline.run()
        >>> print open('./output/part-00000').read()
        XiaoA   20
        XiaoB   21

    """

    def __init__(self, path, **options):
        super(SchemaTextFile, self).__init__(path, **options)
        self.fields = options.get('columns', None)
        self.sep = options.get('separator', "\t")

    def transform_to_node(self, ptype):
        """
        内部接口
        """
        from bigflow import schema
        if (self.fields is not None) and (not schema._is_tuple_serde(ptype.serde())):
            if isinstance(self.fields, int):
                return super(SchemaTextFile, self).transform_to_node(
                    ptype.map(lambda tp: self.sep.join(tuple(str(field) for field in tp))))
            elif schema._is_fieldsdict_serde(ptype.serde()):
                return super(SchemaTextFile, self).transform_to_node(
                    ptype.apply(schema.dict_to_tuple, self.fields)
                    .map(lambda tp: self.sep.join(tuple(str(field) for field in tp))))
            else:
                # non schema pcollection with fields
                return super(SchemaTextFile, self).transform_to_node(
                    ptype.map(lambda d: (self.sep.join(str(d.get(f)) for f in self.fields))))
        elif schema._is_tuple_serde(ptype.serde()):
            return super(SchemaTextFile, self).transform_to_node(
                ptype.map(lambda tp: self.sep.join(tuple(str(field) for field in tp))))
        else:
            raise ValueError("若SchemaPCollection的元素是dict, 必须指定columns(list)表示输出的字段名")

class SequenceFile(FileBase):
    """
    输出到SequenceFile文件的Target，SequenceFile的(Key, Value)将被写为BytesWritable，用户使用as_type()函数自行将数据序列化

    Args:
      path (str):  写文件的path，必须为str类型
      **options: 其中关键参数有：
        overwrite: 如果目标位置已经存在，是否进行覆盖写操作。默认为True。
        async_mode: 是否使用异步写。默认为True。
        key_serde: key如何被序列化为字符串。
        value_serde: value如果被序列化为字符串。
        需要注意, key_serde/value_serde如果设置，则数据必须是一个两个元素的tuple。
        如果不设置，则认为全部的数据使用默认序列化器写到sequence file的value中，key为空。
    """

    def __init__(self, path, **options):
        super(SequenceFile, self).__init__(path, **options)

        self.output_format = _SequenceFileAsBinaryOutputFormat(
                path_util.to_abs_local_path(path.rstrip("/")), **options)

        self.kv_serializer = None
        self.options = options

        # 只有当用户把value_serde和key_serde都设置或者都不设置时时才会生效
        # 否则抛出错误
        k_serde = options.get("key_serde", None)
        v_serde = options.get("value_serde", None)

        if (not k_serde) != (not v_serde):
            raise error.InvalidSeqSerdeException("key and value serde should be both set or not.")
        elif (k_serde is not None) and (v_serde is not None):
            self.kv_serializer = entity.KVSerializeFn(k_serde, v_serde)
        else:
            self.kv_serializer = None

    def as_type(self, kv_serializer):
        """
        通过kv_serializer将数据序列化为(Key, Value)

        Args:
          kv_serializer (callable):  序列化函数

        Returns:
          SequenceFile:  返回self

        .. note:: kv_deserializer的期望签名为:

        kv_deserializer(object) => (str, str)
        """
        self.kv_serializer = kv_serializer
        return self

    def transform_to_node(self, ptype):
        from bigflow.core import entity
        from bigflow import pcollection
        node = ptype.node()
        plan = node.plan()

        objector = self.options.get('serde', ptype.pipeline().default_objector())
        shuffle_scope = plan.shuffle(plan.global_scope(), [node])
        node = shuffle_scope.node(0)
        if self.partition_fn is None:
            node = node.distribute_by_default()
        else:
            node = node.distribute_by(entity.Partitioner(self.partition_fn))

        pvalue = pcollection.PCollection(node, ptype.pipeline())

        for k, action in self.transform_actions.items():
            pvalue = action(pvalue)

        node = pvalue.node()

        if self.partition_number is not None:
            shuffle_scope.with_concurrency(self.partition_number)

        if self.key_reader_obj is not None:
            node = node.sort_by(self.key_reader_obj)

        #serialize = objector.serialize
        is_serialize = True
        serialize = entity.SerdeWrapper(objector, is_serialize)
        if self.kv_serializer is not None:
            serialized = pcollection.PCollection(node, ptype.pipeline()).map(self.kv_serializer).node()
        else:
            serialized = pcollection.PCollection(node, ptype.pipeline()).map(serialize).node()

        node = serialized.process_by(_ToRecord(self.kv_serializer)) \
            .as_type(record_objector.RecordObjector()) \
            .set_effective_key_num(0) \
            .input(0) \
            .done() \
            .ignore_group()
        return node


class UserOutputBase(object):
    """
    用户Output基类
    """

    def open(self, partition):
        """
        用户可以重写该方法。
        传入参数partition表示这是第几个partition
        """
        pass

    def sink(self, data):
        """
        用户可以重写该方法。

        该方法对每条数据调用一次

        """
        raise NotImplementedError()

    def close(self):
        """
        用户可以重写该方法。
        """
        pass

    def partition_fn(self):
        """
        用户可以重写该方法。
        返回一个partition fn。
        partition_fn原型应为：(data, total_partition) => partition
        返回None则表示不太关心如何partition。

        如果partition_number

        """
        return None

    def partition_number(self):
        """
        用户可以重写该方法。
        返回一个int型的数，表示总共要把数据partition成多少份。
        """
        return None

    def pre_process(self, pval):
        """
        用户可以重写该方法。
        进行前处理，默认不处理
        """
        return pval

    def get_commiter(self):
        """
        用户可以重写该方法。
        返回一个commiter, 默认表示不需要commit阶段
        commiter应该是一个无参函数。
        """
        return None


def user_define_format(user_output_base):
    """ 内部函数 """

    class _PythonSinkerDelegator(entity.EntitiedBySelf):
        """ inner class """

        def __init__(self, user_output_base):
            key_serde = entity.Entity.of('EncodePartitionObjector', '')
            key_serde_str = key_serde.to_proto_message().SerializeToString()
            self.key_serdes = [key_serde_str]
            self._user_output_base = user_output_base
            self.path='user_define_format'

        def get_entity_config(self):
            """ 内部函数 """
            return cloudpickle.dumps(self)

        def get_entity_name(self):
            """ 内部函数 """
            return 'PythonSinkerDelegator'

        def open(self, keys):
            """ inner """
            self._user_output_base.open(keys[0])

        def sink(self, data):
            """ 内部函数 """
            self._user_output_base.sink(data)

        def close(self):
            """ 内部函数 """
            self._user_output_base.close()

        def get_commiter(self):
            """ 内部函数 """
            return self._user_output_base.get_commiter()


    class _UserOutputFormat(object):
        """ 内部类 """

        def __init__(self, user_output_base):
            self.output_format = _PythonSinkerDelegator(user_output_base)
            self._user_output_base = user_output_base
            self.is_user_define_format = True

        def transform_to_node(self, pvalue):
            """ inner func """
            from bigflow.core import entity
            pvalue = self._user_output_base.pre_process(pvalue)
            partition_fn = self._user_output_base.partition_fn()
            partition_number = self._user_output_base.partition_number()
            node = pvalue.node()
            plan = node.plan()
            shuffle_scope = plan.shuffle(plan.global_scope(), [node])
            node = shuffle_scope.node(0)

            if partition_fn is None:
                node = node.distribute_by_default()
            else:
                node = node.distribute_by(entity.Partitioner(partition_fn))

            if partition_number is not None:
                shuffle_scope.with_concurrency(partition_number)

            return node

    return _UserOutputFormat(user_output_base)

