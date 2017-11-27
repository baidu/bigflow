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
定义所有的数据源(Source)，用于Pipeline.read()方法

实现一个Source需要实现四个接口：

1. 有一个input_format属性，是一个flume::Loader
2. 有一个objector属性，是一个Objector
3. 有一个uris属性，返回一个uri列表
4. 有一个transform_from_node方法，把一个Node变换成一个PType
5. 有一个get_size方法，计算本文件读出数据量有多少。可以返回-1(?)表示未知大小。

"""

import subprocess

from bigflow import error
from bigflow import pcollection
from bigflow import serde
from bigflow.core import entity
from bigflow.core.serde import record_objector
from bigflow.core.serde import cloudpickle
from bigflow.util import path_util
from bigflow.util import hadoop_client
from flume.proto import entity_pb2

class UserInputBase(object):
    """ 用户输入抽象基类

        用户需要按以下方法重写split/load函数

        Eg. ::

            class LocalFileInput(UserInputBase):
                def __init__(self, dir):
                    self._dir = dir

                def split(self):
                    return [os.path.join(self._dir, filename) for filename in os.listdir(self._dir)]

                def load(split):
                    with open(split) as f:
                    for line in f.readline():
                        yield line.strip()

        用户可以重写post_process以实现一些后处理，
        post_process有一个传入参数，是一个PTable，这个PTable的key是split string,
        value是这个split上的数据。
        默认post_process方法是`bigflow.transforms.flatten_values`.
    """

    def split(self):
        """
            splits urls as some splits. User should override this method.
        """
        raise NotImplementedError()

    def load(self, split):
        """
            Load data from a split.
            The return value will be flattened into a PCollection.
        """
        raise NotImplementedError()

    def post_process(self, ptable):
        """
            User can override post_process method to do some post_process.
        """
        return ptable.flatten_values()

    def get_serde(self):
        """ User can override this method to set the serde """
        import serde
        return serde.any()

    def get_size(self):
        """ user can override this method to calculate the size of the input data """
        return -1

class _TextInputFormat(entity.EntitiedBySelf):
    def __init__(self, config=""):
        self._config = config
        pass

    def get_entity_name(self):
        return "TextInputFormat"

    def get_entity_config(self):
        return self._config


class _TextInputFormatWithUgi(entity.EntitiedBySelf):
    def __init__(self, config=""):
        self._config = config

    def get_entity_name(self):
        return "TextInputFormatWithUgi"

    def get_entity_config(self):
        return self._config


class _TextFromRecord(entity.EntitiedBySelf):
    def __init__(self):
        pass

    def get_entity_name(self):
        return "PythonFromRecordProcessor"

    def get_entity_config(self):
        return cloudpickle.dumps(self)


class _SequenceFileAsBinaryInputFormat(entity.EntitiedBySelf):
    def __init__(self, config=""):
        self._config = config
        pass

    def get_entity_name(self):
        return "SequenceFileAsBinaryInputFormat"

    def get_entity_config(self):
        return self._config


class _SequenceFileAsBinaryInputFormatWithUgi(entity.EntitiedBySelf):
    def __init__(self, config=""):
        self._config = config

    def get_entity_name(self):
        return "SequenceFileAsBinaryInputFormatWithUgi"

    def get_entity_config(self):
        return self._config


class _KVFromBinaryRecord(entity.EntitiedBySelf):
    def __init__(self):
        pass

    def get_entity_name(self):
        return "PythonKVFromRecordProcessor"

    def get_entity_config(self):
        return cloudpickle.dumps(self)


class FileBase(object):
    """
    用于Pipeline.read()方法读取文件的基类

    Args:
      *path:  读取文件的path，必须均为str或unicode类型
    """

    def __init__(self, *path, **options):
        self.uris = map(lambda p: p.replace(",", "\,"), path)
        self.objector = record_objector.RecordObjector()
        self.ugi = options.get("ugi", None)

    def get_size(self, pipeline):
        """
        获得所有读取文件在文件系统中的大小

        Returns:
          int:  文件大小，以字节为单位
        """

        def _get_file_size(uri):
            cmd = list()
            if uri.startswith("hdfs://"):
                fs_name_from_path = hadoop_client.extract_fs_name_from_path(uri)
                replace_explicit_fs_name = False

                config = pipeline.config()
                cmd.append(config.hadoop_client_path)
                cmd.append("fs")
                for kv in config.hadoop_job_conf:
                    if kv.key == "fs.defaultFS" and fs_name_from_path is not None:
                        cmd.extend(["-D", kv.key + "=" + fs_name_from_path])
                    else:
                        cmd.extend(["-D", kv.key + "=" + kv.value])
                if not replace_explicit_fs_name and fs_name_from_path is not None:
                    cmd.extend(["-D fs.defaultFS=" + fs_name_from_path])

                cmd.append("-conf %s" % config.hadoop_config_path)
                cmd.append("-dus %s | cut -f 2" % uri)
            else:
                cmd.append("du -s -b %s | cut -f 1" % uri)

            process = subprocess.Popen(" ".join(cmd), stdout=subprocess.PIPE, shell=True)
            ret = process.wait()
            if ret != 0:
                raise error.BigflowRPCException("Error getting file size for uri: %s" % uri)

            size = 0
            try:
                for line in process.stdout.readlines():
                    size += int(line.strip())
            except Exception as e:
                raise error.BigflowPlanningException("Cannot get input size", e)
            return size

        return sum(map(_get_file_size, self.uris))

    def _use_dce_combine(self, options):
        combine_multi_file = options.get("combine_multi_file", None)
        if combine_multi_file is not None:
            return combine_multi_file
        return options.get('use_dce_combine', True)


def user_define_format(user_input_base):
    """ return a FileBase object from a UserInputBase"""
    assert isinstance(user_input_base, UserInputBase)

    class _LoaderImpl(object):

        def __init__(self, user_input_base):
            self._user_input_base = user_input_base

        def split(self, uri):
            """ inner """
            return self._user_input_base.split()

        def load(self, split):
            """ inner """
            return self._user_input_base.load(split)

    class _UserDefineFileBase(FileBase):

        def __init__(self, user_input_base):
            super(_UserDefineFileBase, self).__init__('user_define_format')
            self.input_format = entity.Entity.of(
                entity.Entity.loader,
                cloudpickle.dumps(_LoaderImpl(user_input_base))
            )
            self.objector = user_input_base.get_serde()
            self._user_input_base = user_input_base

        def get_size(self, pipeline):
            """ get file size """
            return self._user_input_base.get_size()

        def transform_from_node(self, load_node, pipeline):
            """ inner func """
            from bigflow import ptable
            transformed_pcollection = pcollection.PCollection(load_node, pipeline)
            before_post_process = \
                ptable.PTable(transformed_pcollection, key_serde=serde.CPickleSerde())
            return self._user_input_base.post_process(before_post_process)

    return _UserDefineFileBase(user_input_base)

class TextFile(FileBase):
    """
    表示读取的文本文件的数据源

    Args:
      *path:  读取文件的path，必须均为str类型

    读取文件数据示例：::

        >>> lines1 = _pipeline.read(input.TextFile('hdfs:///my_hdfs_dir/'))
        >>> lines2 = _pipeline.read(input.TextFile('hdfs://host:port/my_hdfs_file'))
        >>> lines3 = _pipeline.read(input.TextFile('hdfs:///multi_path1', 'hdfs:///multi_path2'))
        >>> lines4 = _pipeline.read(input.TextFile('./local_file_by_rel_path/'))
        >>> lines5 = _pipeline.read(input.TextFile('/home/work/local_file_by_abs_path/'))
        >>> lines6 = _pipeline.read(input.TextFile(*['hdfs:///multi_path1', 'hdfs:///multi_path2']))

    **options: 其中关键参数：
          combine_multi_file: 是否可以合并多个文件到一个mapper中处理。默认为True。

          partitioned: 默认为False，如果置为True，则返回的数据集为一个ptable，
            ptable的key是split_info，value这个split上的全部数据所组成的pcollection。::

        >>> f1 = open('data1.txt', 'w')
        >>> f1.write('1 2 1')
        >>> f1.close()
        >>> f2 = open('data2.txt', 'w')
        >>> f2.write('1 2 2')
        >>> f2.close()
        >>> table = _pipeline.read(input.TextFile('./data1.txt', './data2.txt', partitioned = True))
        >>> def wordcount(p):
                return p.flat_map(lambda line: line.split()) \\
                        .group_by(lambda word: word) \\
                        .apply_values(transforms.count)

        >>> table.apply_values(wordcount).get()
        {'/home/data1.txt': {'1': 2, '2': 1}, '/home/data2.txt': {'1': 1, '2', 2}}
        # 需要注意，MR模式下，key可能不是这个格式的，切分方法也不一定是按照文件来的。
    """

    def __init__(self, *path, **options):
        super(TextFile, self).__init__(*path, **options)
        self.repeatedly = options.get("repeatedly", False)
        input_format = _TextInputFormat if not self.ugi else _TextInputFormatWithUgi
        if self.repeatedly:
            from flume.proto import entity_pb2
            pb = entity_pb2.PbInputFormatEntityConfig()
            pb.repeatedly = True
            pb.max_record_num_per_round = options.get('max_record_num_per_round', 1000)
            pb.timeout_per_round = options.get('timeout_per_round', 30)
            self.input_format = input_format(pb.SerializeToString())
        elif self._use_dce_combine(options):
            self.input_format = input_format("use_dce_combine")
        else:
            self.input_format = input_format()
        self._options = options

    def transform_from_node(self, load_node, pipeline):
        """
        内部接口
        """
        from bigflow import ptable

        if self.repeatedly:
            transformed = load_node.repeatedly() \
                .process_by(_TextFromRecord()) \
                .as_type(serde.StrSerde()) \
                .set_effective_key_num(0) \
                .input(0).allow_partial_processing() \
                .done()
        else:
            transformed = load_node \
                .process_by(_TextFromRecord()) \
                .as_type(serde.StrSerde()) \
                .set_effective_key_num(0) \
                .input(0).allow_partial_processing() \
                .done()

        transformed.set_size(load_node.size())

        if self._options.get('partitioned', False):
            transformed_pcollection = pcollection.PCollection(transformed, pipeline)
            return ptable.PTable(transformed_pcollection, key_serde=serde.StrSerde())

        return pcollection.PCollection(transformed.leave_scope(), pipeline)


class SchemaTextFile(TextFile):
    """
    读取文本文件生成支持字段操作的SchemaPCollection

    Args:
        *path:  读取文件的path, 必须均为str类型
        **options: Arbitrary keyword arguments, 其中关键参数，
            (1). 若columns(list), 每一项为字段名，则生成SchemaPCollection的元素是dict，dict中的value类型都是str;

            (2). 若columns(list), 每一项为(字段名，类型)，则生成SchemaPCollection的元素是dict，
            dict中的值类型是字段对应的类型;

            (3). 若columns(int)，表示分割的列数，则生成SchemaPCollection的元素是tuple，tuple中的每个元素的类型都是str，
            separator(str)表示每行数据字段分隔符，默认分隔符是Tab("\t");

            (4). 若columns(list), 每一项为python基本类型(int, str, float)，则生成SchemaPcollection的元素是tuple，
            每个tuple中的元素的类型和columns中的类型一一对应；separator(str)表示每行数据字段分隔符，默认分隔符是Tab("\t");

            ignore_overflow(bool)表示如果文件有多余的列，是否可以忽略掉。默认为False，即出现多余的列时即会报错。

            ignore_illegal_line(bool): 表示当文件某一行的列数少于提供的字段数时，是否可以忽略该文件行。若不设置，则抛出异常

    Example:

        >>> open("input-data", "w").write("XiaoA\\t20\\nXiaoB\\t21\\n")
        >>> persons = _pipeline.read(input.SchemaTextFile("input-data", columns = ['name', 'age']))
        >>> persons.get()
        [{'age': '20', 'name': 'XiaoA'}, {'age': '21', 'name': 'XiaoB'}]

        >>> open("input-data", "w").write("XiaoA\\t20\\nXiaoB\\t21\\n")
        >>> persons = _pipeline.read(input.SchemaTextFile("input-data", columns = [('name', str), ('age', int)]))
        >>> persons.get()
        [{'age': 20, 'name': 'XiaoA'}, {'age': 21, 'name': 'XiaoB'}]

        >>> open("temp_data.txt", "w").write("1\\t2.0\\tbiflow\\n10\\t20.10\\tinf")
        >>> data = p.read(input.SchemaTextFile("./temp_data.txt", columns=3))
        >>> data.get()
        [('1', '2.0', 'biflow'), ('10', '20.1', 'inf')]

        >>> open("temp_data.txt", "w").write("1\\t2.0\\tbiflow\\n10\\t20.10\\tinf")
        >>> data = p.read(input.SchemaTextFile("./temp_data.txt", columns=[int, float, str]))
        >>> data.get()
        [(1, 2.0, 'biflow'), (10, 20.1, 'inf')]
    """

    def __init__(self, *path, **options):
        super(SchemaTextFile, self).__init__(*path, **options)
        self.fields = options.get('columns', None)
        self.sep = options.get('separator', "\t")
        self.ignore_overflow = options.get('ignore_overflow', False)
        self.ignore_illegal_line = options.get('ignore_illegal_line', False)

    def transform_from_node(self, load_node, pipeline):
        """
        内部接口
        """
        from bigflow import schema
        if self.fields is None:
            raise ValueError('''columns is necessary，(1) columns(list)，
                each item in columns is string, SchemaPCollection's element
                is dict, (2) columns(int)，SchemaPCollection's element is tuple. eg.
                columns=3 or columns=[(xx, int), (yy, str)] or columns=[xx, yy],
                (3) columns(list), each item in columns is base type in [int, float, str]''')

        if isinstance(self.fields, tuple):
            self.fields = list(self.fields)

        fields_type = []
        ignore_overflow = self.ignore_overflow
        ignore_illegal_line = self.ignore_illegal_line
        if isinstance(self.fields, list):
            def get_fields_type(fields):
                """内部函数"""
                fields_type = []
                for field in fields:
                    if isinstance(field, tuple):
                        if field[1] in [int, str, float]:
                            fields_type.append(field[1])
                        else:
                            raise ValueError('''columns is list(field name or data type),
                                             data type(int/str/float)''')
                    elif field in [int, str, float]:
                        fields_type.append(field)
                    elif isinstance(field, str):
                        fields_type.append(str)
                    else:
                        raise ValueError('''columns is list(field name or data type),
                                         data type(int/str/float)''')
                return fields_type
            fields_type = get_fields_type(self.fields)
            ret = super(SchemaTextFile, self)\
                    .transform_from_node(load_node, pipeline)\
                    .flat_map(entity.SplitStringToTypes(self.sep,
                                                        fields_type,
                                                        ignore_overflow,
                                                        ignore_illegal_line),
                                                        serde=serde.of(tuple(fields_type)))
            if self.fields[0] in [int, float, str]:
                return ret
            else:
                ret = ret.apply(schema.tuple_to_dict, self.fields)
                return ret
        elif isinstance(self.fields, int):
            from bigflow import schema_pcollection
            return schema_pcollection.SchemaPCollection(super(SchemaTextFile, self)
                .transform_from_node(load_node, pipeline)\
                .flat_map(entity.SplitStringToTypes(self.sep,
                                                    [str for _ in xrange(self.fields)],
                                                    True,
                                                    ignore_illegal_line),
                          serde=serde.of(tuple(serde.StrSerde() for index in xrange(self.fields)))))
        else:
            raise ValueError("columns is list(field name)，or int(row number)")

class SequenceFile(FileBase):
    """
    表示读取SequenceFile的数据源，SequenceFile的(Key, Value)必须均为BytesWritable，并由用户自行解析

    Args:
      *path:  读取文件的path，必须均为str类型
      **options: 其中关键参数：
          combine_multi_file: 是否可以合并多个文件到一个mapper中处理。默认为True。
          partitioned: 默认为False，如果置为True，则返回的数据集为一个ptable，
             ptable的key是split_info，value这个split上的全部数据所组成的pcollection。
          key_serde: key如何反序列化
          value_serde: value如何反序列化
          如果未设定key_serde/value_serde，则会忽略掉key，只把value用默认序列化器反序列化并返回。

    Example:
        >>> from bigflow import serde
        >>> StrSerde = serde.StrSerde
        >>> lines = _pipeline.read(
                input.SequenceFile('path', key_serde=StrSerde(), value_serde=StrSerde()))
        >>> lines.get()
        [("key1", "value1"), ("key2", "value2")]

        >>> import mytest_proto_pb2
        >>> msg_type = mytest_proto_pb2.MyTestPbType
        >>> _pipeline.add_file("mytest_proto_pb2.py", "mytest_proto_pb2.py")
        >>> pbs = _pipeline.read(input.SequenceFile('path2', serde=serde.ProtobufSerde(msg_type)))
        >>> pbs.get()  # 如果未设置key_serde/value_serde，则key会被丢弃。
        >>> [<mytest_proto_pb2.MyTestPbType at 0x7fa9e262a870>,
             <mytest_proto_pb2.MyTestPbType at 0x7fa9e262a870>]

        有时，Pb包在本地没有，例如，py文件在hdfs，则可以使用下边的方法：

        >>> _pipeline.add_archive("hdfs:///proto.tar.gz", "proto") #add_archive暂时不支持本地模式
        >>> def get_pb_msg_creator(module_name, class_name):
        ...     import importlib
        ...     return lambda: importlib.import_module(module_name).__dict__[class_name]()
        >>> pbs = _pipeline.read(input.SequenceFile('path2', serde=serde.ProtobufSerde(get_pb_msg_creator("proto.mytest_proto_pb2", "MyTestPbType"))))
        >>> pbs.get()
        >>> [<mytest_proto_pb2.MyTestPbType at 0x7fa9e262a870>,
             <mytest_proto_pb2.MyTestPbType at 0x7fa9e262a870>]
    如果需要自定义Serde，参见：:class:`bigflow.serde.Serde`。

    """

    def __init__(self, *path, **options):
        super(SequenceFile, self).__init__(*path, **options)
        self.repeatedly = options.get("repeatedly", False)
        input_format = _SequenceFileAsBinaryInputFormat if not self.ugi \
                else _SequenceFileAsBinaryInputFormatWithUgi
        if self.repeatedly:
            from flume.proto import entity_pb2
            pb = entity_pb2.PbInputFormatEntityConfig()
            pb.repeatedly = True
            pb.max_record_num_per_round = options.get('max_record_num_per_round', 1000)
            pb.timeout_per_round = options.get('timeout_per_round', 30)
            self.input_format = input_format(pb.SerializeToString())
        elif self._use_dce_combine(options):
            self.input_format = input_format("use_dce_combine")
        else:
            self.input_format = input_format()

        # 只有当用户把value_serde和key_serde都设置或者都不设置时才会生效
        # 否则抛出错误
        k_serde = options.get("key_serde", None)
        v_serde = options.get("value_serde", None)

        if (not k_serde) != (not v_serde):
            raise error.InvalidSeqSerdeException("key and value serde should be both set or not.")
        elif (k_serde is not None) and (v_serde is not None):
            self.kv_deserializer = entity.KVDeserializeFn(k_serde, v_serde)
        else:
            self.kv_deserializer = None
        self._options = options

    def as_type(self, kv_deserializer):
        """
        通过kv_deserializer反序列化读取的(Key, Value)

        kv_deserializer的期望签名为:

        kv_deserializer(key: str, value: str) => object
        """
        self.kv_deserializer = kv_deserializer
        return self

    def transform_from_node(self, load_node, pipeline):
        """
        内部接口
        """
        from bigflow import ptable
        if self.repeatedly:
            transformed = load_node.repeatedly() \
                .process_by(_KVFromBinaryRecord()) \
                .as_type(serde.tuple_of(serde.StrSerde(), serde.StrSerde())) \
                .set_effective_key_num(0) \
                .input(0).allow_partial_processing() \
                .done()
        else:
            transformed = load_node \
                .process_by(_KVFromBinaryRecord()) \
                .as_type(serde.tuple_of(serde.StrSerde(), serde.StrSerde())) \
                .set_effective_key_num(0) \
                .ignore_group() \
                .input(0).allow_partial_processing() \
                .done()

        transformed.set_size(load_node.size())

        transformed = pcollection.PCollection(transformed, pipeline)

        tserde = self._options.get('serde', pipeline.default_objector())

        if self.kv_deserializer is not None:
            transformed = transformed.map(self.kv_deserializer, serde = tserde)
        else:
            is_serialize = False
            deserialize = entity.SerdeWrapper(tserde, is_serialize, 1)
            transformed = transformed.map(deserialize, serde = tserde)

        if self._options.get('partitioned'):
            return ptable.PTable(transformed, key_serde=serde.StrSerde())
        return pcollection.PCollection(transformed.node().leave_scope(), pipeline)


class _TextStreamInputFormat(entity.EntitiedBySelf):
    def __init__(self, config):
        """ 内部方法 """
        self._config = config

    def get_entity_name(self):
        """ 内部方法 """
        return "TextStreamInputFormat"

    def get_entity_config(self):
        """ 内部方法 """
        return self._config


class TextFileStream(FileBase):
    """
    表示读取的文本文件的无穷数据源。

    Args:
      *path:  读取文件目录的path，必须均为str类型

    读取文件数据示例：::

        >>> lines1 = _pipeline.read(input.TextFileStream('hdfs:///my_hdfs_dir/'))
        >>> lines2 = _pipeline.read(input.TextFileStream('hdfs://host:port/my_hdfs_dir/'))
        >>> lines3 = _pipeline.read(input.TextFileStream('hdfs:///multi_path1', 'hdfs:///multi_path2'))
        >>> lines4 = _pipeline.read(input.TextFileStream('./local_file_by_rel_path/'))
        >>> lines5 = _pipeline.read(input.TextFileStream('/home/work/local_file_by_abs_path/'))
        >>> lines6 = _pipeline.read(input.TextFileStream(*['hdfs:///multi_path1', 'hdfs:///multi_path2']))

      **options: 可选的参数。

          [Hint] max_record_num_per_round: 用于指定每轮订阅的日志条数，默认值为1000

          [Hint] timeout_per_round: 用于指定每轮订阅的超时时间（单位为s），默认为10s

    Note:
        1. 如果path中含有子目录，则以子目录作为数据源；如果path中没有子目录，则以path作为数据源

        2. 目录中有效的文件名为从0开始的正整数；如果文件不存在，会一直等待该文件

        3. 目录中的文件不允许被修改，添加需要保证原子（可以先写成其他文件名，然后进行mv）
    """
    def __init__(self, *path, **options):
        """ 内部方法 """
        super(TextFileStream, self).__init__(*path)
        from flume.proto import entity_pb2
        pb = entity_pb2.PbInputFormatEntityConfig()
        pb.repeatedly = True
        pb.max_record_num_per_round = options.get('max_record_num_per_round', 1000)
        pb.timeout_per_round = options.get('timeout_per_round', 30)
        pb.file_stream.filename_pattern = options.get('filename_pattern', 'default').lower()
        self.input_format = _TextStreamInputFormat(pb.SerializeToString())

    def transform_from_node(self, load_node, pipeline):
        """ 内部接口 """
        transformed = load_node.repeatedly() \
            .process_by(_TextFromRecord()) \
            .as_type(serde.StrSerde()) \
            .set_effective_key_num(0) \
            .input(0).allow_partial_processing() \
            .done()

        transformed.set_size(load_node.size())

        return pcollection.PCollection(transformed.leave_scope(), pipeline)


class _SequenceStreamInputFormat(entity.EntitiedBySelf):
    def __init__(self, config):
        """ 内部方法 """
        self._config = config

    def get_entity_name(self):
        """ 内部方法 """
        return "SequenceStreamInputFormat"

    def get_entity_config(self):
        """ 内部方法 """
        return self._config


class SequenceFileStream(FileBase):
    """
    表示读取SequenceFile的无穷数据源，SequenceFile的(Key, Value)必须均为BytesWritable，并由用户自行解析

    Args:

      *path:  读取文件的path，必须均为str类型

      **options: 可选的参数。

          [Hint] max_record_num_per_round: 用于指定每轮订阅的日志条数，默认值为1000

          [Hint] timeout_per_round: 用于指定每轮订阅的超时时间（单位为s），默认为10s

          key_serde: key如何反序列化

          value_serde: value如何反序列化

          如果未设定key_serde/value_serde，则会忽略掉key，只把value用默认序列化器反序列化并返回。

    Note:
        1. 如果path中含有子目录，则以子目录作为数据源；如果path中没有子目录，则以path作为数据源

        2. 目录中有效的文件名为从0开始的正整数；如果文件不存在，会一直等待该文件

        3. 目录中的文件不允许被修改，添加需要保证原子（可以先写成其他文件名，然后进行mv）
    """
    def __init__(self, *path, **options):
        """ 内部方法 """
        super(SequenceFileStream, self).__init__(*path)
        from flume.proto import entity_pb2
        pb = entity_pb2.PbInputFormatEntityConfig()
        pb.repeatedly = True
        pb.max_record_num_per_round = options.get('max_record_num_per_round', 1000)
        pb.timeout_per_round = options.get('timeout_per_round', 30)
        pb.file_stream.filename_pattern = options.get('filename_pattern', 'default').lower()
        self.input_format = _SequenceStreamInputFormat(pb.SerializeToString())

        # 只有当用户把value_serde和key_serde都设置或者都不设置时才会生效
        # 否则抛出错误
        k_serde = options.get("key_serde", None)
        v_serde = options.get("value_serde", None)

        if (not k_serde) != (not v_serde):
            raise error.InvalidSeqSerdeException("key and value serde should be both set or not.")
        elif (k_serde is not None) and (v_serde is not None):
            self.kv_deserializer = entity.KVDeserializeFn(k_serde, v_serde)
        else:
            self.kv_deserializer = None
        self._options = options

    def as_type(self, kv_deserializer):
        """
        通过kv_deserializer反序列化读取的(Key, Value)

        kv_deserializer的期望签名为:

        kv_deserializer(key: str, value: str) => object
        """
        self.kv_deserializer = kv_deserializer
        return self

    def transform_from_node(self, load_node, pipeline):
        """ 内部方法 """
        transformed = load_node.repeatedly() \
            .process_by(_KVFromBinaryRecord()) \
            .as_type(serde.tuple_of(serde.StrSerde(), serde.StrSerde())) \
            .set_effective_key_num(0) \
            .input(0).allow_partial_processing() \
            .done()

        transformed.set_size(load_node.size())

        transformed = pcollection.PCollection(transformed, pipeline)

        tserde = self._options.get('serde', pipeline.default_objector())

        if self.kv_deserializer is not None:
            transformed = transformed.map(self.kv_deserializer, serde = tserde)
        else:
            is_serialize = False
            deserialize = entity.SerdeWrapper(tserde, is_serialize, 1)
            transformed = transformed.map(deserialize, serde = tserde)

        return pcollection.PCollection(transformed.node().leave_scope(), pipeline)
