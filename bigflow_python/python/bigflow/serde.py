#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @Author: zhangyuncong
# @Date:   2015-07-21 15:30:18
# @Last Modified by:   zhangyuncong
"""
Bigflow 内置序列化/反序列化器
"""
import sys
import struct
import marshal

from google.protobuf import message

from bigflow.core.serde import omnitypes_objector
from bigflow.core.serde import cloudpickle
from bigflow.core import entity
from bigflow import error

from bigflow_python.proto import types_pb2


class Serde(entity.EntitiedBySelf):
    """
    所有python实现的serde类要求继承于此类，用户可以通过继承Serde实现自定义的序列化/反序列化器

    >>> from bigflow import serde
    >>>
    >>> class CustomSerde(serde.Serde):
    ...     def serialize(self, obj):
    ...         assert(isinstance(obj, MyCostomObject))
    ...         return str(obj)
    ...     def deserialize(self, buf):
    ...         return MyCostomObject(buf)

    在大部分的变换中都可以传递一个特殊的叫serde的参数，
    设置该参数即可指定产出的PCollection的serde。

        p.map(lambda x: MyCostomObject(x), serde=CustomSerde())


    """
    def get_entity_name(self):
        """
        获取类名
        """
        return entity.Entity.objector

    def get_entity_config(self):
        """
        get_entity_config
        """
        return cloudpickle.dumps(self)


class ObjectorSerde(Serde):
    """
    Serde for objector serde
    """
    def __init__(self, name, config=''):
        """ init """
        super(ObjectorSerde, self).__init__()
        self._name = name
        self._config = config

    def get_entity_name(self):
        """
        获取类名
        """
        return self._name

    def get_entity_config(self):
        """
        get_entity_config
        """
        return self._config


class CppSerde(Serde):
    """所有c++实现的serde类都要求继承于此类"""
    def get_entity_name(self):
        """
        获取类名
        """
        return type(self).__name__

    def get_entity_config(self):
        """
        get_entity_config
        """
        return cloudpickle.dumps(self)


class CPickleSerde(CppSerde):
    """使用CPickle去做序列化/反序列化的Serde"""
    def __init__(self):
        import cPickle
        self.serialize = cPickle.dumps
        self.deserialize = cPickle.loads


class Optional(CppSerde):
    """
    用于支持None的序列化/反序列化
    """

    def __init__(self, serde):
        """ init """
        self._orig_serde = serde

    def origin_serde(self):
        """ origin_serde """
        return self._orig_serde

    def serialize(self, obj):
        """ serialize """
        if obj is None:
            return ''
        return '1' + self._orig_serde.serialize(obj)

    def deserialize(self, buf):
        """ deserialize """
        if buf == '':
            return None
        return self._orig_serde.deserialize(buf[1:])

    def __str__(self):
        return str(self._orig_serde)


class DefaultSerde(omnitypes_objector.OmniObjector, CppSerde):
    """
    默认的序列化/反序列化器
    """
    def __init__(self):
        """ init """
        super(DefaultSerde, self).__init__()

    def __str__(self):
        return "any()"


class IntSerde(CppSerde):
    """
    int类型序列化/反序列化器
    """
    def __init__(self):
        """ init """
        super(IntSerde, self).__init__()

    def __str__(self):
        return "int_()"

    def serialize(self, obj):
        """serialize int obj"""
        return struct.pack("<q", obj)

    def deserialize(self, buf):
        """deserialize"""
        return struct.unpack("<q", buf)[0]


class BoolSerde(CppSerde):
    """
    bool类型序列化/反序列化器
    """
    def __init__(self):
        """ init """
        super(BoolSerde, self).__init__()

    def serialize(self, obj):
        """serialize"""
        a = chr(1) if obj else chr(0)
        return struct.pack('=c', a)

    def deserialize(self, buf):
        """ deserialize """
        b = ord(struct.unpack('=c', buf)[0])
        return True if b > 0 else False

    def __str__(self):
        return "bool_()"


class FloatSerde(CppSerde):
    """
    float类型序列化/反序列化器
    """
    def __init__(self):
        """ init """
        super(FloatSerde, self).__init__()

    def serialize(self, obj):
        """serialize"""
        return struct.pack('<d', obj)

    def deserialize(self, buf):
        """ deserialize """
        return struct.unpack('<d', buf)[0]

    def __str__(self):
        return "float_()"


class StrSerde(CppSerde):
    """
    str类型序列化/反序列化器
    """
    def serialize(self, obj):
        """serialize"""
        return obj

    def deserialize(self, buf):
        """ deserialize """
        return buf

    def __str__(self):
        return "str_()"


class SameTypeListSerde(CppSerde):
    """
    list序列化/反序列化器，要求list中元素均为同一类型
    """
    def __init__(self, value):
        """ init """
        super(SameTypeListSerde, self).__init__()
        self._value = of(value)

    def __str__(self):
        return "list_of(%s)" % self._value

    def serialize(self, obj):
        """serialize"""
        result = []
        for i in obj:
            content_buf = self._value.serialize(i)
            content_size = len(content_buf)
            size_buf = struct.pack('<I', content_size)
            result.append(size_buf)
            result.append(content_buf)
        return ''.join(result)


    def deserialize(self, buf):
        """ deserialize """
        buf_len = len(buf)
        result = []
        buf_offset = 0
        while buf_offset < buf_len:
            element_size = struct.unpack("<I", buf[buf_offset:buf_offset + 4])[0]
            val = self._value.deserialize(buf[buf_offset + 4:buf_offset + 4 + element_size])
            result.append(val)
            buf_offset += 4 + element_size
        return result


class TupleLikeListSerde(CppSerde):
    """
    list序列化/反序列化器，list中每个元素可以为任意类型
    """
    def __init__(self, *value):
        """
        构造函数

        Args:
          *value:  通过已存在的对象实例构造
        """
        super(TupleLikeListSerde, self).__init__()
        self._values = map(of, value)

    def __str__(self):
        strs = map(str, self._values)
        return "list_of(%s)" % (','.join(strs))

    def serialize(self, obj):
        """serialize"""
        result = ''
        for p in zip(self._values, obj):
            element_buf = p[0].serialize(p[1])
            element_size = struct.pack("<I", len(element_buf))
            result += element_size
            result += element_buf
        return result

    def deserialize(self, buf):
        """ deserialize """
        start = 0
        result = []
        buf_len = len(buf)

        if buf_len == 0:
            raise SystemExit

        offset = 0
        values_idx = 0
        while offset < buf_len:
            element_size = struct.unpack("<I", buf[offset:offset + 4])[0]
            val = self._values[values_idx].deserialize(buf[offset + 4:offset + 4 + element_size])
            result.append(val)
            offset += 4 + element_size
            values_idx = (values_idx + 1) % len(self._values)
        return result


class ListSerde(CppSerde):
    """
    list序列化/反序列化器
    """
    def __init__(self, *value):
        """ init """
        super(ListSerde, self).__init__()
        if len(value) == 1:
            self._delegate = SameTypeListSerde(value[0])
        else:
            self._delegate = TupleLikeListSerde(*value)
        self._fields = map(of, value)
        self.serialize = self._delegate.serialize
        self.deserialize = self._delegate.deserialize

    def __str__(self):
        return str(self._delegate)

    def get_fields(self):
        """get_fields"""
        return self._fields


class TupleSerde(CppSerde):
    """
    tuple序列化/反序列化器
    """

    def __init__(self, *args):
        """ init """
        super(TupleSerde, self).__init__()
        self._args = map(of, args)

    def get_args(self):
        """get_args"""
        return self._args

    def __str__(self):
        strs = map(str, self._args)
        return "tuple_of(%s)" % (','.join(strs))

    def serialize(self, obj):
        """serialize"""
        result = ''
        for p in zip(self._args, obj):
            element_buf = p[0].serialize(p[1])
            element_size = struct.pack("<I", len(element_buf))
            result += element_size
            result += element_buf
        return result

    def deserialize(self, buf):
        """ deserialize """
        start = 0
        result = []
        buf_len = len(buf)

        if len(buf) == 0:
            raise SystemExit
        offset = 0
        args_idx = 0
        while offset < buf_len:
            element_size = struct.unpack("<I", buf[offset:offset + 4])[0]
            val = self._args[args_idx].deserialize(buf[offset + 4:offset + 4 + element_size])
            result.append(val)
            offset += 4 + element_size
            args_idx = (args_idx + 1) % len(self._args)
        return tuple(result)


class ChainSerde(CppSerde):
    """
        serde for chain transform.
        for example:
            serdex should deserialize a string unless the last serde.
            ChainSerde(serdeA, serdB);
            serdeB->serialize(serdeA->serialize(object,obj));
            serdeA->deserialize(serdeB->deserialize(buf));
    """

    def __init__(self, *args):
        """ init """
        super(ChainSerde, self).__init__()
        self._args = map(of, args)

    def get_args(self):
        """ get_args """
        return self._args

    def __str__(self):
        strs = map(str, self._args)
        return "chain_of(%s)" % (','.join(strs))

    def serialize(self, obj):
        """ serialize """
        result = ''
        tmp_obj = obj
        for serde in self._args:
            mid_obj = serde.serialize(tmp_obj)
            if mid_obj is not None:
                tmp_obj = mid_obj
            else:
                return result
        result = mid_obj
        return result

    def deserialize(self, buf):
        """ deserialize """
        result = None
        tmp_buf = buf
        serde_num = len(self._args)
        for idx in xrange(0, serde_num):
            mid_result = self._args[serde_num - idx - 1].deserialize(tmp_buf)
            if mid_result is not None:
                tmp_buf = mid_result
            else:
                return result
        result = mid_result
        return result


class IdlPacketSerde(CppSerde):
    """
    Serde for idl_packet
    """
    def __init__(self, log_type):
        """
        init
        log_type: log_text/log_bin
        """
        super(IdlPacketSerde, self).__init__()
        self._log_type = log_type

    def get_log_type(self):
        """get log_type """
        return self._log_type

    def __str__(self):
        return "idl_packet of (%s)" % self._log_type

    def serialize(self, obj):
        """serialzie"""
        raise NotImplementedError()

    def deserialize(self, buf):
        """deserialize"""
        raise NotImplementedError()


class ProtobufSerde(Serde):
    """
    Serde for protobuf
    """
    def __init__(self, msg_type, raise_exception_when_failed=True):
        """ init """
        if callable(msg_type):
            self._msg_type = msg_type
        else:
            self._msg_type = type(msg_type)

        self._raise_exception_when_failed = raise_exception_when_failed

    def get_msg_type(self):
        """get_msg_type"""
        return self._msg_type

    def serialize(self, msg):
        """serialize"""
        return msg.SerializeToString()

    def deserialize(self, buf):
        """ deserialize """
        msg = self.get_msg_type()()
        try:
            msg.ParseFromString(buf)
        except Exception as err:
            if self._raise_exception_when_failed:
                raise
            else:
                print >> sys.stderr, err
                return None
        return msg

    def __str__(self):
        return 'proto_of(%s)' % self._msg_type


class FastPbSerdeFactory(object):
    """
    Create ProtobufSerde by fast-python-pb

    >>> _serde_factory = serde.FastPbSerdeFactory(_pipeline, './sample.proto')
    >>> _serde = _serde_factory.get_serde('bigflow.MESSAGE')
    >>> data = _pipeline.read(input.SequenceFile(input_path, serde=_serde))

    """
    def __init__(self, pipeline, proto_file):
        import os
        bigflow_python_home = os.getenv("BIGFLOW_PYTHON_HOME")
        module_name = proto_file.split('/')[-1].split('.')[0]
        proto_path = '/'.join(proto_file.split('/')[:-1])
        command = "%s/bigflow/bin/fast-pb-build %s %s %s" \
                %(bigflow_python_home, module_name, proto_path, proto_file)
        result = os.system(command)
        if result != 0:
            raise error.BigflowRuntimeException("Failed to fast-pb-build")

        egg_file = '%s/pb_modules/%s/dist/proto_wrapper-1.0-py2.7-linux-x86_64.egg' \
                %(bigflow_python_home, module_name)
        pipeline.add_egg_file(egg_file)

    def get_serde(self, class_name):
        """ get protobuf serde """

        class MessageTypeDelegator(object):
            """ inner class """
            def __init__(self, class_name):
                self._class_name = class_name
                self._msg_type = None

            def __call__(self):
                if not self._msg_type:
                    module_name = '.'.join(self._class_name.split('.')[:-1])
                    msg_name = self._class_name.split('.')[-1]
                    module = __import__(module_name, globals(), locals(), [msg_name])
                    self._msg_type = module.__dict__[msg_name]
                return self._msg_type()

        delegator = MessageTypeDelegator(class_name)
        return ProtobufSerde(delegator)

class SetSerde(CppSerde):
    """
    Serde for set
    """
    def __init__(self, value):
        """ init """
        super(SetSerde, self).__init__()
        self._value = of(value)

    def __str__(self):
        return "set_of(%s)" % self._value

    def get_value_serde(self):
        """get_value_serde"""
        return self._value

    def serialize(self, obj):
        """serialize"""
        result = []
        for i in obj:
            content_buf = self._value.serialize(i)
            result_buf = struct.pack('<I', len(content_buf))
            result.append(result_buf)
            result.append(content_buf)
        return ''.join(result)

    def deserialize(self, buf):
        """ deserialize """
        buf_len = len(buf)
        result = set()
        buf_offset = 0
        while buf_offset < buf_len:
            element_size = struct.unpack("<I", buf[buf_offset:buf_offset + 4])[0]
            val = self._value.deserialize(buf[buf_offset + 4:buf_offset + 4 + element_size])
            result.add(val)
            buf_offset += 4 + element_size
        return result


class DictSerde(CppSerde):
    """
    Serde for dict
    """
    def __init__(self, key, value):
        """ init """
        super(DictSerde, self).__init__()
        self._key = of(key)
        self._value = of(value)
        self._tp_serde = list_of(tuple_of(key, value))

    def __str__(self):
        return "dict_of(%s, %s)" % (self._key, self._value)

    def serialize(self, obj):
        """serialize"""
        return self._tp_serde.serialize(obj.items())

    def deserialize(self, buf):
        """ deserialize """
        return dict(self._tp_serde.deserialize(buf))


def of(rtype):
    """
        Return the serde you want.

        If the input arg is a type instance, such as int, str, float
        or a tuple of type instances, such as (int, str)
        or a list of type instances, such as [int, str] and [int]
        the function will return the corresponding serde to serialize and deserialize the data.

        The returned serde can process the Nonable data.

        Note:   [int] means a list of ints, it can accept any number of ints you want,
                but [int, str] can only accecpt a list who has exactly 2 elements.
                (if your data to serialize has more than 2 elements, the remaining elements may be
                lost)

    """
    from bigflow.core import entity

    if hasattr(rtype, 'serialize') and hasattr(rtype, 'deserialize'):
        return rtype

    if isinstance(rtype, Serde):
        return rtype

    if isinstance(rtype, entity.EntitiedBySelf):
        raise Exception("Not Support this serde :", rtype)

    if isinstance(rtype, type) and issubclass(rtype, message.Message):
        return proto_of(rtype)


    serde = {
        tuple: lambda: tuple_of(*rtype),
        list: lambda: list_of(*tuple(rtype)),
        set: lambda: set_of(list(rtype)[0]),
        dict: lambda: dict_of(rtype.keys()[0], rtype.values()[0])
    }

    if type(rtype) in serde:
        return serde[type(rtype)]()

    types = {
        int: int_,
        float: float_,
        str: str_,
        bool: bool_
    }

    assert rtype in types, "%s(%s) can not use as a serde" % (rtype, type(rtype))
    return types[rtype]()


def sample(value):
    """
        Return a serde can process your input data.

        Note: if the sample is a list, when all the data is the same type, we assume

    """
    assert value is not None, "sample must not have None"
    assert value is not [], "sample must not have []"
    assert value is not {}, "sample must not have {}"
    assert value is not set(), "sample must not have empty set"

    if isinstance(value, dict):
        return dict_of(sample(value.keys()[0]), sample(value.values()[0]))
    if isinstance(value, set):
        return set_of(sample(list(value)[0]))
    if isinstance(value, list):
        all_same = reduce(lambda x, y: x if type(x) == type(y) != type(None) else None, value)
        if all_same:
            return list_of(type(all_same))
        else:
            return list_of(*tuple(map(sample, value)))
    if isinstance(value, tuple):
        return tuple_of(*tuple(map(sample, value)))
    return of(type(value))


def any():
    """
        Return a serde use the default serde.
    """
    return DefaultSerde()


_ = any()


def int_():
    """
        Return an optional int serde.
    """
    return Optional(IntSerde())


def float_():
    """
        Return an optional float serde.
    """
    return Optional(FloatSerde())


def str_():
    """
        Return an optional str serde.
    """
    return Optional(StrSerde())


def bool_():
    """
        Return an optional bool serde.
    """
    return Optional(BoolSerde())


def tuple_of(*args):
    """
        Return an optional tuple serde.
    """
    return Optional(TupleSerde(*args))


def list_of(*value):
    """
        Return an optional list serde.
    """
    return Optional(ListSerde(*value))


def proto_of(msg_type):
    """
        Return an protobuf serde
        Warning: this is not optional
    """
    return ProtobufSerde(msg_type)


def set_of(value):
    """
        Return an optional set serde
    """
    return Optional(SetSerde(value))


def dict_of(key, value):
    """
        Return an optional dict serde
    """
    return Optional(DictSerde(key, value))


def extract_elem(serde, n, if_not_found=None):
    """ inner function"""
    if isinstance(serde, Optional):
        ret = extract_elem(serde._orig_serde, n, None)
        if ret is None:
            return if_not_found
        if not isinstance(ret, Optional):
            return Optional(ret)
        else:
            return ret

    if isinstance(serde, TupleSerde):
        return serde._args[n]

    if isinstance(serde, ListSerde):
        return extract_elem(serde._delegate, n, if_not_found)

    if isinstance(serde, SameTypeListSerde):
        return serde._value

    if isinstance(serde, TupleLikeListSerde):
        return serde._values[n]

    #print serde, isinstance(serde, ListSerde), type(serde)
    return if_not_found


# strip tuple/list/dict/set
def origin(serde):
    """ inner function"""
    if isinstance(serde, Optional):
        return origin(serde._orig_serde)
    if isinstance(serde, TupleSerde):
        return tuple(map(origin, serde._args))
    if isinstance(serde, ListSerde):
        return origin(serde._delegate)
    if isinstance(serde, TupleLikeListSerde):
        return list(map(origin, serde._values))
    if isinstance(serde, SameTypeListSerde):
        return [origin(serde._value)]

    if isinstance(serde, DictSerde):
        return {origin(serde._key): origin(serde._value)}

    if isinstance(serde, SetSerde):
        return {origin(serde._value)}

    if isinstance(serde, Serde):
        return serde

    assert False, str(serde) + " is not a serde"


def _key_serde(serde, if_not_found=None):
    return extract_elem(serde, 0, if_not_found)


def _value_serde(serde, if_not_found=None):
    """ inner function """
    origin_serde = origin(serde)
    if isinstance(origin_serde, list) or isinstance(origin_serde, tuple):
        if len(origin_serde) == 2:
            return Optional(of(origin_serde[1]))
        else:
            raise Exception('the elem of group_by_key/cogroup/join must have two elements')

    return if_not_found


def common_serde(*serdes):
    """
    输出输入serde集合中的公共serde
    目前只维护系统定义的serde, 对于用户自定义的serde, 返回None
    Args:
        serdes: serde instances or classes
    Returns:
        如果找到合适的公共serde则返回，否则返回None
    """
    import inspect

    serde_of_seq = [StrSerde, ListSerde, TupleSerde, SetSerde]
    serde_of_map = [DictSerde]
    serde_of_single = [IntSerde, BoolSerde, FloatSerde]
    serde_of_default = [DefaultSerde, CPickleSerde]

    # following serde appears in serdes, None will return
    serde_of_user = [ProtobufSerde]

    available_serde = serde_of_seq + serde_of_map + \
            serde_of_single + serde_of_default + serde_of_user

    def _inner_map(sd):
        """map serde instance to class"""
        if inspect.isclass(sd):
            return sd
        if sd.__class__ == Optional:
            return sd.origin_serde().__class__
        else:
            return sd.__class__
    serde_classes = map(_inner_map, serdes)
    serde_set = set(serde_classes)

    if len(serde_set) == 1:
        return serdes[0]() if inspect.isclass(serdes[0]) else serdes[0]

    if len(serde_set) == 0 or \
            not set(serde_of_user).isdisjoint(serde_set) or\
            not set(serde_set).issubset(set(available_serde)):
        return None

    return DefaultSerde()

