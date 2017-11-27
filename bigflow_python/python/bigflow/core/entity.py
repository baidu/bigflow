# -*- coding: utf-8 -*-
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
"""
Python Implementation of Flume Entity. Entity is

"""

import copy
import pickle
import threading
import os
import uuid
import atexit
import subprocess

from bigflow import error
from bigflow import pcollection, ptable, pobject
from bigflow.core.serde import cloudpickle
from bigflow.util.log import logger

_mutex = threading.Lock()
_entity_id = 0
ENTITY_FOLDER_BASE= "entity-" + str(uuid.uuid1())
ENTITY_FOLDER = os.path.normpath(os.path.join(os.getcwd(), ENTITY_FOLDER_BASE))
FLUME_WORKER_ENTITY_FOLDER = ENTITY_FOLDER_BASE
ONE_MEGA_BYTES = 1024 * 1024
ENTITY_PROTO_SIZE_LIMIT = ONE_MEGA_BYTES

def _to_proto_message_wrapper(func):
    """wraps to_proto_message funcs
        1. increase global counter
        2. size > 1M, output config to a file
    """

    def _wrapper(self, *args, **kwargs):
        """inner wrapper"""
        import sys
        import os

        proto = func(self, *args, **kwargs)
        with _mutex:
            proto.id = sys.modules[__name__]._entity_id
            sys.modules[__name__]._entity_id += 1
        if (proto.ByteSize() > ENTITY_PROTO_SIZE_LIMIT):
            # output to a file
            if hasattr(self, "get_entity_name"):
                name = self.get_entity_name()
            else:
                name = self.__class__.__name__
            folder = sys.modules[__name__].ENTITY_FOLDER
            file = os.path.join(folder, "_".join([name, str(uuid.uuid1())]))
            with open(file, 'wb') as fd:
                fd.write(proto.config)

            # clear config filed
            proto.ClearField('config')
            proto.config_file = (
                    os.path.join(FLUME_WORKER_ENTITY_FOLDER, os.path.basename(file)))

        return proto

    return _wrapper


class EntitiedBySelf(object):
    """
    An entity that returns given entity_name and entity_config
    """

    def __init__(self):
        pass

    def get_entity_name(self):
        """
        Get entity_name of this entity

        Raises:
          NotImplementedError:  if not implemented
        """
        raise NotImplementedError

    def get_entity_config(self):
        """
        Get entity_config of this entity

        Raises:
          NotImplementedError:  if not implemented
        """
        raise NotImplementedError


class Entity(object):
    """
    A wrapper of serializable operators of Flume.
    """

    loader = "PythonLoaderDelegator"
    processor = "PythonProcessorDelegator"
    objector = "PythonObjectorDelegator"
    sinker = "PythonSinkerDelegator"
    key_reader = "PythonKeyReaderDelegator"
    partitioner = "PythonPartitionerDelegator"
    sort_key_reader = "StrKeyReaderDelegator"
    window = "PythonWindowFnDelegator"
    trigger = "PythonTriggerDelegator"
    time_reader = "PythonTimeReaderDelegator"

    def __init__(self, name="", operator=None, message=None):
        if message is None:
            if len(name) == 0:
                raise error.InvalidLogicalPlanException("Invalid name for entity.")
            if operator is None:
                raise error.InvalidLogicalPlanException("Invalid operator(None) for entity.")

            if isinstance(operator, EntitiedBySelf):
                self.__name = operator.get_entity_name()
                self.__config = operator.get_entity_config()
            elif isinstance(operator, str):
                self.__name = name
                self.__config = operator
            else:
                self.__name = name
                self.__config = cloudpickle.dumps(operator)
        else:
            self.from_proto_message(message)

    def is_empty(self):
        return not self.__name or len(self.__name) == 0

    @property
    def config(self):
        """ return config """
        return self.__config

    @property
    def name(self):
        """ return name """
        return self.__name


    def from_proto_message(self, message):
        from bigflow.core import entity_names
        for key, value in entity_names.__dict__.items():
            if isinstance(key, str) and isinstance(value, str) and value == message.name:
                self.__name = key
        if self.__name is None:
            raise error.InvalidLogicalPlanException("Invalid name/type for entity.")
        self.__config = message.config


    @_to_proto_message_wrapper
    def to_proto_message(self):
        from flume.proto import entity_pb2
        from bigflow.core import entity_names
        message = entity_pb2.PbEntity()
        message.name = entity_names.__dict__[self.__name]
        message.config = self.__config

        return message

    @staticmethod
    def of(name, operator):
        if isinstance(operator, Entity):
            return operator
        return Entity(name, operator)

    @staticmethod
    def from_message(pb_message):
        return Entity(message=pb_message)

    def create_and_setup(self):
        if self.is_empty():
            raise error.InvalidLogicalPlanException("Empty entity")

        instance = pickle.loads(self.__config)

        return instance

    def __eq__(self, other):
        if isinstance(other, Entity):
            return self.__name == other.__name and self.config == other.__config

        return False

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.__name) ^ hash(self.__config) ^ hash((self.__name, self.__config))


class PythonTimeReaderDelegator(EntitiedBySelf):
    """ PythonTimeReaderDelegator """
    def __init__(self, functor):
        self._fn = Functor.of(functor)
    def get_entity_name(self):
        return "PythonTimeReaderDelegator"
    def get_entity_config(self):
        return self._fn.to_proto_message().SerializeToString()

class Functor(object):
    def to_proto_message(self):
        raise NotImplementedError()

    def expect_iterable(self):
        pass

    @staticmethod
    def of(fn):
        """
        if fn is a Functor, return itself
        if is callable, we will wrap it as a PyFn
        otherwise, we will create a fn who return a deepcopy of the param
        NOTE: !!!!copy.deepcopy CANNOT be changed!!!
        """
        if isinstance(fn, Functor):
            return fn
        elif callable(fn):
            return PyFn(fn)
        else:
            return PyFn(lambda *p: copy.deepcopy(fn))

class PyFn(Functor):
    def __init__(self, fn):
        self.__fn = fn
        self.__expect_iterable = False

    def expect_iterable(self):
        self.__expect_iterable = True

    @_to_proto_message_wrapper
    def to_proto_message(self):
        from flume.proto import entity_pb2
        from bigflow.core import entity_names
        pb_entity = entity_pb2.PbEntity()
        pb_entity.name = entity_names.__dict__['PythonImplFunctor']
        config = {}
        config['fn'] = self.__fn
        config['expect_iterable'] = self.__expect_iterable
        pb_entity.config = cloudpickle.dumps(config)
        return pb_entity


class CppFunctor(Functor):
    def name(self):
        return type(self).__name__

    def config(self):
        """config"""
        return ""

    @_to_proto_message_wrapper
    def to_proto_message(self):
        from flume.proto import entity_pb2
        from bigflow.core import entity_names
        pb_entity = entity_pb2.PbEntity()
        pb_entity.name = entity_names.__dict__[self.name()]
        pb_entity.config = self.config()
        return pb_entity

class CartesianFn(CppFunctor):
    pass

class FullJoinInitializeFn(CppFunctor):
    pass

class FullJoinTransformFn(CppFunctor):
    pass

class FullJoinFinalizeFn(CppFunctor):
    pass

class OneSideJoinFn(CppFunctor):
    pass

class ExtractValueFn(CppFunctor):
    pass

class Partitioner(EntitiedBySelf):
    def __init__(self, partition_fn):
        self.partition = partition_fn

    def get_entity_config(self):
        return cloudpickle.dumps(self)

    def get_entity_name(self):
        return "PythonPartitionerDelegator"


class BucketPartitioner(EntitiedBySelf):
    """ BucketPartitioner Entity Delegator """
    def __init__(self, bucket_size=1000):
        self.bucket_size = bucket_size

    def get_entity_config(self):
        """ inner functor """
        from bigflow_python.proto import entity_config_pb2
        pb = entity_config_pb2.PbBucketPartitionerConfig()
        pb.bucket_size = self.bucket_size
        return pb.SerializeToString()

    def get_entity_name(self):
        """ inner functor """
        return "BucketPartitioner"

class SelfNamedEntityBase(EntitiedBySelf):
    def get_entity_name(self):
        """ return child class name """
        return type(self).__name__

    def get_entity_config(self):
        """
        Get entity_config of this entity

        """
        return ""

class PythonEnvironment(SelfNamedEntityBase):
    pass

class Processor(EntitiedBySelf):
    def __init__(self, *fns):
        self.__fns = fns
        self.__normal_input_num = 1
        self.__config = None
        self.__side_inputs = []

    def normal_input_num(self, n = None):
        if n is None:
            return self.__normal_input_num
        self.__normal_input_num = n

    def set_config(self, config):
        self.__config = config

    def get_entity_config(self):
        from bigflow_python.proto import processor_pb2
        processor = processor_pb2.PbPythonProcessorConfig()
        if self.__config is not None:
            processor.config = cloudpickle.dumps(self.__config)
        for fn in self.__fns:
            fn = Functor.of(fn)
            processor.functor.add().CopyFrom(fn.to_proto_message())
        for side_input in self.__side_inputs:
            side_input_type = processor_pb2.POBJECT_TYPE
            if isinstance(side_input, pcollection.PCollection):
                side_input_type = processor_pb2.PCOLLECTION_TYPE
            processor.side_input_type.append(side_input_type)
        return processor.SerializeToString()

    def get_entity_name(self):
        return type(self).__name__

    def set_side_inputs(self, *side_inputs):
        self.__side_inputs = side_inputs
        return self

class FlatMapProcessor(Processor):
    def __init__(self, fn):
        fn = Functor.of(fn)
        fn.expect_iterable()
        super(FlatMapProcessor, self).__init__(fn)

class FilterProcessor(Processor):
    def __init__(self, fn, *side_inputs):
        fn = Functor.of(fn)
        super(FilterProcessor, self).__init__(fn)
        self.set_side_inputs(*side_inputs)

class MapProcessor(Processor):
    def __init__(self, fn):
        fn = Functor.of(fn)
        super(MapProcessor, self).__init__(fn)

    def get_entity_name(self):
        return "FlatMapProcessor"

class CombineProcessor(Processor):
    def __init__(self, fn):
        super(CombineProcessor, self).__init__(fn)
        self.normal_input_num(0)

class ReduceProcessor(Processor):
    def __init__(self, fn):
        super(ReduceProcessor, self).__init__(fn)

class AccumulateProcessor(Processor):
    def __init__(self, zero_fn, accumulate_fn):
        super(AccumulateProcessor, self).__init__(zero_fn, accumulate_fn)


class CountProcessor(Processor):
    def __init__(self):
        super(CountProcessor, self).__init__()


class SumProcessor(Processor):
    def __init__(self):
        super(SumProcessor, self).__init__()


class TakeProcessor(Processor):
    def __init__(self, n):
        if isinstance(n, pobject.PObject):
            super(TakeProcessor, self).__init__()
            self.set_side_inputs(n)
        else:
            super(TakeProcessor, self).__init__()
            self.set_config(n)

class SelectElementsProcessor(Processor):
    def __init__(self, n, order, key_fn = None):
        if key_fn is None:
            super(SelectElementsProcessor, self).__init__()
        else:
            super(SelectElementsProcessor, self).__init__(key_fn)
        d = {}
        if isinstance(n, pobject.PObject):
            self.set_side_inputs(n)
            d["num"] = -1
        else:
            d["num"] = n
        d["order"] = order
        self.set_config(d)

class TransformProcessor(Processor):
    def __init__(self, status_serde, initialize_fn, transform_fn, finalize_fn):
        msg = Entity.of(Entity.objector, status_serde).to_proto_message()
        super(TransformProcessor, self).__init__(initialize_fn, transform_fn, finalize_fn)
        self.set_config(msg.SerializeToString())

class FlattenProcessor(Processor):
    def __init__(self, serde):
        msg = Entity.of(Entity.objector, serde).to_proto_message()
        super(FlattenProcessor, self).__init__()
        #print len(msg.SerializeToString())
        self.set_config(msg.SerializeToString())

class GetLastKeyProcessor(Processor):
    def __init__(self, deserialize_fn):
        super(GetLastKeyProcessor, self).__init__(deserialize_fn)

class ValueProcessor(Processor):
    def __init__(self, fn):
        if fn is None:
            fn = ExtractValueFn()
        fn = Functor.of(fn)
        super(ValueProcessor, self).__init__(fn)

    def get_entity_name(self):
        return "FlatMapProcessor"


class PipeProcessor(Processor):
    """ PipeProcessor """
    def __init__(self, command, **kargs):
        super(PipeProcessor, self).__init__()
        config = dict()
        config['is_nested_ptype'] = kargs.get('is_nested_ptype', False)
        config['command'] = command
        # default buffer size 64M
        config['buffer_size'] = kargs.get('buffer_size', 64 * 1024 * 1024)
        config['type'] = kargs.get('type', 'streaming')
        config['field_delimiter'] = kargs.get('field_delimiter', '\t')
        config['line_delimiter'] = kargs.get('line_delimiter', '\n')
        config['input_fields_num'] = kargs.get('input_fields_num', 1)
        config['output_fields_num'] = kargs.get('output_fields_num', 1)
        self.set_config(config)


class BarshalObjector(EntitiedBySelf):
    """ BarshalObjector """

    def get_entity_name(self):
        """ get name """
        return "BarshalObjector"

    def get_entity_config(self):
        """ get config """
        return ''

class SplitStringToTypes(CppFunctor):
    def __init__(self, sep, fields_type, ignore_overflow, ignore_illegal_line):
        """
        接受一行python字符串, 分割符号，每列对应的python类型
        将字符串按分隔符分割，并将每一列转化为对应的python类型

        Args:
            seq: Python string, 分割符
            fields_type: Python type, 每列对应的python类型
            ignore_overflow: Python boolean, 是否允许文件列数多于字段数
            ignore_illegal_line: Python boolean, 是否允许文件列数小于字段数时忽略该行
        """
        self._sep = sep
        self._fields_type = fields_type
        self._ignore_overflow = ignore_overflow
        self._ignore_illegal_line = ignore_illegal_line

    def config(self):
        """ Config: Pass sep, fields_type arguments to cpp runtime"""
        return cloudpickle.dumps((self._sep, self._fields_type,
                                  self._ignore_overflow, self._ignore_illegal_line))


class SerdeWrapper(CppFunctor):
    """SerdeWrapper"""

    def __init__(self, objector, is_serialize=True, apply_tuple_index=-1):
        self._is_serialize = is_serialize
        self._objector = objector
        self._apply_index = apply_tuple_index

    def config(self):
        """Config: Pass serialized arguments to cpp runtime"""
        return cloudpickle.dumps((self._is_serialize, self._objector, self._apply_index))


class KVDeserializeFn(CppFunctor):
    """Deserialize function for (Key, Value"""

    def __init__(self, *deserializers):
        self.deserializers = deserializers

    def config(self):
        """Pass deserializers to cpp runtime"""
        return cloudpickle.dumps(self.deserializers)


class KVSerializeFn(CppFunctor):
    """serialize function for (Key, Value"""

    def __init__(self, *serializers):
        self.serializers = serializers

    def config(self):
        """Pass serializers to cpp runtime"""
        return cloudpickle.dumps(self.serializers)


# please add normal code before following code
def clear_folder(name):
    """clear folder when exists."""
    logger.debug("deleting folder %s." % ENTITY_FOLDER)
    subprocess.call("command rm -rf %s" % ENTITY_FOLDER, shell=True)


subprocess.check_call("command mkdir %s" % ENTITY_FOLDER, shell=True)
# when python exits, folder will be deleted
atexit.register(clear_folder, ENTITY_FOLDER)

# EOF
