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

Author: Zhang Yuncong <bigflow-opensource@baidu.com>
"""

import unittest
import shutil
import os
import struct

from bigflow import serde
from bigflow import transforms

from bigflow.test import test_base
from bigflow.util.log import logger
from bigflow_python.proto import processor_pb2

from google.protobuf import message

class TestPythonStrSerde(serde.Serde):
    """
    测试str类型序列化/反序列化器
    """
    def serialize(self, obj):
        """serialize"""
        return obj

    def deserialize(self, buf):
        """ deserialize """
        return buf

    def __str__(self):
        return "str_()"


class TestPythonIntSerde(serde.Serde):
    """
    测试int类型序列化/反序列化器
    """
    def serialize(self, obj):
        """serialize"""
        return obj

    def serialize(self, obj):
        """serialize int obj"""
        return struct.pack("<q", obj)

    def deserialize(self, buf):
        """deserialize"""
        return struct.unpack("<q", buf)[0]


class TestPythonTupleSerde(serde.Serde):
    """
    tuple类型序列化/反序列化器
    """
    def __init__(self, *args):
        """ init """
        self._args = map(serde.of, args)

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


class TestNonCppSerdeImpl(test_base.PipelineBasedTest):
    """测试非cpp实现的serde"""

    def test_python_str_serde(self):
        """test_python_str_serde"""
        data = self._pipeline.parallelize(["abc", "cbd", "ghj"])\
            .map(lambda r: r, serde=TestPythonStrSerde())

        self.assertItemsEqual(["abc", "cbd", "ghj"], data.get())

    def test_python_tuple_serde(self):
        """test_python_tuple_serde"""
        data = self._pipeline.parallelize(["abc", "cbd", "ghj"])\
            .map(lambda r: (r, 1), serde=TestPythonTupleSerde(\
                TestPythonStrSerde(), TestPythonIntSerde()))

        self.assertItemsEqual([("abc", 1), ("cbd", 1), ("ghj", 1)], data.get())


def serde_test(fn):
    """ inner """
    def real_fn(test_obj):
        """ inner """
        test_obj.start_serde_test()
        fn(test_obj)
        test_obj.end_serde_test()
    return real_fn


class TestSerdeCase(test_base.PipelineBasedTest):
    """ test """
    def start_serde_test(self):
        """ test """
        self._checking_condition=[]

    def end_serde_test(self):
        """ test """
        import sys
        from bigflow.core import entity
        logger.info(str(self._checking_condition))
        values = map(lambda condition: condition[1], self._checking_condition)
        p_values = self._pipeline.parallelize([values]) # 避免map结点超过32个（Hadoop的限制）
        p_value_list = []


        out = []
        for (i, (sd, value)) in enumerate(self._checking_condition):
            sd1 = serde.of(int)
            sd2 = sd

            cpp_deserialize_fn = entity.KVDeserializeFn(sd1, sd2)
            cpp_serialize_fn = entity.KVSerializeFn(sd1, sd2)

            python_deserialize_fn = lambda kv: (sd1.deserialize(kv[0]), sd2.deserialize(kv[1]))
            python_serialize_fn = lambda kv: (sd1.serialize(kv[0]), sd2.serialize(kv[1]))

            serialize_fns = [cpp_serialize_fn, python_serialize_fn]
            deserialize_fns = [cpp_deserialize_fn, python_deserialize_fn]

            kv_val = (1, value)
            def _assert_eq_val(v):
                assert v == kv_val
            for serialize_fn in serialize_fns:
                for deserialize_fn in deserialize_fns:
                    out.append(p_values.map(lambda x: (1, x[i]))
                            .map(serialize_fn)
                            .map(deserialize_fn)
                            .map(_assert_eq_val))
        if out:
            transforms.union(*out).cache()
        else:
            print >> sys.stderr, "SKIP a test!!!"
        self._pipeline.run()

    def check(self, sd, value):
        """ inner """
        sd = serde.of(sd)
        self.assertEqual(value, sd.deserialize(str(sd.serialize(value))))
        import marshal
        import sys
        try:
            assert value == marshal.loads(marshal.dumps(value))
        except:
            print >>sys.stderr, 'skip an unsupported serde', str(sd)
        else:
            self._checking_condition.append((sd, value))

    def check_sample(self, value):
        """ inner """
        sd = serde.sample(value)
        self.check(sd, value)

    @serde_test
    def test_serde_normal(self):
        """ inner """
        self.check(int, 1)
        self.check(int, 0)
        self.check(int, None)
        self.check(bool, True)
        self.check(bool, False)
        self.check(bool, None)
        self.check(serde.int_(), 1)
        self.check(serde.int_(), 0)
        self.check(serde.int_(), None)

        self.check((int, ), (1,))
        self.check((int, ), (0,))
        self.check((int, ), (None,))
        self.check((int, ), None)

        self.check([float], [1.1, 1.2])
        self.check([float], [1.1, None])
        self.check([float], [None])
        self.check([float], [])
        self.check([float], None)

        self.check(str, None)
        self.check(str, '')
        self.check(str, '1')

        config = processor_pb2.PbPythonProcessorConfig()
        config.config = "123456"

        self.check(processor_pb2.PbPythonProcessorConfig, config)
        self.check((str, ([(int, float)], processor_pb2.PbPythonProcessorConfig)), \
              ('1', ([(1, None), (2, 1.1)], config)))

        self.check(serde._, 1)
        self.check(serde._, 1.1)
        self.check(serde._, None)
        self.check(serde._, [None, (None, 1), 2])
        self.check((serde._, int), ('abc', 2))
        self.check([serde._], [1, '2', 3.1, (1, 2)])

        self.check(serde.set_of(int), {1, 2, 3})
        self.check({int}, {1, 2, None})
        self.check({int}, None)

        self.check(serde.dict_of(int, str), {1: '123', 2: None})
        self.check({int:str}, {1: '123', 2: None})
        self.check({int:str}, None)
        self.check({int:(str, {float}, type(config))}, \
            {1: ('1', {2.2, 3.4}), 2: ('2', {3.2}, config)})

        self.check(serde.sample([1, 2, 3]), [1])

        self.check_sample(1)
        self.check_sample(2)
        self.check_sample({1})
        self.check_sample({1, 2})
        self.check_sample({'1'})
        self.check_sample({''})
        self.check_sample({'', '1'})
        self.check_sample({'1': 1, '2': 2})
        self.check_sample(({'1': 1, '2': 2}, {1, 2}))
        self.check_sample(({'1': 1, '2': 2}, [1.0, 2.0]))
        self.check_sample(({'1': 1, '2': 2}, config))
        self.check_sample(({True: True, False: True}))
        self.check_sample([1, '2', config])

    def serde_eq(self, expect, real):
        """ inner """
        self.assertEqual(str(serde.of(expect)), str(serde.of(real)))

    @serde_test
    def test_key_value_serde(self):
        """ inner """
        self.serde_eq(int, serde._key_serde(serde.of([int, str]), None))
        self.serde_eq(str, serde._key_serde(serde.of((str, int)), None))

        self.serde_eq(int, serde._value_serde(serde.of((str, int)), None))
        self.serde_eq(int, serde._value_serde(serde.of([str, int]), None))

    @serde_test
    def test_proto_serde_ignore_exception(self):
        """ test """

        self.assertEqual(None,
            serde.ProtobufSerde(processor_pb2.PbPythonProcessorConfig, False).deserialize('1111'))

        self.assertRaises(message.DecodeError,
            serde.ProtobufSerde(processor_pb2.PbPythonProcessorConfig).deserialize, '1111')

    def test_common_serde(self):
        """test common serde"""
        serdes = []
        serdes.append(serde.IntSerde)
        serdes.append(serde.FloatSerde)
        serdes.append(serde.StrSerde())
        serdes.append(serde.ListSerde())
        serdes.append(serde.TupleSerde())
        serdes.append(serde.DictSerde(str, str))
        serdes.append(serde.BoolSerde())
        serdes.append(serde.DefaultSerde())
        serdes.append(serde.CPickleSerde())
        self.assertIsInstance(serde.common_serde(*serdes), serde.DefaultSerde)

        serdes.append(serde.ProtobufSerde(lambda x:x))
        self.assertEqual(None, serde.common_serde(*serdes))

        serdes = []
        class TestSerde1(object):
            """for test"""
            pass
        class TestSerde2(object):
            """for test"""
            pass
        serdes.append(TestSerde1)
        self.assertIsInstance(serde.common_serde(*serdes), TestSerde1)
        serdes.append(TestSerde2)
        self.assertEquals(None, serde.common_serde(*serdes))

if __name__ == "__main__":
    serde.USE_DEFAULT_SERDE=False
    unittest.main()
