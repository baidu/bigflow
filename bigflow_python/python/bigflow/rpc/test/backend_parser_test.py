#!/usr/bin/evn python
# -*- coding: utf-8 -*-
########################################################################
#
# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.

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
Logging parser related tests

Author: Ye, Xianjin(bigflow-opensource@baidu.com)
"""

import logging
import unittest

from bigflow.rpc import backend_parser


class MockLoggingHandler(logging.Handler):
    """Mock logging handler to hold all the output logging for further assertion or checking

    Logged messages are available through the instance's ```messages``` dict, and grouped by five
    logging levels: {'debug', 'info', 'warning', 'error', 'critical'}

    """

    def __init__(self, *args, **kwargs):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': [],
        }
        super(MockLoggingHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        self.acquire()
        try:
            self.messages[record.levelname.lower()].append(record.getMessage())
        finally:
            self.release()

    def reset(self):
        self.acquire()
        try:
            for level in self.messages:
                del self.messages[level][:]
        finally:
            self.release()


class BackendParserTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(BackendParserTest, cls).setUpClass()
        logger = backend_parser.logger
        cls._mock_handler = MockLoggingHandler(level='DEBUG')
        logger.addHandler(cls._mock_handler)
        cls.logged_msgs = cls._mock_handler.messages

    def setUp(self):
        super(BackendParserTest, self).setUp()
        self._mock_handler.reset()

    def test_simple_parser(self):
        backend_parser.backend_parser("start to launch job...")
        backend_parser.backend_parser("Planner start optimizing")
        backend_parser.backend_parser("Planner finished optimizing")
        expected = ["Start new job",
                    "Backend planner start optimizing",
                    "Backend planner finished optimizing",
                    "Start running job"]
        self.assertEqual(expected, self.logged_msgs['info'])

    def test_hadoop_logger_parser(self):
        messages = [
            "Hello world",
            "zhu xi da fa hao",
            "Happy 1024 day!"
        ]
        hadoop_stderr_msg = "Hadoop client STDERR:"
        expected = [hadoop_stderr_msg + msg for msg in messages]
        for log, _ in zip(messages, expected):
            backend_parser.backend_parser(hadoop_stderr_msg + log)

        self.assertEqual(expected, self.logged_msgs['info'])

    def test_local_uri_parser(self):
        backend_parser.backend_parser("io_format.cpp hello bigflow split uri : "
                                      "hdfs:///path/to/dev/null")
        self.assertEqual(["Reading input: hdfs:///path/to/dev/null"], self.logged_msgs['info'])

    def test_spark_driver_parser_normal(self):
        messages = [
            "16:57:04 WARN UserGroupInformation: DEBUG hadoop ugi bigflow",
            "17/10/18 16:57:05 INFO Client: Source and destination file systems are the same.",
            "17/10/18 16:57:13 INFO Client: Application report for "
            "application_1500613944784_123204 (state: ACCEPTED)",

        ]

        expected = [
            "WARN UserGroupInformation: DEBUG hadoop ugi bigflow",
            "INFO Client: Source and destination file systems are the same.",
            "INFO Client: Application report for application_1500613944784_123204 (state: ACCEPTED)"
        ]
        for msg in messages:
            backend_parser.backend_parser(msg)

        self.assertEqual(expected, self.logged_msgs['info'])

    def test_spark_driver_exception(self):
        tracking_log = """17/10/18 16:57:28 INFO Client:
        client token: N/A
        diagnostics: N/A
        ApplicationMaster host: 255.255.255.255
        tracking URL: http://hostname:8388/proxy/application_1500613944784_123204/
        user: bigflow"""
        binding_exception_log = """17/10/18 16:57:02 WARN java.net.BindException: Address already in use
java.net.BindException: Address already in use
        at sun.nio.ch.Net.bind0(Native Method)
        at sun.nio.ch.Net.bind(Net.java:437)
        at sun.nio.ch.Net.bind(Net.java:429)
        at sun.nio.ch.ServerSocketChannelImpl.bind(ServerSocketChannelImpl.java:223)
Caused by: javax.security.auth.login.LoginException: java.lang.NullPointerException
        at org.apache.hadoop.security.UserGroupInformation$BaiduHadoopLoginModule.commit(UserGroupInformation.java:247)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:497)

        at xxx
        ... 20 more"""
        addition_log = "17/10/18 16:57:02 WARN ServerConnector: Stopped Spark"

        expected_logs = []
        for log in tracking_log.split("\n"):
            backend_parser.backend_parser(log)
            if "INFO" in log:
                expected_logs.append(log[log.find("INFO"):])
            else:
                expected_logs.append(log)

        for log in binding_exception_log.split("\n"):
            backend_parser.backend_parser(log)
            if "WARN" in log:
                expected_logs.append(log[log.find("WARN"):])
            else:
                expected_logs.append(log)
        backend_parser.backend_parser(addition_log)
        expected_logs.append(addition_log[addition_log.find("WARN"):])
        self.assertEqual("\n".join(expected_logs), "\n".join(self.logged_msgs['info']))


if __name__ == "__main__":
    unittest.main()
