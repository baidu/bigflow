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
Service for RPC calls

"""

import sys
import os
import time
import re
import threading
import urllib2
import json

import bigflow
import bigflow.error as error
import bigflow.rpc.backend_parser as backend_parser
import bigflow.util.decorators as decorators
import bigflow.util.log as log
import bigflow.util.process_util as process_util
import bigflow_python.proto.service_pb2 as service_pb2


@decorators.singleton
class Service(object):
    """
    The class that handles a RPC service.
    """

    def __init__(self):
        self.server = None
        log.logger.debug("Start initialize")
        env_port = os.getenv("BIGFLOW_SERVER_PORT")
        env_port = int(env_port) if env_port is not None else None

        self.server = None
        self.__backend_log_file = None

        self.__mutex = threading.Lock()
        self.__server_port = env_port
        self.__server_process = self.__init_server(port=env_port)
        self.__wait_for_server_port()
        self.__server_ip_port = "127.0.0.1:%d" % (self.__server_port)

       # self.__tt = transport.Transport()
       # self.__pp = protocol.PbProtocol(self.__tt)
       # self.__cc = channel.RpcChannel(self.__pp, '127.0.0.1', self.__server_port)

        self.__retry_times = 4
        self.__init_interval_second = 0.1

    def request(self, request, func_name, response_buffer_size=10000, timeout_ms=1000):
        """
        Send a request to backend server
        """
       #ctrl = controller.RpcController()
       #ctrl.response_buffer_size = response_buffer_size
       #ctrl.timeout_ms = timeout_ms
        #ss = stub.RpcStub(service_pb2.BigflowService_Stub, self.__cc, request, ctrl)

        import google.protobuf.json_format as json_format
        data = json_format.MessageToJson(request, False, True)
        #data = protobuf_json.pb2json(request)
        #log.logger.info(data)
        #request_json = json.dumps(data)
        request_json = data
        #log.logger.info(request)
        #log.logger.info(request_json)
        url = "http://%s/BigflowService/%s" % (self.__server_ip_port, func_name)
        #log.logger.info(url)
        req = urllib2.Request(url)
        req.add_header('Content-Type', 'application/json')
        try:
            response = urllib2.urlopen(req, request_json, 365 * 24 * 60 * 60)
            #log.logger.info(response.read())
            return response.read()
        except urllib2.HTTPError as e:
            log.logger.info(e)
            raise

       # interval = self.__init_interval_second

       # retries = 0
       # while True:
       #     try:
       #         return getattr(ss, func_name)(request, ctrl)
       #     except(pbrpc_error.ReadError, pbrpc_error.WriteError) as expr:
       #         if retries < self.__retry_times:
       #             time.sleep(interval)
       #             interval *= 2
       #             retries += 1
       #         else:
       #             raise error.BigflowRPCException("rpc meet I/O error", expr)
       #     except AttributeError:
       #         log.logger.error("rpc request get AttributeError")
       #         raise
       #     except Exception as expr:
       #         raise error.BigflowRPCException("Unknown exception", expr)

    def _get_service_port(self, line):
        with self.__mutex:
            if self.__server_port is None:
                pattern = re.compile(r"Bigflow RPC Server started at: localhost:(\d+)")
                match = pattern.search(line)
                if match is not None:
                    self.__server_port = int(match.group(1))

    def __init_server(self, path=None, params=[], port=None):
        cmd = []
        if path is None:
            root = os.path.dirname(bigflow.__file__)
            path = os.getenv("BIGFLOW_SERVER_PATH", "%s/../flume/worker" % root)

        cmd.append(path)

        bigflow_home = os.getenv("BIGFLOW_PYTHON_HOME")

        if bigflow_home is None:
            raise error.BigflowPlanningException("BIGFLOW_PYTHON_HOME is not set!")

        bigflow_home = os.path.abspath(bigflow_home)

        cmd.append("--flume_planner_max_steps=20000")
        keep_resource = os.getenv('BIGFLOW_PYTHON_KEEP_RESOURCE', "false")
        cmd.append("--bigflow_python_keep_resource=" + keep_resource)

        if port is not None:
            cmd.append("--service_port=%d" % port)

        for param in params:
            cmd.append(param)
        self.server = process_util.Subprocess(cmd)

        log_file = os.getenv("BIGFLOW_LOG_FILE_BACKEND")
        if log_file:
            log_file = os.path.abspath(log_file + ".log")
            log_dir = os.path.dirname(log_file)
            if not os.path.isdir(log_dir):
                try:
                    os.makedirs(log_dir)
                except Exception as e:
                    raise error.BigflowPlanningException(
                            "Cannot create log file directory [%s]" % log_dir,
                            e)

            def _log_printer(line):
                self.__backend_log_file.write(line)
            #log.logger.info(cmd)
            log.logger.info("Bigflow Backend log is written to [%s]" % log_file)
            self.__backend_log_file = open(log_file, "w")

            def _stdout_printer(line):
                sys.stderr.write(line)
                sys.stderr.flush()

            self.server.add_stderr_listener(_log_printer)
            self.server.add_stdout_listener(_stdout_printer)
            self.server.add_stderr_listener(backend_parser.backend_parser)

        else:
            def _stderr_printer(line):
                sys.stderr.write(line)
                sys.stderr.flush()

            def _stdout_printer(line):
                sys.stderr.write(line)
                sys.stderr.flush()

            log.logger.info("Bigflow Backend log is written to STDERR")

            self.server.add_stderr_listener(_stderr_printer)
            self.server.add_stdout_listener(_stdout_printer)

        self.server.add_stderr_listener(self._get_service_port)
        return self.server.open()

    def __wait_for_server_port(self):
        total_wait_second = 0.0
        while total_wait_second < 60.0:
            with self.__mutex:
                if self.__server_port is not None:
                    return

            if self.__server_process.poll() is not None:
                self.server.join_all_reading_threads()

                raise error.BigflowRuntimeException(
                    "Bigflow server start failed, return code is [%d]. "
                    "There are more details in log file." %(self.__server_process.returncode)
                )

            time.sleep(0.1)
            total_wait_second += 0.1

        raise error.BigflowRPCException("Unable to get backend service port")

    def __del__(self):
        if self.__backend_log_file is not None:
            self.__backend_log_file.close()

if __name__ == '__main__':
    pass
