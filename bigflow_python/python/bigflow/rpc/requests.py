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
RPC requests

"""

import os
import threading
import json

from bigflow import error
from bigflow.core.serde import omnitypes_objector
from bigflow.rpc import service
from bigflow.util import decorators
from bigflow.util.log import logger
from bigflow_python.proto import service_pb2

error_keywords = ["error", "Error", "Failure", "failure", "Undetermined", "FileNotFoundException", "IOException"]

def _error_listener(line):
    with _mutex:
        for word in error_keywords:
            pos = line.find(word)
            if pos != -1:
                _message.append(line[pos:])


import platform

if os.getenv("__PYTHON_IN_REMOTE_SIDE", None) != "true" and platform.system() == 'Linux':
    logger.debug("Getting service")
    _service = service.Service()
    _service.server.add_stderr_listener(_error_listener)
    _message = []
    _mutex = threading.Lock()

def check_response(func):
    """
    A decorator that expect check response of RPC calls and throw BigflowRPCException if any error
    happened.

    Args:
      func (function):  function to decorate

    Raises:
      error.BigflowRPCException:  if any error happened
    """

    def _wrapper(*args, **kw):
        with _mutex:
            _message[:] = []  # why Python list has no clear() ??

        result, status = func(*args, **kw)
        if not status.success:
            err_msg = "Error running command [%s], reason: %s" % (func.__name__, status.reason)
            raise error.BigflowRPCException(err_msg)

        return result
    return _wrapper


def launch(pipeline_id, logical_plan_message, resource_message, commit_args=None, context=None):
    """
    Send the rpc command to the other end to launch the logical plan

    Args:

    Raises:
      error.BigflowRPCException:  if any error happened
    """

    request = service_pb2.LaunchRequest()
    request.pipeline_id = pipeline_id
    request.logical_plan.CopyFrom(logical_plan_message)

    request.resource.CopyFrom(resource_message)

    if commit_args is not None:
        request.hadoop_commit_args.extend(commit_args)

    if context is not None:
        assert isinstance(context, str)
        request.pipeline_context = context

    response = _service.request(request, "launch")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.VoidResponse())
    #logger.info(res)

    if not res.status.success:
        backend_log_path = os.getenv("BIGFLOW_LOG_FILE_BACKEND", "")
        error_message = "Job ran failed"
        if len(_message) > 0:
            error_message += ", possible_reason: \n" + "".join(_message)
        if backend_log_path:
            error_message += "Please check backend log['%s.log'] for details" % backend_log_path
        raise error.BigflowRuntimeException(error_message)


@check_response
def suspend(pipeline_id, logical_plan_message, commit_args=None):
    """
    Send the rpc command to the other end to suspend the logical plan

    Args:

    Raises:
      error.BigflowRPCException:  if any error happened
    """

    request = service_pb2.SuspendRequest()
    request.pipeline_id = pipeline_id
    request.logical_plan.CopyFrom(logical_plan_message)

    if commit_args is not None:
        request.hadoop_commit_args.extend(commit_args)

    response = _service.request(request, "suspend")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.VoidResponse())
    return None, res.status


@check_response
def kill(pipeline_id, logical_plan_message, commit_args=None):
    """
    Send the rpc command to the other end to kill the logical plan

    Args:

    Raises:
      error.BigflowRPCException:  if any error happened
    """

    request = service_pb2.KillRequest()
    request.pipeline_id = pipeline_id
    request.logical_plan.CopyFrom(logical_plan_message)

    if commit_args is not None:
        request.hadoop_commit_args.extend(commit_args)

    response = _service.request(request, "kill")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.VoidResponse())
    return None, res.status


@check_response
def get_status(pipeline_id, logical_plan_message, commit_args=None):
    """
    Send the rpc command to the other end to get the logical plan status

    Args:

    Raises:
      error.BigflowRPCException:  if any error happened
    """

    request = service_pb2.GetStatusRequest()
    request.pipeline_id = pipeline_id
    request.logical_plan.CopyFrom(logical_plan_message)

    if commit_args is not None:
        request.hadoop_commit_args.extend(commit_args)

    response = _service.request(request, "get_status")

    descriptor = response.DESCRIPTOR.enum_types_by_name['Status']

    return descriptor.values_by_number[response.app_status].name, response.status


@check_response
def register_pipeline(pb_pipeline, pipeline_id):
    request = service_pb2.RegisterPipelineRequest()
    request.pipeline.CopyFrom(pb_pipeline)
    request.pipeline_id = pipeline_id

    response = _service.request(request, "register_pipeline")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.VoidResponse())
    #logger.info(res)
    return None, res.status


@check_response
def write_record(file_path, records, objector):
    """
    write records to file path with objector
    """
    request = service_pb2.WriteLocalSeqFileRequest()
    request.file_path = file_path
    for record in records:
        request.key.append("")
        request.value.append(objector.serialize(record))

    response = _service.request(request, "write_local_seqfile")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.VoidResponse())

    return None, res.status


@check_response
def unregister_pipeline(pipeline_id):
    """
    unregister pipeline to backend
    """
    request = service_pb2.UnRegisterPipelineRequest()
    request.pipeline_id = pipeline_id

    response = _service.request(request, "unregister_pipeline")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.VoidResponse())
    #logger.info(res)
    return None, res.status


@check_response
def default_hadoop_config_path():
    response = _service.request(service_pb2.SingleStringRequest(), "default_hadoop_config_path")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.SingleStringResponse())
    #logger.info(res)
    return res.response.encode("utf8"), res.status


@check_response
def default_hadoop_client_path():
    response = _service.request(service_pb2.SingleStringRequest(), "default_hadoop_client_path")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.SingleStringResponse())
    #logger.info(res)
    return res.response.encode("utf8"), res.status


@check_response
def default_fs_defaultfs(hadoop_config_path=None):
    """
    request default fs.defaultFS from hadoop config
    """
    if hadoop_config_path is None:
        hadoop_config_path = default_hadoop_config_path()
    request = service_pb2.SingleStringRequest()
    request.request = hadoop_config_path
    response = _service.request(request, "default_fs_defaultfs")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.SingleStringResponse())
    #logger.info(res)
    return res.response.encode("utf8"), res.status


@check_response
def default_hadoop_job_ugi(hadoop_config_path=None):
    """
    request default hadoop.job.ugi from hadoop config
    """
    if hadoop_config_path is None:
        hadoop_config_path = default_hadoop_config_path()
    request = service_pb2.SingleStringRequest()
    request.request = hadoop_config_path
    response = _service.request(request, "default_hadoop_job_ugi")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.SingleStringResponse())
    return res.response.encode("utf8"), res.status


@check_response
def default_spark_home_path():
    response = _service.request(service_pb2.SingleStringRequest(), "default_spark_home_path")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.SingleStringResponse())
    return res.response.encode("utf8"), res.status


@check_response
def default_tmp_hdfs_path(unique_id):
    request = service_pb2.SingleStringRequest()
    request.request = unique_id
    response = _service.request(request, "tmp_hdfs_path")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.SingleStringResponse())
    return res.response.encode("utf8"), res.status


@check_response
def is_node_cached(pipeline_id, node_id):
    request = service_pb2.IsNodeCachedRequest()
    request.pipeline_id = pipeline_id
    request.node_id = node_id
    response = _service.request(request, "is_node_cached")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.SingleBooleanResponse())
    return res.response, res.status


@check_response
def get_cached_data(pipeline_id, node_id):
    request = service_pb2.GetCachedDataRequest()
    request.pipeline_id = pipeline_id
    request.node_id = node_id
    response = _service.request(request, "get_cached_data")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.GetCachedDataResponse())
    return res.keys_value, res.status


@check_response
def get_toft_style_path(input_path, hadoop_config_path=None, \
    fs_defaultfs=None, hadoop_job_ugi=None):
    """
    request toft style path from flume
    """
    if hadoop_config_path is None:
        hadoop_config_path = default_hadoop_config_path()

    request = service_pb2.GetToftStylePathRequest()
    request.input_path = input_path
    request.hadoop_conf_path = hadoop_config_path
    if fs_defaultfs is not None:
        request.fs_defaultfs = fs_defaultfs
    if hadoop_job_ugi is not None:
        request.hadoop_job_ugi = hadoop_job_ugi
    response = _service.request(request, "get_toft_style_path")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.SingleStringResponse())
    #logger.info(res)
    return res.response.encode("utf8"), res.status


def get_meta_info(config):
    """
    request fields name
    """

    request = service_pb2.GetMetaInfoRequest()
    request.config = config
    response = _service.request(request, "get_meta_info")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.GetMetaInfoRequest())
    return res


def hadoop_commit(args, hadoop_client_path=None, hadoop_config_path=None):
    if hadoop_client_path is None:
        hadoop_client_path = default_hadoop_client_path()

    if hadoop_config_path is None:
        hadoop_config_path = default_hadoop_config_path()

    request = service_pb2.HadoopCommitRequest()
    request.hadoop_client_path = hadoop_client_path
    request.hadoop_config_path = hadoop_config_path
    request.args.extend(args)

    response = _service.request(request, "hadoop_commit")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.VoidResponse())
    return res.status.success


def get_counters():
    """
    request counters
    """

    request = service_pb2.GetCountersRequest()
    response = _service.request(request, "get_counters")

    import google.protobuf.json_format as json_format
    res = json_format.Parse(response, service_pb2.GetCountersResponse())
    return res


def reset_counter(name):
    """
    reset a counter
    """

    request = service_pb2.ResetCounterRequest()
    request.name = name
    response = _service.request(request, "reset_counter")

    return response


def reset_all_counters():
    """
    reset all counters
    """

    request = service_pb2.ResetAllCountersRequest()
    response = _service.request(request, "reset_all_counters")

    return response


if __name__ == '__main__':
    pass
