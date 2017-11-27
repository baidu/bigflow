/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/
// Author: Wang Cong <wangcong09@baidu.com>

#include "stdlib.h"

#include "boost/foreach.hpp"

#include "bigflow_python/python_client.h"
#include "flume/runtime/local/local_backend.h"
#include "flume/runtime/spark/spark_backend.h"
#include "flume/runtime/counter.h"

namespace baidu {
namespace bigflow {
namespace service {

void ServiceImpl::register_pipeline(
        ::google::protobuf::RpcController* controller,
        const RegisterPipelineRequest* request,
        VoidResponse* response,
        ::google::protobuf::Closure* done) {
    ResponseStatus* status = response->mutable_status();
    std::string pipeline_id = request->pipeline_id();

    if (_map.count(request->pipeline_id()) != 0u) {
        status->set_success(false);
        status->set_reason("ID " + request->pipeline_id() + " already registred");
    } else {
        const PbPipeline& pipeline = request->pipeline();
        flume::runtime::Backend* backend;
        switch (pipeline.type()) {
            case PbPipeline::LOCAL:
                backend = new flume::runtime::local::LocalBackend(pipeline.local_config());
                _map.insert(pipeline_id, backend);
                status->set_success(true);
                break;
            case PbPipeline::SPARK:
                backend = new flume::runtime::spark::SparkBackend(pipeline.spark_config());
                _map.insert(pipeline_id, backend);
                status->set_success(true);
                break;
            default:
                status->set_success(false);
                status->set_reason("Unknown pipeline type");
        }
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::unregister_pipeline(
        ::google::protobuf::RpcController* controller,
        const UnRegisterPipelineRequest* request,
        VoidResponse* response,
        ::google::protobuf::Closure* done) {
    ResponseStatus* status = response->mutable_status();
    std::string pipeline_id = request->pipeline_id();

    if (_map.count(pipeline_id) == 0u) {
        status->set_success(false);
        status->set_reason("ID " + pipeline_id + " is not registred yet");
    } else {
        LOG(INFO) << "Destory backend for pipeline " << pipeline_id;
        _map.erase(pipeline_id);
        status->set_success(true);
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::launch(
        ::google::protobuf::RpcController* controller,
        const LaunchRequest* request,
        VoidResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) <<"start to launch job...";
    ResponseStatus* status = response->mutable_status();

    if (_map.count(request->pipeline_id()) != 0u) {
        flume::runtime::Backend& backend = _map.at(request->pipeline_id());
        const flume::PbLogicalPlan& plan = request->logical_plan();
        const python::PbPythonResource& resource = request->resource();

        std::vector<std::string> hadoop_commit_args;
        hadoop_commit_args.insert(
                hadoop_commit_args.begin(),
                request->hadoop_commit_args().begin(),
                request->hadoop_commit_args().end());

        int ret = python::launch(backend, plan, resource, hadoop_commit_args);
        if (ret == 0) {
            status->set_success(true);
        } else {
            status->set_success(false);
            status->set_reason("Job ran failed");
        }
    } else {
        status->set_success(false);
        status->set_reason("Pipeline: " + request->pipeline_id() + " is not registered");
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::suspend(
        ::google::protobuf::RpcController* controller,
        const SuspendRequest* request,
        VoidResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "start to suspend job...";
    ResponseStatus* status = response->mutable_status();
    if (0u != _map.count(request->pipeline_id())) {
        flume::runtime::Backend& backend = _map.at(request->pipeline_id());
        const flume::PbLogicalPlan& plan = request->logical_plan();

        std::vector<std::string> hadoop_commit_args;
        hadoop_commit_args.insert(
                hadoop_commit_args.begin(),
                request->hadoop_commit_args().begin(),
                request->hadoop_commit_args().end());

        int ret = python::suspend(backend, plan, hadoop_commit_args);
        if (ret == 0) {
            status->set_success(true);
        } else {
            status->set_success(false);
            status->set_reason("Job suspended failed");
        }
    } else {
        status->set_success(false);
        status->set_reason("Pipeline: " + request->pipeline_id() + " is not registered");
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::kill(
        ::google::protobuf::RpcController* controller,
        const KillRequest* request,
        VoidResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "start to kill job...";
    ResponseStatus* status = response->mutable_status();
    if (0u != _map.count(request->pipeline_id())) {
        flume::runtime::Backend& backend = _map.at(request->pipeline_id());
        const flume::PbLogicalPlan& plan = request->logical_plan();

        std::vector<std::string> hadoop_commit_args;
        hadoop_commit_args.insert(
                hadoop_commit_args.begin(),
                request->hadoop_commit_args().begin(),
                request->hadoop_commit_args().end());

        int ret = python::kill(backend, plan, hadoop_commit_args);
        if (ret == 0) {
            status->set_success(true);
        } else {
            status->set_success(false);
            status->set_reason("Job suspended failed");
        }
    } else {
        status->set_success(false);
        status->set_reason("Pipeline: " + request->pipeline_id() + " is not registered");
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::get_status(
        ::google::protobuf::RpcController* controller,
        const GetStatusRequest* request,
        GetStatusResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "start to get job status...";
    ResponseStatus* status = response->mutable_status();
    if (0u != _map.count(request->pipeline_id())) {
        flume::runtime::Backend& backend = _map.at(request->pipeline_id());
        const flume::PbLogicalPlan& plan = request->logical_plan();

        std::vector<std::string> hadoop_commit_args;
        hadoop_commit_args.insert(
                hadoop_commit_args.begin(),
                request->hadoop_commit_args().begin(),
                request->hadoop_commit_args().end());


        flume::runtime::Backend::AppStatus app_status;
        int ret = python::get_status(backend, plan, hadoop_commit_args, &app_status);
        if (ret == 0) {
            status->set_success(true);
            GetStatusResponse_Status app_status_;
            switch (app_status) {
                case flume::runtime::Backend::AM_SUBMIT:
                    response->set_app_status(GetStatusResponse_Status_AM_SUBMIT);
                    break;
                case flume::runtime::Backend::AM_ALLOCATE:
                    response->set_app_status(GetStatusResponse_Status_AM_ALLOCATE);
                    break;
                case flume::runtime::Backend::AM_RUN:
                    response->set_app_status(GetStatusResponse_Status_AM_RUN);
                    break;
                case flume::runtime::Backend::AM_KILL:
                    response->set_app_status(GetStatusResponse_Status_AM_KILL);
                    break;
                case flume::runtime::Backend::AM_FAIL:
                    response->set_app_status(GetStatusResponse_Status_AM_FAIL);
                    break;
                case flume::runtime::Backend::AM_UNKNOWN:
                    response->set_app_status(GetStatusResponse_Status_AM_UNKNOWN);
                    break;
                case flume::runtime::Backend::APP_SUBMIT:
                    response->set_app_status(GetStatusResponse_Status_APP_SUBMIT);
                    break;
                case flume::runtime::Backend::APP_ALLOCATE:
                    response->set_app_status(GetStatusResponse_Status_APP_ALLOCATE);
                    break;
                case flume::runtime::Backend::APP_RUN:
                    response->set_app_status(GetStatusResponse_Status_APP_RUN);
                    break;
                case flume::runtime::Backend::APP_KILL:
                    response->set_app_status(GetStatusResponse_Status_APP_KILL);
                    break;
                case flume::runtime::Backend::APP_FAIL:
                    response->set_app_status(GetStatusResponse_Status_APP_FAIL);
                    break;
                case flume::runtime::Backend::APP_UNKNOWN:
                    response->set_app_status(GetStatusResponse_Status_APP_UNKNOWN);
                    break;
                default:
                    CHECK(false);
            }
        } else {
            status->set_success(false);
            status->set_reason("Get job status failed");
        }
    } else {
        status->set_success(false);
        status->set_reason("Pipeline: " + request->pipeline_id() + " is not registered");
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::is_node_cached(
        ::google::protobuf::RpcController* controller,
        const IsNodeCachedRequest* request,
        SingleBooleanResponse* response,
        ::google::protobuf::Closure* done) {
    ResponseStatus* status = response->mutable_status();
    LOG(INFO) << "Received is_node_cached request, pipeline ID: " << request->pipeline_id()
        << ", node_id: " << request->node_id();
    if (_map.count(request->pipeline_id()) != 0u) {
        const flume::runtime::Backend& backend = _map.at(request->pipeline_id());
        response->set_response(backend.IsNodeCached(request->node_id()));
        status->set_success(true);
    } else {
        status->set_success(false);
        status->set_reason("Pipeline: " + request->pipeline_id() + " is not registered");
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::get_cached_data(
        ::google::protobuf::RpcController* controller,
        const GetCachedDataRequest* request,
        GetCachedDataResponse* response,
        ::google::protobuf::Closure* done) {
    using flume::runtime::Backend;
    using flume::runtime::Session;
    using flume::runtime::CacheManager;

    LOG(INFO) << "Received get_cached_data request, id = " << request->node_id();

    ResponseStatus* status = response->mutable_status();
    if (_map.count(request->pipeline_id()) != 0u) {
        Backend& backend = _map.at(request->pipeline_id());
        Backend::CacheIteratorPtr iter = backend.GetCachedData(request->node_id());
        while (iter->Next()) {
            KeysValue* ksv = response->add_keys_value();
            const std::vector<toft::StringPiece>& keys = iter->Keys();
            for (uint32_t i = 1; i < keys.size(); i ++) {
                ksv->add_key(keys[i].as_string());
            }
            ksv->set_value(iter->ValueStr().as_string());
        }
        LOG(INFO) << "got [" << response->keys_value_size() << "] cached records";
        status->set_success(true);
    } else {
        status->set_success(false);
        status->set_reason("Pipeline: " + request->pipeline_id() + " is not registered");
    }
    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::get_counters(
        ::google::protobuf::RpcController* controller,
        const GetCountersRequest* request,
        GetCountersResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received get_counters request";
    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    typedef std::map<std::string, const baidu::flume::runtime::Counter*> CounterMap;

    CounterMap counters = python::g_counter_session->GetAllCounters();
    for (CounterMap::iterator ptr = counters.begin(); ptr != counters.end(); ++ptr) {
        response->add_name(ptr->first);
        response->add_value(ptr->second->GetValue());
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::reset_counter(
        ::google::protobuf::RpcController* controller,
        const ResetCounterRequest* request,
        ResetCounterResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received reset_counter request";
    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    typedef std::map<std::string, const baidu::flume::runtime::Counter*> CounterMap;
    std::string prefix, name;
    baidu::flume::runtime::CounterSession::GetPrefixFromCounterKey(request->name(), &prefix, &name);
    python::g_counter_session->GetCounter(prefix, name)->Reset();

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::reset_all_counters(
        ::google::protobuf::RpcController* controller,
        const ResetAllCountersRequest* request,
        ResetAllCountersResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received reset_counter request";
    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    typedef std::map<std::string, const baidu::flume::runtime::Counter*> CounterMap;

    python::g_counter_session->ResetAllCounters();

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

}  // namespace service
}  // namespace bigflow
}  // namespace baidu

