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

#include "flume/runtime/platform/hadoop_client.h"
#include "flume/util/config_util.h"
#include "flume/util/hadoop_conf.h"

namespace baidu {
namespace bigflow {
namespace service {

void ServiceImpl::default_hadoop_config_path(
        ::google::protobuf::RpcController* controller,
        const SingleStringRequest* request,
        SingleStringResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received default_hadoop_config request: " << request->request();

    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    response->set_response(flume::util::DefaultHadoopConfigPath());

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::default_hadoop_client_path(
        ::google::protobuf::RpcController* controller,
        const SingleStringRequest* request,
        SingleStringResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received default_hadoop_client request: " << request->request();

    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    response->set_response(flume::util::DefaultHadoopClientPath());

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::default_fs_defaultfs(
        ::google::protobuf::RpcController* controller,
        const SingleStringRequest* request,
        SingleStringResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received default_fs_defaultfs request: " << request->request();

    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    baidu::flume::util::HadoopConf conf(request->request());
    response->set_response(conf.fs_defaultfs());

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::default_hadoop_job_ugi(
        ::google::protobuf::RpcController* controller,
        const SingleStringRequest* request,
        SingleStringResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received default_hadoop_job_ugi request: " << request->request();

    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    baidu::flume::util::HadoopConf conf(request->request());
    response->set_response(conf.hadoop_job_ugi());

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::default_spark_home_path(
        ::google::protobuf::RpcController* controller,
        const SingleStringRequest* request,
        SingleStringResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received default_spark_home_path request: " << request->request();

    response->set_response(flume::util::DefaultSparkHomePath());
    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::tmp_hdfs_path(
        ::google::protobuf::RpcController* controller,
        const SingleStringRequest* request,
        SingleStringResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received default_tmp_hdfs_path request: " << request->DebugString();

    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    response->set_response(flume::util::DefaultTempHDFSPath(request->request()));

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

void ServiceImpl::hadoop_commit(
        ::google::protobuf::RpcController* controller,
        const HadoopCommitRequest* request,
        VoidResponse* response,
        ::google::protobuf::Closure* done) {

    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    using flume::runtime::HadoopClient;
    HadoopClient client(request->hadoop_client_path(), request->hadoop_config_path(), ".");
    std::vector<std::string> args;
    args.insert(args.begin(), request->args().begin(), request->args().end());
    bool is_success = (0 == client.CommitWithArgs(
                args,
                std::vector<std::string>(),
                std::vector<std::string>(),
                NULL,
                NULL));

    status->set_success(is_success);

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

}  // namespace service
}  // namespace bigflow
}  // namespace baidu

