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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "bigflow_python/python_interpreter.h"  // NOLINT

#include <unistd.h>
#include <string>

// can't find why this header must at here
// it's about _POSIX_C_SOURCE and _XOPEN_SOURCE
#include "bigflow_python/common/python.h"  // NOLINT
#include "bigflow_python/python_interpreter.h"  // NOLINT
#include "bigflow_python/python_client.h"  // NOLINT
#include "bigflow_python/register.h"  // NOLINT

#include "brpc/server.h"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/python/def.hpp"
#include "glog/logging.h"
#include "gflags/gflags.h"
#include "toft/base/scoped_ptr.h"
#include "toft/storage/seqfile/local_sequence_file_writer.h"
#include "toft/system/threading/thread.h"

#include "bigflow_python/bigflow.h"
#include "bigflow_python/proto/service.pb.h"
#include "flume/flume.h"
#include "flume/runtime/backend.h"

using ::brpc::Server;
using ::brpc::ServerOptions;
using ::brpc::Controller;

DEFINE_string(service_port, "", "port to use");
DECLARE_int32(flume_default_concurrency);

namespace baidu {
namespace bigflow {
namespace service {

class ServiceImpl : public ::baidu::bigflow::service::BigflowService {
public:
    ServiceImpl() {}
    virtual ~ServiceImpl() {}

    virtual void register_pipeline(
            ::google::protobuf::RpcController* controller,
            const RegisterPipelineRequest* request,
            VoidResponse* response,
            ::google::protobuf::Closure* done);

    virtual void unregister_pipeline(
            ::google::protobuf::RpcController* controller,
            const UnRegisterPipelineRequest* request,
            VoidResponse* response,
            ::google::protobuf::Closure* done);

    virtual void launch(
            ::google::protobuf::RpcController* controller,
            const LaunchRequest* request,
            VoidResponse* response,
            ::google::protobuf::Closure* done);

    virtual void suspend(
            ::google::protobuf::RpcController* controller,
            const SuspendRequest* request,
            VoidResponse* response,
            ::google::protobuf::Closure* done);

    virtual void kill(
            ::google::protobuf::RpcController* controller,
            const KillRequest* request,
            VoidResponse* response,
            ::google::protobuf::Closure* done);

    virtual void get_status(
            ::google::protobuf::RpcController* controller,
            const GetStatusRequest* request,
            GetStatusResponse* response,
            ::google::protobuf::Closure* done);

    virtual void is_node_cached(
            ::google::protobuf::RpcController* controller,
            const IsNodeCachedRequest* request,
            SingleBooleanResponse* response,
            ::google::protobuf::Closure* done);

    virtual void get_cached_data(
            ::google::protobuf::RpcController* controller,
            const GetCachedDataRequest* request,
            GetCachedDataResponse* response,
            ::google::protobuf::Closure* done);

    virtual void write_local_seqfile(
            ::google::protobuf::RpcController* controller,
            const WriteLocalSeqFileRequest* request,
            VoidResponse* response,
            ::google::protobuf::Closure* done);

    virtual void default_hadoop_config_path(
            ::google::protobuf::RpcController* controller,
            const SingleStringRequest* request,
            SingleStringResponse* response,
            ::google::protobuf::Closure* done);

    virtual void default_hadoop_client_path(
            ::google::protobuf::RpcController* controller,
            const SingleStringRequest* request,
            SingleStringResponse* response,
            ::google::protobuf::Closure* done);

    virtual void default_fs_defaultfs(
            ::google::protobuf::RpcController* controller,
            const SingleStringRequest* request,
            SingleStringResponse* response,
            ::google::protobuf::Closure* done);

    virtual void default_hadoop_job_ugi(
            ::google::protobuf::RpcController* controller,
            const SingleStringRequest* request,
            SingleStringResponse* response,
            ::google::protobuf::Closure* done);

    virtual void default_spark_home_path(
            ::google::protobuf::RpcController* controller,
            const SingleStringRequest* request,
            SingleStringResponse* response,
            ::google::protobuf::Closure* done);

    virtual void tmp_hdfs_path(
            ::google::protobuf::RpcController* controller,
            const SingleStringRequest* request,
            SingleStringResponse* response,
            ::google::protobuf::Closure* done);

    virtual void hadoop_commit(
            ::google::protobuf::RpcController* controller,
            const HadoopCommitRequest* request,
            VoidResponse* response,
            ::google::protobuf::Closure* done);

    virtual void get_toft_style_path(
            ::google::protobuf::RpcController* controller,
            const GetToftStylePathRequest* request,
            SingleStringResponse* response,
            ::google::protobuf::Closure* done);

    virtual void get_counters(
            ::google::protobuf::RpcController* controller,
            const GetCountersRequest* request,
            GetCountersResponse* response,
            ::google::protobuf::Closure* done);

    virtual void reset_counter(
            ::google::protobuf::RpcController* controller,
            const ResetCounterRequest* request,
            ResetCounterResponse* response,
            ::google::protobuf::Closure* done);

    virtual void reset_all_counters(
            ::google::protobuf::RpcController* controller,
            const ResetAllCountersRequest* request,
            ResetAllCountersResponse* response,
            ::google::protobuf::Closure* done);


private:
    boost::ptr_map<std::string, flume::runtime::Backend> _map;
};

class Service {
public:
    bool start() {
        _server.reset(new Server());
        _service.reset(new ServiceImpl());
        _thread.reset(new toft::Thread());

        ServerOptions option;
        option.num_threads = 1;
        option.max_concurrency = 1;

        _server->AddService(_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE);

        int ret = -1;
        if (FLAGS_service_port.empty()) {
            ret = _server->Start("127.0.0.1", brpc::PortRange(10000, 50000), &option);
        } else {
            std::string ip_port = "127.0.0.1:" + FLAGS_service_port;
            ret = _server->Start(ip_port.c_str(), &option);
        }
        if (0 != ret) {
            //TODO LOG(ERROR) << "Failed to start Bigflow RPC server: " << hulu::pbrpc::PbrpcError(ret);
            return false;
        }

        butil::EndPoint ep = _server->listen_address();
        std::string host = "";
        ret = endpoint2hostname(ep, &host);
        if (0 != ret) {
            LOG(ERROR) << "Failed to start Bigflow RPC server: endpoint2hostname failed";
            return false;
        }

        // important! father process need this message to get port
        LOG(INFO) << "Bigflow RPC Server started at: " << host;

        _thread->Start(boost::bind(&Service::check_parent_process, this));
        return true;
    }

    int wait_for_stop() {
        if (_server.get()) {
            if (_server->Join() != 0) {
                return -1;
            }
        }

        return 0;
    }

    void stop() {
        if (_server.get()) {
            _server->Stop(0);
        }
    }

private:
    bool check_parent_process() {
        static const int INTERVAL = 500 * 1000; // 0.5s
        while (true) {
            usleep(INTERVAL);
            if (getppid() == 1) {
                stop();
                return true;
            }
        }
        return false;
    }

private:
    toft::scoped_ptr<google::protobuf::Service> _service;
    toft::scoped_ptr<Server> _server;
    toft::scoped_ptr<toft::Thread> _thread;
};

}  // namespace service
}  // namespace bigflow
}  // namespace baidu

#include "bigflow_python/rpc/impls/backends_impl.h"
#include "bigflow_python/rpc/impls/write_local_seqfile_impl.h"
#include "bigflow_python/rpc/impls/config_util_impl.h"
#include "bigflow_python/rpc/impls/get_toft_style_path_impl.h"

void initialize() {
    baidu::bigflow::python::PythonInterpreter::Instance()
        ->set_exception_handler(&baidu::bigflow::python::throw_exception_to_client); // init env
    baidu::bigflow::python::PythonInterpreter::Instance()
        ->set_exception_handler_with_error_msg(&baidu::bigflow::python::throw_exception_to_client); // init env
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "show_registered_class_name") {
        baidu::bigflow::python::register_classes();
        return 0;
    }

    try {
        ::baidu::bigflow::InitBigflow(argc, argv, initialize);
        ::baidu::bigflow::service::Service service;
        if (service.start()) {
            return service.wait_for_stop();
        } else {
            return -1;
        }
    } catch(boost::python::error_already_set) {
        PyErr_Print();
        return -1;
    }
}
