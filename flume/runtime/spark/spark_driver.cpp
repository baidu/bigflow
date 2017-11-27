/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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
// Author: Ye, Xianjin(yexianjin@baidu.com)
//

#include "flume/runtime/spark/spark_driver.h"

#include <cstdlib>
#include <iostream>  // NOLINT
#include <string>
#include <utility>

#include "boost/asio/ip/tcp.hpp"
#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"
#include "gflags/gflags.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TBufferTransports.h"
#include "thrift/protocol/TBinaryProtocol.h"
#include "toft/base/array_size.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/encoding/shell.h"
#include "toft/storage/path/path.h"
#include "toft/system/process/sub_process.h"
#include "toft/storage/file/local_file.h"
#include "toft/storage/seqfile/data_input_buffer.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/thrift/TBackendService.h"
#include "flume/core/entity.h"
#include "flume/core/sinker.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/spark/spark_cache_iterator.h"
#include "flume/util/config_util.h"

namespace baidu {
namespace flume {
class TBackendServiceClient;

namespace runtime {
namespace spark {

using core::Entity;
using core::Sinker;

const char* SparkDriver::kSparkJarName = "spark_launcher.jar";
const char* SparkDriver::kLog4jConfiguration =
    "!/com/baidu/flume/runtime/spark/log4j-defaults.properties";

class SparkDriver::Impl {
public:
    Impl(const PbJobConfig& job_config, const std::string& resource_path):
        _resource_path(resource_path),
        _job_config(job_config),
        _log4j_configuration(""),
        _spark_user(""),
        _stopped(false) {
        for (int i = 0; i < job_config.kv_config_size(); ++i) {
            // extract driver's log4j configuration path
            if (job_config.kv_config(i).key() == "flume.runtime.spark.log4j.configuration") {
                _log4j_configuration = "file://" + job_config.kv_config(i).value();
            }

            // extract spark user from hadoop.job.ugi
            if (job_config.kv_config(i).key() == "hadoop.job.ugi") {
                std::string ugi = job_config.kv_config(i).value();
                _spark_user = ugi.substr(0, ugi.find(","));
            }
        }
        if (!_log4j_configuration.size()) {
            std::string jar_path =
                toft::Path::Join(resource_path, "javalib", std::string(kSparkJarName));
            _log4j_configuration = "jar:file://" + jar_path + std::string(kLog4jConfiguration);
        }
        _backend_file = toft::Path::Join(resource_path, toft::CreateCanonicalUUIDString());
    }

    virtual ~Impl() {
    }

    bool start() {
        LOG(INFO) << "Start Spark Driver from backend";
        std::vector<std::string> args;
        args.push_back(_job_config.spark_home_path() + "/bin/spark-submit");

        // Spark driver would use the default log4j configuration in spark_launcher.jar unless
        // user specified the configuration path in job_config.
        if (_log4j_configuration.size()) {
            args.push_back("--driver-java-options");
            args.push_back("-Dlog4j.configuration=" + _log4j_configuration);
        }


        args.push_back("--class");
        args.push_back("com.baidu.flume.runtime.spark.backend.FlumeBackendServer");

        set_kv_config(&args);

        args.push_back("javalib/" + std::string(kSparkJarName));

        // tmp_data_path
        args.push_back("--prepared_archive_path");
        args.push_back(_job_config.prepared_archive_path());

        // added files
        args.push_back("--application_archive");
        _archive_path = create_archive();
        args.push_back(_archive_path);

        args.push_back("--tmp_setup_file");
        args.push_back(_backend_file);
        LOG(INFO) << "Submit command: " << toft::JoinCommandLine(args);

        toft::SubProcess::CreateOptions options;
        options.SetWorkDirectory(_resource_path);
        options.AddEnvironment("GLOG_logtostderr", "1");
        if (_spark_user.size()) {
            options.AddEnvironment("SPARK_USER", _spark_user.data());
        }
        _process.reset(new toft::SubProcess());
        bool ret =  _process->Create(args, options);
        CHECK(ret) << "Start Spark Driver failed";
        // wait for spark driver to register its monitor port and rpc port
        wait_and_setup_channel(300 * 1000 * 1000); // 300s
        LOG(INFO) << "Spark Driver started";
        return true;
    }

    bool run_job(const PbJob& job) {
        CHECK(_driver_client.get());
        TVoidResponse response;
        LOG(INFO) << "Start run_job request";
        _driver_client->runJob(response, job.SerializeAsString());
        LOG(INFO) << "Response status: " << response.status.success;
        return response.status.success;
    }

    boost::shared_ptr<KVIterator> get_cache_data(const std::string& node_id) {
        TGetCachedDataResponse response;
        _driver_client->getCachedData(response, node_id);
        boost::shared_ptr<spark::CacheIterator> iter(new spark::CacheIterator);
        for (auto record : response.data) {
            iter->Put(record.keys, record.value);
        }
        return iter;
    }

    bool kill() {return true;}

    void stop() {
        if (!_stopped) {
            _driver_client->stop();
            _t_transport->close();
            _socket->close();
            // delete archive path
            if (!_archive_path.empty() && toft::File::Exists(_archive_path)) {
                toft::File::Delete(_archive_path);
            }
            _stopped = true;
        }
    }

private:

    void set_kv_config(std::vector<std::string>* args) {
        for (int i = 0; i < _job_config.kv_config_size(); ++i) {
            const PbKVConfig& kv_config = _job_config.kv_config(i);
            // only pass configures start with `spark.` to spark-submit
            std::string prefix = "spark.";
            // key startswith spark.
            if (kv_config.key().compare(0, prefix.length(), prefix) == 0) {
                args->push_back("--conf");
                args->push_back(kv_config.key() + "=" + kv_config.value());
            }
        }
    }

    std::string create_archive() {
        // package resource files
        std::string tar_file =
            toft::Path::GetCwd() + "/.flume-app-" + toft::CreateCanonicalUUIDString() + ".tar.gz";
        {
            std::string cmd = "tar -zcf " + tar_file
                              + " --exclude=flume/worker"
                              + " -C " + _resource_path + " .";
            LOG(INFO) << "Command: " << cmd;
            CHECK_EQ(0, system(cmd.c_str())) << "Create archive failed";
        }
        return tar_file;
    }

    void wait_and_setup_channel(int wait_us) {
        toft::LocalFileSystem fs;
        int total_wait_us = 0;
        int sleep_interval = 10 * 1000; // 10 ms
        while(total_wait_us < wait_us) {
            if (fs.Exists(_backend_file)) {
                toft::scoped_ptr<toft::File> backend_file(fs.Open(_backend_file, "r"));
                std::string m_error;
                toft::DataInputBuffer dis(backend_file.release(),&m_error);
                int backend_port, monitor_port;
                dis.ReadInt(&backend_port);
                dis.ReadInt(&monitor_port);
                CHECK(m_error.empty()) << m_error;
                dis.Reset();
                LOG(INFO) << "backend port: " << backend_port << ", monitor port: " << monitor_port;
                if (backend_port == 0 || monitor_port == 0) {
                    CHECK(false) << "Spark Driver didn't start up correctly";
                }
                // Delete _backend_file
                fs.Delete(_backend_file); // best effort to delete tmp file
                _socket.reset(new boost::asio::ip::tcp::socket(io_service));
                boost::asio::ip::tcp::endpoint
                    endpoint(boost::asio::ip::address::from_string("127.0.0.1"), monitor_port);
                _socket->connect(endpoint);
                _t_socket.reset(new apache::thrift::transport::TSocket("127.0.0.1", backend_port));
                _t_transport.reset(new apache::thrift::transport::TFramedTransport(_t_socket));
                _t_protocol.reset(new apache::thrift::protocol::TBinaryProtocol(_t_transport));
                _driver_client.reset(new baidu::flume::TBackendServiceClient(_t_protocol));
                // connect spark driver
                _t_transport->open();
                return;
            }
            usleep(sleep_interval);
            total_wait_us += sleep_interval;
        }
        CHECK(false) << "Spark Driver didn't start up in " << wait_us + " us";
    }

private:
    std::string _resource_path;
    std::string _backend_file;
    std::string _log4j_configuration;
    std::string _spark_user; // deal with hadoop's ugi problem
    bool _stopped;
    std::string _archive_path;
    boost::asio::io_service io_service;
    toft::scoped_ptr<boost::asio::ip::tcp::socket> _socket;
    boost::shared_ptr<::apache::thrift::transport::TSocket> _t_socket;
    boost::shared_ptr<::apache::thrift::transport::TTransport> _t_transport;
    boost::shared_ptr<::apache::thrift::protocol::TProtocol> _t_protocol;
    toft::scoped_ptr<baidu::flume::TBackendServiceClient> _driver_client;
    PbJobConfig _job_config;
    toft::scoped_ptr<toft::SubProcess> _process;
};

SparkDriver::SparkDriver(const PbJobConfig& job_config, const std::string& resource_path)
    : _impl(new SparkDriver::Impl(job_config, resource_path)) {
}

SparkDriver::~SparkDriver(){
    _impl->stop();
}

bool SparkDriver::start() {
    return _impl->start();
}

bool SparkDriver::run_job(const PbJob job) {
    return _impl->run_job(job);
}

boost::shared_ptr<KVIterator> SparkDriver::get_cache_data(const std::string &node_id) {
    return _impl->get_cache_data(node_id);
}

void SparkDriver::stop() {
    _impl->stop();
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

