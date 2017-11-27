/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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

#include "flume/runtime/spark/spark_job.h"

#include <cstdlib>
#include <iostream>  // NOLINT
#include <string>

#include "boost/filesystem.hpp"
#include "boost/foreach.hpp"
#include "gflags/gflags.h"
#include "re2/re2/re2.h"
#include "toft/base/array_size.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/encoding/shell.h"
#include "toft/storage/path/path.h"
#include "toft/system/process/sub_process.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/core/entity.h"
#include "flume/core/sinker.h"
#include "flume/runtime/backend.h"
#include "flume/util/config_util.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

using core::Entity;
using core::Sinker;
using re2::RE2;

const std::string SparkJob::kSparkJarName = "spark_launcher.jar";

class SparkJob::Impl {
public:
    Impl(const PbJobConfig& job_config, const std::string& resource_path):
        _resource_path(resource_path),
        _mode("JNI"),
        _job_config(job_config) {
            for (int i = 0; i < job_config.kv_config_size(); ++i) {
                if (job_config.kv_config(i).key() == "flume.runtime.spark.mode") {
                    _mode = job_config.kv_config(i).value();
                    if (_mode != "PIPE" && _mode != "JNI") {
                        LOG(FATAL) << "Invalid Spark runtime-mode: " << _mode;
                    }
                }
            }
            LOG(INFO) << "Flume running Apache Spark in " << _mode;
        }

    virtual ~Impl() {}

    bool run() {
        std::vector<std::string> args;
        args.push_back(_job_config.spark_home_path() + "/bin/spark-submit");

        args.push_back("--class");
        args.push_back("com.baidu.flume.runtime.spark.SparkJob");

        set_kv_config(&args);

        // args.push_back("--driver-class-path");
        // args.push_back("javalib/spark_launcher.jar");

        args.push_back("javalib/spark_launcher.jar");

        args.push_back("--pb_job_message");
        args.push_back("flume/plan");

        args.push_back("--type");
        args.push_back(_mode);

        // tmp_data_path
        args.push_back("--prepared_archive_path");
        args.push_back(_job_config.prepared_archive_path());

        // added files
        args.push_back("--application_archive");
        // separated by ; for more files
        args.push_back(create_archive());

        LOG(INFO) << "Submit command: " << toft::JoinCommandLine(args);

        toft::SubProcess::CreateOptions options;
        options.SetWorkDirectory(_resource_path);
        options.AddEnvironment("GLOG_logtostderr", "1");
        _process.reset(new toft::SubProcess());
        _process->Create(args, options);
        _process->WaitForExit();
        LOG(INFO) << "Done spark job commit, errcode: " << _process->ExitCode();

        return 0u == _process->ExitCode();
    }

    bool kill() {}

    std::string job_id() { return ""; }
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
            system(cmd.c_str());
        }
        return tar_file;
    }

private:
    std::string _resource_path;
    std::string _mode;
    PbJobConfig _job_config;
    toft::scoped_ptr<toft::SubProcess> _process;
};

SparkJob::SparkJob(const PbJobConfig& job_config, const std::string& resource_path)
    : _impl(new SparkJob::Impl(job_config, resource_path)) {
}

SparkJob::~SparkJob() {
}

bool SparkJob::run() {
    return _impl->run();
}

void SparkJob::kill() {
    _impl->kill();
}

std::string SparkJob::job_id() {
    return _impl->job_id();
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
