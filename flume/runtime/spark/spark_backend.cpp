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

#include "flume/runtime/spark/spark_backend.h"

#include <cstdlib>
#include <fstream>  // NOLINT(readability/streams)

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
//#include "fake_thirdlib/libgoogle_so.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
//#include "googlelib/libhdfs_so.h"
//#include "googlelib/libgoogle_so.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "re2/re2/re2.h"
#include "toft/base/class_registry.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/path/path.h"
#include "toft/system/process/this_process.h"
#include "toft/system/process/sub_process.h"

#include "flume/flags.h"
#include "flume/planner/spark/spark_planner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/common/file_cache_manager.h"
#include "flume/runtime/common/local_dataset.h"
#include "flume/runtime/resource.h"
#include "flume/runtime/task.h"
#include "flume/runtime/spark/hadoop_input_executor.h"
#include "flume/flume-runtime-spark-profile_scripts.h"
#include "flume/runtime/spark/spark_job.h"
#include "flume/runtime/spark/spark_driver.h"
#include "flume/flume-runtime-spark-spark_launcher_jar.h"
#include "flume/runtime/spark/spark_task.h"
//#include "flume/service/log_service.h"
//#include "flume/service/log_service_util.h"
//#include "flume/service/server_launcher.h"
#include "flume/util/config_util.h"

DECLARE_bool(flume_commit);

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

namespace {

template <int N>
void AddJavaLibraryFromBytes(
        Resource* resource,
        const std::string& name,
        const char (&bytes) [N]) {
    resource->AddJavaLibraryFromBytes(name, bytes, N);
}

template <int N>
void AddResourceFromBytes(const std::string& name, const char (&bytes) [N],
        Resource::Entry* entry) {
    entry->AddNormalFileFromBytes(name, bytes, N);
}

}  // namespace

REGISTER_BACKEND("spark", SparkBackend);

SparkBackend::SparkBackend(): _launch_id(0) {
    common_initialize();
    _backend_unique_id = toft::CreateCanonicalUUIDString();

    m_job_config.set_hadoop_config_path(util::DefaultHadoopConfigPath());
    m_job_config.set_hadoop_client_path(util::DefaultHadoopClientPath());
}

SparkBackend::SparkBackend(const PbSparkConfig& config): _launch_id(0) {
    common_initialize();

    // set cpu and heap profiling switch
    m_job_config.set_cpu_profile(config.cpu_profile());
    m_job_config.set_heap_profile(config.heap_profile());

    // m_job_config.tmp_data_path = config.tmp_data_path();
    m_job_config.set_prepared_archive_path(config.prepared_archive_path());

    _backend_unique_id = toft::CreateCanonicalUUIDString();
    // Hadoop configuration path
    m_job_config.set_hadoop_config_path(config.hadoop_config_path().empty()
            ? util::DefaultHadoopConfigPath() : config.hadoop_config_path());
    // Hadoop client path
    m_job_config.set_hadoop_client_path(config.hadoop_client_path().empty()
            ? util::DefaultHadoopClientPath() : config.hadoop_client_path());
    // Spark home path
    m_job_config.set_spark_home_path(config.spark_home_path().empty()
            ? util::DefaultSparkHomePath() : config.spark_home_path());

    if (config.has_default_concurrency()) {
        m_job_config.set_default_concurrency(config.default_concurrency());
    }

    // Temp path
    std::string path =
        toft::Path::ToAbsolute(flume::util::DefaultTempHDFSPath(_backend_unique_id));
    m_job_config.set_tmp_data_path_input(path);
    m_job_config.set_tmp_data_path_output(path);

    for (int i = 0; i < config.kv_config_size(); ++i) {
        *m_job_config.add_kv_config() = config.kv_config(i);
    }
}

SparkBackend::~SparkBackend() {
    LOG(INFO) << "Desctructing SparkBackend";
}

Backend::Status SparkBackend::Launch(
        const PbLogicalPlan& plan,
        Resource* resource,
        CounterSession* counters) {
    LOG(INFO) << "Launching Spark job ...";
    toft::scoped_ptr<Resource> to_be_update_resource; // auto delete after launch
    PbJobConfig merged_job_conf(m_job_config);
    if (_driver.get() == NULL) {
        _resource.reset(resource);
        _counters.reset(new CounterSession());
        _entry = GetFlumeEntry(resource);
        _entry->AddExecutable("worker", toft::ThisProcess::BinaryPath());

        AddJavaLibraryFromBytes(
                resource,
                SparkDriver::kSparkJarName,
                RESOURCE_flume_runtime_spark_static_spark_launcher_jar);

        Resource::Entry* flume_entry = resource->GetRootEntry()->GetEntry("flume");
        AddScript("draw.sh", RESOURCE_flume_runtime_spark_static_draw_sh, flume_entry);
        AddScript("pprof", RESOURCE_flume_runtime_spark_static_pprof, flume_entry);

        CHECK_EQ(Resource::kOk, resource->GetStatus());


        if (!resource->GetCacheFileList().empty()) {
            PbKVConfig* item = merged_job_conf.add_kv_config();
            item->set_key("__bigflow_on_spark__.cache.files");
            LOG(INFO) << "Bigflow cache files: " << resource->GetCacheFileList();
            item->set_value(resource->GetCacheFileList());
        }
        if (!resource->GetCacheArchiveList().empty()) {
            PbKVConfig* item = merged_job_conf.add_kv_config();
            item->set_key("__bigflow_on_spark__.cache.archives");
            LOG(INFO) << "Bigflow cache archives: " << resource->GetCacheArchiveList();
            item->set_value(resource->GetCacheArchiveList());
        }
        _driver.reset(new SparkDriver(merged_job_conf, resource->ViewAsDirectory()));
        _driver->start();
    } else {
        to_be_update_resource.reset(resource);
    }

    /* Generate plan */
    toft::scoped_ptr<Session> to_be_update_session(m_session->Clone());

    planner::spark::SparkPlanner planner(to_be_update_session.get(), &merged_job_conf);
    planner.SetDebugDirectory(
        _entry->GetEntry("dot-" + boost::lexical_cast<std::string>(_launch_id++)));

    PbPhysicalPlan physical_plan = planner.Plan(plan);
    CHECK_EQ(1, physical_plan.job_size());
    const PbJob& pb_job = physical_plan.job(0);

    std::string job_string = pb_job.SerializeAsString();
    // plan in every new resource's flume directory
    GetFlumeEntry(resource)->AddNormalFileFromBytes("plan", job_string.data(), job_string.length());
    _result.ok = _driver->run_job(pb_job);

    if (!_result.ok) {
        _driver->stop();
    }
    if (_result.ok &&
            !(m_job_config.has_immediately_commit() && m_job_config.immediately_commit())) {
        std::vector<std::string> args;

        args.push_back(toft::ThisProcess::BinaryPath());
        args.push_back("--flume_execute");
        args.push_back("--flume_backend=spark");
        args.push_back("--flume_commit");

        toft::SubProcess::CreateOptions options;
        options.SetWorkDirectory(resource->ViewAsDirectory());
        options.AddEnvironment("GLOG_logtostderr", "1");
        toft::scoped_ptr<toft::SubProcess> process;
        process.reset(new toft::SubProcess());
        process->Create(args, options);
        process->WaitForExit();
        if (process->ExitCode() != 0) {
            LOG(WARNING) << "Error commiting Spark job, errcode:" << process->ExitCode();
            _result.ok = false;
        } else {
            LOG(INFO) << "Done commiting Spark job";
        }
    }

    if (_result.ok) {
        m_session.swap(to_be_update_session);
    }

    return _result;
}

template<size_t N>
void SparkBackend::AddScript(const std::string name, const char (&data)[N], Resource::Entry* entry) {
    entry->AddExecutableFromBytes(name, data, N);
}

void SparkBackend::Abort(const std::string& reason, const std::string& detail) {
}

void SparkBackend::KillJob(const std::string& reason, const std::string& detail) {
}

void SparkBackend::Execute() {
    PbJob pb_job;
    LoadJobMessage(&pb_job);
    const PbSparkJob& pb_spark_job = pb_job.spark_job();

    if (FLAGS_flume_commit) {
        // Commit the job
        if (m_job_config.has_immediately_commit() && m_job_config.immediately_commit()) {
            LOG(FATAL) << "Not implemented yet!";
        } else {
            LOG(INFO) << "Loading output information from plan ...";
            for (int i = 0; i < pb_spark_job.rdd_size(); ++i) {
                const PbSparkRDD& rdd = pb_spark_job.rdd(i);
                for (int j = 0; j < rdd.task_size(); ++j) {
                    const PbSparkTask& task = rdd.task(j);
                    std::vector<PbExecutor> executors;
                    Backend::GetExecutors(task.root(), PbExecutor_Type_LOGICAL, &executors);
                    Backend::CommitOutputs(executors);
                }
            }
        }
    } else {
        // Run as executor when at Spark-Pipe mode

        PbSparkRDD pb_spark_rdd = pb_spark_job.rdd(0);

        int32_t task_index = SparkBackend::GetTaskIndex();
        CHECK_GE(task_index, 0);

        // FIXME this is not correct actually, just for a raw test
        PbSparkTask pb_spark_task = pb_spark_rdd.task(task_index);

        int64_t partition = SparkBackend::GetPartitionNumber();
        CHECK_GE(partition, 0);
        _task.reset(new SparkTask(
                    pb_spark_job.job_info(),
                    pb_spark_task,
                    /*is_use_pipe=*/true,
                    partition));

        _task->run("tmp");

        while (fgets(_input_buffer.get(), 64 * 1024, stdin)) {
            _task->input_executor()->process_input("", _input_buffer.get());
        }
        LOG(INFO) << "Calling input_done!";
        _task->input_done();
        LOG(INFO) << "Done processing inputs";
    }
}

CacheManager* SparkBackend::CreateCacheManager() {
    return NULL;
}

void SparkBackend::common_initialize() {
    m_session.reset(new runtime::Session());
    _input_buffer.reset(new char[64 * 1024]);
}

void SparkBackend::LoadJobMessage(PbJob* message) {
    // According comments in protobuf/io/coded_stream.h:326, 512MB will cause integer overflow in
    // protobuf
    using ::google::protobuf::io::IstreamInputStream;
    using ::google::protobuf::io::CodedInputStream;
    static const int kTotalBytesLimit = 512 * 1024 * 1024 - 1;
    std::ifstream stream("./flume/plan", std::ios_base::binary | std::ios_base::in);
    IstreamInputStream raw_in(&stream);
    CodedInputStream coded_in(&raw_in);
    coded_in.SetTotalBytesLimit(kTotalBytesLimit, kTotalBytesLimit);
    CHECK(message->ParseFromCodedStream(&coded_in))
            << "parse plan protobuf message failed";
}

int32_t SparkBackend::GetTaskIndex() {
    return FLAGS_flume_spark_task_index;
}

int64_t SparkBackend::GetPartitionNumber() {
    LOG(INFO) << "Partition number prefix: [" << FLAGS_flume_spark_partition_prefix << "]";
    re2::RE2 partition_regex(FLAGS_flume_spark_partition_prefix + "(\\d+)");

    toft::scoped_array<char> input_stdin(new char[64 * 1024]);
    CHECK(fgets(input_stdin.get(), 64 * 1024, stdin));
    int32_t partition;
    if (re2::RE2::PartialMatch(input_stdin.get(), partition_regex, &partition)) {
        LOG(INFO) << "Parsed partition number: [" << partition << "]";
        return partition;
    } else {
        LOG(FATAL)
            << "Failed parsing partition number from Spark Pipe: " << input_stdin.get();
        return -1;
    }
}

boost::shared_ptr<KVIterator> SparkBackend::GetCachedData(const std::string& identity) {
    CHECK_NOTNULL(_driver.get());
    return _driver->get_cache_data(identity);
}


}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
