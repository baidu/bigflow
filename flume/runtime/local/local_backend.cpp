/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/runtime/local/local_backend.h"

#include <cstdlib>
#include <fstream>  // NOLINT(readability/streams)

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"
#include "ctemplate/template.h"
#include "toft/base/class_registry.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/path/path.h"
#include "toft/system/process/this_process.h"

#include "flume/flags.h"
#include "flume/planner/local/local_planner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/common/file_cache_manager.h"
#include "flume/runtime/common/local_dataset.h"
#include "flume/runtime/local/local_executor_factory.h"
#include "flume/runtime/local/flags.h"
#include "flume/runtime/resource.h"
#include "flume/runtime/task.h"
#include "flume/flume-runtime-local-scripts.h"
#include "flume/util/config_util.h"
#include "flume/core/entity.h"
#include "flume/core/environment.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace local {

using core::Entity;
using core::Environment;

static const char* kServerAddressPrefix = "LogServerAddress:";

REGISTER_BACKEND("local", LocalBackend);

LocalBackend::LocalBackend() {
    m_backend_unique_id = toft::CreateCanonicalUUIDString();

    m_job_config.set_hadoop_config_path(util::DefaultHadoopConfigPath());
    m_job_config.set_hadoop_client_path(util::DefaultHadoopClientPath());

    m_session.reset(new runtime::Session());

    m_entry = NULL;
}

LocalBackend::LocalBackend(const PbLocalConfig& config) {
    m_backend_unique_id = toft::CreateCanonicalUUIDString();
    // Hadoop configuration path
    m_job_config.set_hadoop_config_path(config.hadoop_config_path().empty()
            ? util::DefaultHadoopConfigPath() : config.hadoop_config_path());
    // Hadoop client path
    m_job_config.set_hadoop_client_path(config.hadoop_client_path().empty()
            ? util::DefaultHadoopClientPath() : config.hadoop_client_path());
    // Tmp path
    std::string path =
        toft::Path::ToAbsolute(flume::util::DefaultTempLocalPath(m_backend_unique_id));
    m_job_config.set_tmp_data_path_input(path);
    m_job_config.set_tmp_data_path_output(path);

    // Stream
    m_job_config.set_round_times(config.round_times());

    // Status
    m_job_config.set_status_as_meta(config.status_as_meta());
    if (config.has_status_meta_path()) {
        m_job_config.set_status_meta_path(config.status_meta_path());
    }
    if (config.has_status_table_path()) {
        m_job_config.set_status_table_path(config.status_table_path());
    }
    if (config.has_status_cache_size()) {
        m_job_config.set_status_cache_size(config.status_cache_size());
    }
    if (config.has_use_leveldb_storage()) {
        m_job_config.set_local_use_leveldb_storage(config.use_leveldb_storage());
    }

    m_session.reset(new runtime::Session());
}

LocalBackend::~LocalBackend() {}

int32_t LocalBackend::RunExecute() {
    std::string prog = "/flume/start.sh";
    std::vector<std::string> args;
    args.push_back(m_resource->ViewAsDirectory() + prog);

    toft::SubProcess::CreateOptions options;
    options.SetWorkDirectory(m_resource->ViewAsDirectory());
    options.AddEnvironment("GLOG_logtostderr", "1");
    SetLibraryPath(&options);

    m_process.reset(new toft::SubProcess());
    m_process->Create(args, options);
    m_process->WaitForExit();
    LOG(INFO) << "Done local job, errcode:" << m_process->ExitCode();
    return m_process->ExitCode();
}

int32_t LocalBackend::RunCommit() {
    std::string prog = "/flume/commit.sh";
    std::vector<std::string> args;
    args.push_back(m_resource->ViewAsDirectory() + prog);

    toft::SubProcess::CreateOptions options;
    options.SetWorkDirectory(m_resource->ViewAsDirectory());
    options.AddEnvironment("GLOG_logtostderr", "1");
    SetLibraryPath(&options);

    m_process.reset(new toft::SubProcess());
    m_process->Create(args, options);
    m_process->WaitForExit();
    LOG(INFO) << "Done local job commit, errcode:" << m_process->ExitCode();
    return m_process->ExitCode();
}

Backend::Status LocalBackend::Launch(
        const PbLogicalPlan& plan,
        Resource* resource,
        CounterSession* counters) {
    LOG(INFO) << "Launching local job ...";

    m_resource.reset(resource);
    m_counters.reset(new CounterSession());
    toft::scoped_ptr<Session> current_session(m_session->Clone());

    // Build runtime environment
    Resource::Entry* entry = GetFlumeEntry(m_resource.get());
    entry->AddExecutable("worker", toft::ThisProcess::BinaryPath());
    CHECK_EQ(Resource::kOk, resource->GetStatus());

    // Use start.sh to start flume execute mode
    RenderScripts();

    // Generate plan
    planner::local::LocalPlanner planner(current_session.get(), &m_job_config);
    planner.SetDebugDirectory(entry->GetEntry("dot"));
    PbPhysicalPlan physical_plan = planner.Plan(plan);

    std::string content = physical_plan.SerializeAsString();
    entry->AddNormalFileFromBytes("physical_plan", content.data(), content.length());

    // Run execute
    m_result.ok = 0 == RunExecute();

    // Run commit
    if (m_result.ok) {
        m_result.ok = 0 == RunCommit();
    }

    // Update session
    toft::MutexLocker lock(&m_mutex);
    if (m_result.ok) {
        m_session.swap(current_session);
    }
    counters->Merge(*m_counters);

    return m_result;
}

void LocalBackend::RenderScripts() {
    ctemplate::TemplateDictionary dict("flume");

    AddScript(dict, "start.sh", RESOURCE_flume_runtime_local_static_start_sh);
    AddScript(dict, "commit.sh", RESOURCE_flume_runtime_local_static_commit_sh);
}

template<size_t N>
void LocalBackend::AddScript(
        const ctemplate::TemplateDictionary& dict,
        const std::string& name,
        const char (&data)[N]) {
    Resource::Entry* entry = GetFlumeEntry(m_resource.get());
    // the key of file must be unique in process
    std::string key = "local-" + name;
    std::string content;
    ctemplate::StringToTemplateCache(key, data, N, ctemplate::DO_NOT_STRIP);
    ctemplate::ExpandTemplate(key, ctemplate::DO_NOT_STRIP, &dict, &content);
    entry->AddExecutableFromBytes(name, content.data(), content.size());
}

void LocalBackend::Commit() {
    PbPhysicalPlan plan;
    {
        std::ifstream stream("./flume/physical_plan", std::ios_base::binary | std::ios_base::in);
        CHECK(plan.ParseFromIstream(&stream));
    }
    LOG(INFO) << "Loading output information from plan ...";
    const PbJob& job = plan.job(0);
    std::vector<PbExecutor> executors;
    Backend::GetExecutors(job.local_job().task().root(), PbExecutor_Type_LOGICAL, &executors);
    Backend::CommitOutputs(executors);
}

void LocalBackend::Execute() {
    // Run commit
    if (FLAGS_flume_local_commit) {
        Commit();
        return;
    }

    // Run execute
    PbPhysicalPlan plan;
    {
        std::ifstream stream("./flume/physical_plan", std::ios_base::binary | std::ios_base::in);
        CHECK(plan.ParseFromIstream(&stream));
    }

    const PbJob& job = plan.job(0);

    CHECK(plan.has_environment()) << "PbPhysicalPlan does not have environment.";
    const PbEntity& env_message = plan.environment();
    Entity<Environment> entity = Entity<Environment>::From(env_message);
    boost::scoped_ptr<Environment> env(entity.CreateAndSetup());
    env->do_setup();

    // Load Config from job message, so that the CreateCacheManager will work as normal
    m_job_config = job.job_config();

    runtime::LocalDatasetManager dataset_manager;
    dataset_manager.Initialize(".swap", FLAGS_flume_local_max_memory_metabytes * 1024 * 1024);

    runtime::local::LocalExecutorFactory factory(this);

    runtime::Task task;
    const PbExecutor& message = job.local_job().task().root();
    factory.Initialize(job.local_job(), &dataset_manager);

    task.Initialize(message, &factory);
    task.Run("");

    env->do_cleanup();
    return;
    //std::exit(0);
}

void LocalBackend::Abort(const std::string& reason, const std::string& detail) {
    LOG(ERROR) << "Local execution aborted: " << reason;

    if (m_process) {
        if (m_process->IsAlive()) {
            toft::MutexLocker lock(&m_mutex);
            m_result.ok = false;
            m_result.reason = reason;
            m_result.detail = detail;

            m_process->Terminate();
        }
    } else {
        exit(-1);
    }
}

void LocalBackend::KillJob(const std::string& reason, const std::string& detail) {
    toft::MutexLocker lock(&m_mutex);
    m_result.reason = reason;
    m_result.detail = detail;
    m_result.ok = false;
}

void LocalBackend::UpdateCounter(const std::map<std::string, uint64_t>& counters) {
    toft::MutexLocker lock(&m_mutex);
    std::map<std::string, uint64_t>::const_iterator it = counters.begin();
    for (; it != counters.end(); ++it) {
        runtime::Counter* counter = m_counters->GetCounter(it->first);
        counter->Update(it->second);
    }
}

void LocalBackend::SetLibraryPath(toft::SubProcess::CreateOptions* options) {
    std::string base = m_resource->ViewAsDirectory();
    std::string ld_library_path = google::StringFromEnv("LD_LIBRARY_PATH", "");
    ld_library_path += ":" + base + "/lib";
    options->AddEnvironment("LD_LIBRARY_PATH", ld_library_path.c_str());

    std::string class_path = google::StringFromEnv("CLASSPATH", "");
    BOOST_FOREACH(const std::string& jar_file, m_resource->ListJavaLibraries()) {
        class_path += ":" + base + "/javalib/" + jar_file;
    }
    options->AddEnvironment("CLASSPATH", class_path.c_str());

    std::string python_path = google::StringFromEnv("PYTHONPATH", "");
    BOOST_FOREACH(const std::string& egg_file, m_resource->ListPythonLibraries()) {
        python_path += ":" + base + "/pythonlib/" + egg_file;
    }
    options->AddEnvironment("PYTHONPATH", python_path.c_str());
}

CacheManager* LocalBackend::CreateCacheManager() {
    return new FileCacheManager(m_job_config.tmp_data_path_output(),  "local-task");
}

}  // namespace local
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
