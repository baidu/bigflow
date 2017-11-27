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

#include "boost/python.hpp"

#include "python_client.h"

#include <fstream>  // NOLINT(readability/streams)
#include <iostream>

#include "flume/flume.h"
#include "flume/planner/local/local_planner.h"
#include "flume/runtime/backend.h"
#include "flume/core/logical_plan.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/local/local_backend.h"
#include "flume/runtime/local/local_executor_factory.h"
#include "flume/runtime/task.h"
#include "flume/util/reflection.h"
#include "boost/shared_ptr.hpp"
#include "boost/python.hpp"
#include "toft/base/scoped_ptr.h"

#include "bigflow_python/register.h"
#include "bigflow_python/proto/python_resource.pb.h"

DEFINE_bool(bigflow_python_keep_resource, false, "do not delete resource");
DEFINE_bool(bigflow_python_test, false, "run python API locally");
DEFINE_bool(bigflow_python_local, false, "run python API locally");
DEFINE_string(bigflow_python_path, ".", "path of logical plan message");
//DEFINE_string(bigflow_python_resource_path, "./resources", "path of resources message");

namespace baidu {
namespace bigflow {
namespace python {

flume::runtime::CounterSession* g_counter_session = new flume::runtime::CounterSession();

namespace {

// a simple impl, just to show the registered items to stdout
class ReflectionRegister {
public:
    template<typename Base, typename T>
    void add(const std::string& key) {
        using flume::Reflection;
        std::string name = Reflection<Base>::template TypeName<T>();
        std::cout << key << "|" << name << std::endl;
    }

    void show() {}
};

}  // namespace

int launch(flume::runtime::Backend& backend,
           const flume::PbLogicalPlan& logical_plan,
           const PbPythonResource& python_resource,
           const std::vector<std::string>& hadoop_commit_args) {
    bool keep_resource = FLAGS_bigflow_python_keep_resource;
    flume::runtime::Resource* resource = new flume::runtime::Resource(!keep_resource);

    for (int i = 0; i < python_resource.normal_file_size(); ++i) {
        const PbAddedFile& added_file = python_resource.normal_file(i);
        if (added_file.is_executable()) {
            resource->AddFileWithExecutePermission(
                    added_file.path_in_resource(),
                    added_file.file_path());
        } else {
            resource->AddFile(
                    added_file.path_in_resource(),
                    added_file.file_path());
        }
    }

    for (int i = 0; i < python_resource.lib_file_size(); ++i) {
        const PbAddedLibrary& added_lib = python_resource.lib_file(i);
        resource->AddDynamicLibrary(
                added_lib.file_name(),
                added_lib.file_path());
    }

    for (int i = 0; i < python_resource.egg_file_size(); ++i) {
        const PbAddedLibrary& added_egg_file = python_resource.egg_file(i);
        resource->AddPythonLibrary(
                added_egg_file.file_name(),
                added_egg_file.file_path());
    }

    for (int i = 0; i < python_resource.binary_file_size(); ++i) {
        const PbAddedBinary& added_binary_file = python_resource.binary_file(i);
        resource->AddFileFromBytes(
                added_binary_file.file_name(),
                added_binary_file.binary().data(),
                added_binary_file.binary().size());
    }

    if (python_resource.has_cache_file_list()) {
        resource->SetCacheFileList(python_resource.cache_file_list());
    }

    if (python_resource.has_cache_archive_list()) {
        resource->SetCacheArchiveList(python_resource.cache_archive_list());
    }

    backend.SetJobCommitArgs(hadoop_commit_args);
    flume::core::LogicalPlan::Status result = backend.Launch(
            logical_plan,
            resource,
            g_counter_session);

    if (result) {
        LOG(INFO) << "Job ran successfully.";
        return 0;
    } else {
        LOG(ERROR) << "Job ran failed because: " << result.reason;
        LOG(ERROR) << "Details: " << result.detail;
        return 1;
    }
}

int suspend(
        flume::runtime::Backend& backend,
        const flume::PbLogicalPlan& logical_plan,
        const std::vector<std::string>& hadoop_commit_args) {
    flume::runtime::Resource* resource = new flume::runtime::Resource();
    backend.SetJobCommitArgs(hadoop_commit_args);
    flume::core::LogicalPlan::Status result = backend.Suspend(logical_plan, resource);

    if (result) {
        LOG(INFO) << "Job suspend successfully.";
        return 0;
    } else {
        LOG(ERROR) << "Job suspend failed because: " << result.reason;
        LOG(ERROR) << "Details: " << result.detail;
        return 1;
    }
}

int kill(
        flume::runtime::Backend& backend,
        const flume::PbLogicalPlan& logical_plan,
        const std::vector<std::string>& hadoop_commit_args) {
    flume::runtime::Resource* resource = new flume::runtime::Resource();
    backend.SetJobCommitArgs(hadoop_commit_args);
    flume::core::LogicalPlan::Status result = backend.Kill(logical_plan, resource);
    if (result) {
        LOG(INFO) << "Job kill successfully.";
        return 0;
    } else {
        LOG(ERROR) << "Job kill failed because: " << result.reason;
        LOG(ERROR) << "Details: " << result.detail;
        return 1;
    }
}

int get_status(
        flume::runtime::Backend& backend,
        const flume::PbLogicalPlan& logical_plan,
        const std::vector<std::string>& hadoop_commit_args,
        flume::runtime::Backend::AppStatus* status) {
    flume::runtime::Resource* resource = new flume::runtime::Resource();
    backend.SetJobCommitArgs(hadoop_commit_args);
    flume::core::LogicalPlan::Status result = backend.GetStatus(logical_plan, resource, status);
    if (result) {
        LOG(INFO) << "Get job status successfully.";
        return 0;
    } else {
        LOG(ERROR) << "Get job status failed because: " << result.reason;
        LOG(ERROR) << "Details: " << result.detail;
        return 1;
    }
}

int launch() {
    // Initialize backend
    boost::shared_ptr<flume::runtime::Backend> backend;
    if (FLAGS_bigflow_python_local) {
        backend.reset(new flume::runtime::local::LocalBackend());
    }

    // Load LogicalPlan
    flume::PbLogicalPlan logical_plan;
    {
        std::string path(FLAGS_bigflow_python_path + "/logical_plan");
        std::ifstream stream(path.c_str(), std::ios_base::binary | std::ios_base::in);
        CHECK(logical_plan.ParseFromIstream(&stream));
    }
    // Load python resource
    PbPythonResource python_resource;
    {
        std::string path(FLAGS_bigflow_python_path + "/resource");
        std::ifstream stream(path.c_str(), std::ios_base::binary | std::ios_base::in);
        CHECK(python_resource.ParseFromIstream(&stream));
    }
    std::vector<std::string> empty_args;
    return launch(*backend, logical_plan, python_resource, empty_args);
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

