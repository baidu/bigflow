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
//
// Interface for running Bigflow Python API in different backends.

#ifndef BIGFLOW_PYTHON_PYTHON_CLIENT
#define BIGFLOW_PYTHON_PYTHON_CLIENT

#include <vector>
#include <string>

#include "flume/runtime/backend.h"
#include "flume/runtime/counter.h"

namespace baidu {
namespace flume {
class PbLogicalPlan;

namespace runtime {
class Backend;
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

namespace baidu {
namespace bigflow {
namespace python {

extern flume::runtime::CounterSession* g_counter_session;

class PbPythonResource;

int launch();

int launch(
        flume::runtime::Backend& backend,
        const flume::PbLogicalPlan& logical_plan,
        const PbPythonResource& python_resource,
        const std::vector<std::string>& hadoop_commit_args);

int suspend(
        flume::runtime::Backend& backend,
        const flume::PbLogicalPlan& logical_plan,
        const std::vector<std::string>& hadoop_commit_args);

int kill(
        flume::runtime::Backend& backend,
        const flume::PbLogicalPlan& logical_plan,
        const std::vector<std::string>& hadoop_commit_args);

enum AppStatus {
};

int get_status(
        flume::runtime::Backend& backend,
        const flume::PbLogicalPlan& logical_plan,
        const std::vector<std::string>& hadoop_commit_args,
        flume::runtime::Backend::AppStatus* status);

void register_classes();

}  // namespace baidu
}  // namespace bigflow
}  // namespace python

#endif  // BIGFLOW_PYTHON_PYTHON_CLIENT

