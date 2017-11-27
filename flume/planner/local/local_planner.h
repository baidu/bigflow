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
//
// Author: Wen Xiang <wenxiang@baidu.com>
// Modified: daiweiwei01@baidu.com
//
// Planner for execution in localhost only.

#ifndef FLUME_PLANNER_LOCAL_LOCAL_PLANNER_H_
#define FLUME_PLANNER_LOCAL_LOCAL_PLANNER_H_

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/proto/config.pb.h"
#include "flume/runtime/session.h"
#include "flume/runtime/resource.h"

namespace baidu {
namespace flume {
namespace planner {
namespace local {
class LocalPlanner {
public:
    LocalPlanner() : m_session(NULL), m_job_config(NULL), m_entry(NULL) {} // compatibe with wing's RunLocally

    LocalPlanner(runtime::Session *session, PbJobConfig *job_config);

    PbPhysicalPlan Plan(const PbLogicalPlan& message);

    void SetDebugDirectory(baidu::flume::runtime::Resource::Entry*);
private:
    runtime::Session *m_session;
    PbJobConfig *m_job_config;
    runtime::Resource::Entry* m_entry;
};

}  // namespace local
}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_LOCAL_LOCAL_PLANNER_H_
