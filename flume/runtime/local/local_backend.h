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
// Author: Wen Xiang <wenxiang@baidu.com>
//
// Run logical plan in local machine.

#ifndef FLUME_RUNTIME_LOCAL_LOCAL_BACKEND_H
#define FLUME_RUNTIME_LOCAL_LOCAL_BACKEND_H

#include <map>
#include <string>

#include "toft/system/threading/mutex.h"
#include "toft/system/process/sub_process.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/config.pb.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/resource.h"
#include "flume/runtime/session.h"

namespace ctemplate {
class TemplateDictionary;
}  // namespace ctemplate

namespace baidu {
namespace flume {

namespace runtime {

class CounterSession;

namespace local {

class LocalBackend : public Backend {
public:
    explicit LocalBackend(const PbLocalConfig& config);
    virtual ~LocalBackend();

    // Run a logical plan in backend. Take the ownership of resource.
    virtual Status Launch(
            const PbLogicalPlan& plan,
            Resource* resource,
            CounterSession* counters);

    // Execute method should never return.
    virtual void Execute();

    // Abort the executation of this Backend instance. Maybe called as client or worker.
    virtual void Abort(const std::string& reason, const std::string& detail);

    // internal interface, Do Not Use This unless you know exactly what you are doing.
    // If you use this method in client, cache and the newer features may do not work.
    LocalBackend();

protected:
    virtual CacheManager* CreateCacheManager();

    // Theses methods are called by Launch
    virtual int32_t RunExecute();
    virtual int32_t RunCommit();

    // These methods are called by Execute
    virtual void Commit();

private:
    void KillJob(const std::string& reason, const std::string& detail);
    void UpdateCounter(const std::map<std::string, uint64_t>& counters);

    void SetLibraryPath(toft::SubProcess::CreateOptions* options);

    void RenderScripts();

    template<size_t N>
    void AddScript(
            const ctemplate::TemplateDictionary& dict,
            const std::string& name,
            const char (&data)[N]);

private:
    std::string m_backend_unique_id;
    toft::Mutex m_mutex;
    toft::scoped_ptr<toft::SubProcess> m_process;
    toft::scoped_ptr<Resource> m_resource;

    Status m_result;
    toft::scoped_ptr<CounterSession> m_counters;

    Resource::Entry* m_entry;
};

}  // namespace local
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_LOCAL_LOCAL_BACKEND_H
