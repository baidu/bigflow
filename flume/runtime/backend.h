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
// Interface for running tasks in different backends.

#ifndef FLUME_RUNTIME_BACKEND_H
#define FLUME_RUNTIME_BACKEND_H

#include <string>
#include <vector>

#include "boost/shared_ptr.hpp"
#include "gflags/gflags.h"
#include "toft/base/class_registry.h"
#include "toft/base/scoped_ptr.h"

#include "flume/proto/config.pb.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/cache_iterator.h"
#include "flume/runtime/counter.h"
#include "flume/runtime/resource.h"
#include "flume/runtime/session.h"

namespace baidu {
namespace flume {
namespace runtime {

// Backend represents execution engines which can execute logical plan. Each implemention
// of Backend has two roles:
//      First, it behaves like a controller/client of an execution engine. Note that even for
//      the same execution engine, there may be many instances of Backend, each should
//      work independently. An particular implemention of Backend may have specialized way
//      to configure and launch a logical plan, however, when an instance of Backend is
//      created by CREATE_BACKEND, it should have the ability to Launch() without any
//      specialized configuration.
//
//      Second, it behaves like an entry to run as worker in backend engine. When an
//      binary run with --flume_execute, the Flume-Framework will create an instance of
//      Backend according to --flume_backend, call its Execute() method.
//
//  In general, user can use LogicalPlan::Run to run in a default or debug manner.
//  However, experienced user may prefer to use different specializd Backend directly.
class Backend {
public:
    struct Status {
        bool ok;
        std::string reason;
        std::string detail;

        Status() {}

        Status(bool ok_,
               const std::string& reason_,
               const std::string& detail_) : ok(ok_), reason(reason_), detail(detail_) {}

        operator bool() const { return ok; }
    };

    enum AppStatus {
        AM_SUBMIT = 0,
        AM_ALLOCATE = 1,
        AM_RUN = 2,
        AM_KILL = 3,
        AM_FAIL = 4,
        AM_UNKNOWN = 5,

        APP_SUBMIT = 100,
        APP_ALLOCATE = 101,
        APP_RUN = 102,
        APP_KILL = 103,
        APP_FAIL = 104,
        APP_UNKNOWN = 105,
    };

    typedef boost::shared_ptr<KVIterator> CacheIteratorPtr;

    static void RunAsWorker();
    static void AbortCurrentJob(const std::string& reason, const std::string& detail);

    // A helper method to get the reserved entry for Flume-Framework.
    static Resource::Entry* GetFlumeEntry(Resource* resource);

    // A helper method to get the executor
    static void GetExecutors(const PbExecutor& root,
                             const PbExecutor_Type& type,
                             std::vector<PbExecutor>* executors);

    // A helper method to commit
    static void CommitOutputs(const std::vector<PbExecutor>& executors);

    static PbScope* GetScope(const std::string& id, PbLogicalPlan* plan);

public:
    Backend();

    explicit Backend(const PbJobConfig& job_config);

    virtual ~Backend() {}

    // Run a logical plan in backend. Take the ownership of resource.
    virtual Status Launch(
            const PbLogicalPlan& plan,
            Resource* resource,
            CounterSession* counters) = 0;

    virtual Status Suspend(const PbLogicalPlan& plan, Resource* resource);

    virtual Status Kill(const PbLogicalPlan& plan, Resource* resource);

    virtual Status GetStatus(const PbLogicalPlan& plan, Resource* resource, AppStatus* status);

    // Execute method should never return.
    virtual void Execute() = 0;

    // Abort the executation of this Backend instance. Maybe called as client or worker.
    virtual void Abort(const std::string& reason, const std::string& detail) = 0;

    bool IsNodeCached(const std::string& identity) const;

    virtual CacheIteratorPtr GetCachedData(const std::string& identity);

    const Session& GetSession() const;

    CacheManager* GetCacheManager();

    virtual void SetJobCommitArgs(const std::vector<std::string>& commit_args){}

    // Generate loader/sinker's split result. Maybe modify the concurrency in logical plan.
    virtual void GenerateSplitInfo(
            const std::string& split_path,
            const PbLogicalPlan& src,
            PbLogicalPlan* dst);

    virtual void GetSplitInfo(
            const std::string& split_path,
            std::map<std::string, std::vector<std::string> >* split_info);

protected:
    virtual CacheManager* CreateCacheManager() = 0;

    // Theses methods are called by Launch
    virtual int32_t RunSplit(const std::string& log_server) { return 0; };
    virtual int32_t RunExecute(const std::string& log_server) { return 0; };
    virtual int32_t RunCommit(const std::string& log_server) { return 0; };

    // These methods are called by Execute
    virtual void Split() {};
    virtual void Commit() {};

protected:
    static Backend* s_worker_instance;
    PbJobConfig m_job_config;
    toft::scoped_ptr<Session> m_session;
    toft::scoped_ptr<CacheManager> m_cache_manager;
};

TOFT_CLASS_REGISTRY_DEFINE(backend_registry, Backend);

#define REGISTER_BACKEND(entry_name, class_name) \
    TOFT_CLASS_REGISTRY_REGISTER_CLASS( \
        ::baidu::flume::runtime::backend_registry, \
        ::baidu::flume::runtime::Backend, \
        entry_name, class_name)

#define CREATE_BACKEND(entry_name) \
    TOFT_CLASS_REGISTRY_CREATE_OBJECT( \
        ::baidu::flume::runtime::backend_registry, \
        entry_name)

// A flag to test if we are in worker mode
DECLARE_bool(flume_execute);

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_BACKEND_H
