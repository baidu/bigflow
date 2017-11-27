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
// Author: Wang Cong <wangcong09@baidu.com>
//
// Run logical plan in Spark.

#ifndef FLUME_RUNTIME_SPARK_SPARK_BACKEND_H
#define FLUME_RUNTIME_SPARK_SPARK_BACKEND_H

#include <map>
#include <string>

#include "toft/base/scoped_ptr.h"
#include "toft/base/scoped_array.h"
#include "toft/system/threading/mutex.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/config.pb.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/resource.h"
#include "flume/runtime/session.h"

namespace re2 {
class RE2;
};  // namespace re2

namespace baidu {
namespace flume {
namespace runtime {

class CounterSession;

namespace spark {

class SparkTask;
class SparkDriver;

class SparkBackend : public Backend {
public:
    explicit SparkBackend(const PbSparkConfig& config);
    virtual ~SparkBackend();

    // Run a logical plan in backend. Take the ownership of resource.
    virtual Status Launch(const PbLogicalPlan& plan,
                          Resource* resource, CounterSession* counters);

    // Execute method should never return.
    virtual void Execute();

    // Abort the executation of this Backend instance. Maybe called as client or worker.
    virtual void Abort(const std::string& reason, const std::string& detail);

    virtual void KillJob(const std::string& reason, const std::string& detail);

    // internal interface, Do Not Use This unless you know exactly what you are doing.
    // If you use this method in client, cache and the newer features may do not work.
    SparkBackend();

    // Maybe move to a Runtime class like DCE backend
    static void LoadJobMessage(PbJob* message);
    static int32_t GetTaskIndex();
    static int64_t GetPartitionNumber();

    // overload function of AddScript,
    // don't need to expand by ctemplate.
    template<size_t N>
    void AddScript(const std::string name, const char (&data)[N], Resource::Entry* entry);

    virtual boost::shared_ptr<KVIterator> GetCachedData(const std::string& identity);

private:
    void common_initialize();
    virtual CacheManager* CreateCacheManager();

private:
    std::string _backend_unique_id;

    toft::Mutex _mutex;
    toft::scoped_ptr<Resource> _resource;

    toft::scoped_array<char> _input_buffer;
    toft::scoped_ptr<SparkTask> _task;

    toft::scoped_ptr<SparkDriver> _driver;

    Status _result;
    toft::scoped_ptr<CounterSession> _counters;

    Resource::Entry* _entry;

    int _launch_id; // increment by one per launch request
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_SPARK_SPARK_BACKEND_H

/** Codes that submit job through JNI **/
// #include "flume/runtime/spark/spark_context.h"
// toft::scoped_ptr<SparkContext> _spark_context;
/**                                  **/
