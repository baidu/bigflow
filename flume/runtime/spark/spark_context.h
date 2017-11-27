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

#ifndef FLUME_RUNTIME_SPARK_SPARK_CONTEXT_H
#define FLUME_RUNTIME_SPARK_SPARK_CONTEXT_H

#include <string>

#include "toft/base/scoped_ptr.h"

#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {

class PbJobConfig;

namespace runtime {
namespace spark {

class SparkContext {
public:
    SparkContext(const PbJobConfig& config);
    ~SparkContext();

    void submit_job(const PbJob& job);

    bool run_job(const PbJob& job);

    void kill_job();
private:
    class Impl;

private:
    toft::scoped_ptr<Impl> _impl;
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_SPARK_SPARK_CONTEXT_H
