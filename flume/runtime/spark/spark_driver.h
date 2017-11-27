/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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
// Author: Ye, Xianjin(bigflow-opensource@baidu.com)
//         Zhang, Yuncong (bigflow-opensource@baidu.com)
//

#ifndef FLUME_RUNTIME_SPARK_SPARK_DRIVER_H
#define FLUME_RUNTIME_SPARK_SPARK_DRIVER_H

#include <string>
#include <vector>

#include "boost/shared_ptr.hpp"

#include "toft/base/scoped_ptr.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/kv_iterator.h"

namespace baidu {
namespace flume {

class PbJobConfig;

namespace runtime {

class KVIterator;

namespace spark {

class SparkDriver {
public:
    static const char* kSparkJarName; // = spark_launcher.jar
    static const char* kLog4jConfiguration; // = !/com/baidu/flume/runtime/spark/log4j-defaults.properties

public:
    SparkDriver(const PbJobConfig& job_config, const std::string& resource_path);

    ~SparkDriver();

    bool start();

    bool run_job(const PbJob job);

    boost::shared_ptr<KVIterator> get_cache_data(const std::string& node_id);

    void stop();

private:
    class Impl;

private:
    toft::scoped_ptr<Impl> _impl;
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif //FLUME_RUNTIME_SPARK_SPARK_DRIVER_H
