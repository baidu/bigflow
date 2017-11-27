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
// Common tags shared by all passes of Spark planner

#ifndef FLUME_PLANNER_SPARK_TAGS_H_
#define FLUME_PLANNER_SPARK_TAGS_H_

#include <vector>
#include <string>

#include "flume/planner/common/util.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

// left default value 0 as invalid value
enum ExternalExecutor {
    HADOOP_INPUT    = 1,  // PbSparkTask::PbHadoopInput
    SHUFFLE_INPUT   = 2,  // PbSparkTask::PbShuffleInput
    SHUFFLE_OUTPUT  = 3,  // PbSparkTask::PbShuffleOutput
    CACHE_INPUT     = 4,  // PbSparkTask::PbCacheInput
};

struct IsHadoopInput {};

struct NotUserSetBucketSize {};

struct TaskIndex : public Value<int> {};

struct TaskConcurrency : public Value<int> {};
// set by planner/dce/add_task_unit_pass.h, used by planner/dce/set_default_concurrency_pass.h
struct SinglePoint {};  // indicate that unit is a single point at global scope

struct TaskOffset : public Value<int> {};

struct RddIndex : public Value<int> {};

struct PreTaskInSameRdd : public Pointer<Unit> {};

struct OrderScopes : public std::vector<PbScope> {};

struct NodeIdToCacheTasks : public std::map<std::string, Unit*> {};

struct CacheOrigin : public std::set<Unit*> {};

struct CacheNodeInfo {
    std::string cache_node_id;
    uint32_t key_num; // not include the glboal key
    bool operator==(const CacheNodeInfo& that)const {
        return this->cache_node_id == that.cache_node_id && this->key_num == that.key_num;
    }
};

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_SPARK_TAGS_H_

