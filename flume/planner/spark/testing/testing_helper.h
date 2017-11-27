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

//
// Created by zhangyuncong on 2017/9/7.
//

#ifndef BLADE_PLANNER_SPARK_TESTING_HELPER_H
#define BLADE_PLANNER_SPARK_TESTING_HELPER_H

#include <string>

#include "flume/proto/physical_plan.pb.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/testing/plan_desc.h"


namespace baidu {
namespace flume {
namespace planner {
namespace spark {

TagDescRef SparkTaskType(PbSparkTask::Type type);
TagDescRef CacheFrom(const std::string& node_id);
TagDescRef CacheEffectiveKeyNum(const int32_t& effective_key_num);

class PassTest : public flume::planner::PassTest {
protected:
    PlanDesc CacheTaskFrom(const std::string& node_id);
    PlanDesc CacheTaskFrom(const PlanDesc* desc,
                           const PlanDesc*  desc1 = NULL);
};
} // spark
} // planner
} // flume
} // baidu

#endif //BLADE_PLANNER_SPARK_TESTING_HELPER_H
