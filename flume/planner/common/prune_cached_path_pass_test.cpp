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

// Autoher: daiweiwei01@baidu.com
//
#include "flume/planner/common/prune_cached_path_pass.h"

#include "gtest/gtest.h"

#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

class PruneCachedPathPassTest : public PassTest {};

TEST_F(PruneCachedPathPassTest, GlobalCachedNode) {
    PbJobConfig message;
    JobConfig job_config;
    job_config.Assign(&message);

    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = LeafUnit() + PbNodeTag(PbLogicalPlanNode::PROCESS_NODE);
    PlanDesc p2 = ProcessNode();
    
    PlanDesc origin = (PlanRoot()[
        p0, 
        p1 + UnitType(Unit::CACHE_READER),
        p2
    ] +NewTagDesc<JobConfig>(job_config))
    | p0 >> p1 >> p2;
   
    PlanDesc l = LoadNode();
    PlanDesc key_extractor = ProcessNode();

    PlanDesc expected = (PlanRoot()[
        InputScope() [
            l,
            key_extractor
        ],
        p0,
        p1  + UnitType(Unit::PROCESS_NODE),
        p2
    ] +NewTagDesc<JobConfig>(job_config))
    | p0
    | l >> key_extractor >> p1 >> p2;
   
     
    PruneCachedPathPass pass;
    ExpectPass<PruneCachedPathPass>(&pass, origin, expected);
}

TEST_F(PruneCachedPathPassTest, ShuffledCachedNode) {
    PbJobConfig message;
    JobConfig job_config;
    job_config.Assign(&message);

    PlanDesc p0 = ProcessNode();
    PlanDesc sn_0 = ShuffleNode();
    PlanDesc p1 = LeafUnit() + PbNodeTag(PbLogicalPlanNode::PROCESS_NODE);
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = (PlanRoot()[
        p0,
        GroupScope() [
            sn_0,
            p1 + UnitType(Unit::CACHE_READER)
        ],
        p2
    ] +NewTagDesc<JobConfig>(job_config))
    | p0 >> sn_0 >> p1 >> p2;
   
    PlanDesc l = LoadNode();
    PlanDesc key_extractor = ProcessNode();
    PlanDesc sn_1 = ShuffleNode();

    PlanDesc expected = (PlanRoot()[
        InputScope() [
            l,
            key_extractor
        ],
        p0,
        GroupScope() [
            sn_0,
            sn_1,
            p1  + UnitType(Unit::PROCESS_NODE)
        ],
        p2
    ] +NewTagDesc<JobConfig>(job_config))
    | p0 >> sn_0
    | l >> key_extractor >> sn_1 >> p1 >> p2;
   
     
    PruneCachedPathPass pass;
    ExpectPass<PruneCachedPathPass>(&pass, origin, expected);

}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
