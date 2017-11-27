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
// Author: Pan Yuchang <panyuchang@baidu.com>
//

#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "flume/planner/common/task_flow_analysis.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

typedef DataFlowAnalysis::Info DataInfo;
typedef TaskFlowAnalysis::Info TaskInfo;

TaskInfo::NodeSet AsTaskNodeSet(Unit* n1, Unit* n2 = NULL, Unit* n3 = NULL,
                        Unit* n4 = NULL, Unit* n5 = NULL, Unit* n6 = NULL) {
    TaskInfo::NodeSet units;
    Unit* params[] = {n1, n2, n3, n4, n5, n6};
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (params[i] != NULL) {
            units.insert(params[i]);
        }
    }
    return units;
}
DataInfo::NodeSet AsDataNodeSet(Unit* n1, Unit* n2 = NULL, Unit* n3 = NULL,
                        Unit* n4 = NULL, Unit* n5 = NULL, Unit* n6 = NULL) {
    DataInfo::NodeSet units;
    Unit* params[] = {n1, n2, n3, n4, n5, n6};
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (params[i] != NULL) {
            units.insert(params[i]);
        }
    }
    return units;
}

DataInfo::NodeSet& Upstreams(Unit* unit) {
    return unit->get<DataInfo>().upstreams;
}

DataInfo::NodeSet& Downstreams(Unit* unit) {
    return unit->get<DataInfo>().downstreams;
}

TaskInfo::NodeSet& InputTasks(Unit* unit) {
    return unit->get<TaskInfo>().input_tasks;
}

TaskInfo::NodeSet& OutputTasks(Unit* unit) {
    return unit->get<TaskInfo>().output_tasks;
}

TEST(DataFlowAnalysisTest, UpstreamDownstream) {
    Plan plan;
    Unit* n1 = plan.NewUnit("n1", true);
    Unit* n2 = plan.NewUnit("n2", true);
    Unit* n3 = plan.NewUnit("n3", true);
    Unit* n4 = plan.NewUnit("n4", true);
    Unit* n5 = plan.NewUnit("n5", true);

    plan.AddDependency(n1, n2);
    plan.AddDependency(n2, n3);
    plan.AddDependency(n1, n4);
    plan.AddDependency(n3, n5);
    plan.AddDependency(n4, n5);

    Unit* s1 = plan.NewUnit("s1", false);
    Unit* s2 = plan.NewUnit("s2", false);
    Unit* s3 = plan.NewUnit("s3", false);
    s1->set_type(Unit::TASK);
    s2->set_type(Unit::TASK);
    s3->set_type(Unit::TASK);

    Unit* root = plan.Root();
    plan.AddControl(root, s1);
    plan.AddControl(s1, n1);
    plan.AddControl(s1, n2);
    plan.AddControl(root, s2);
    plan.AddControl(s2, n3);
    plan.AddControl(s2, n4);
    plan.AddControl(root, s3);
    plan.AddControl(s3, n5);

    DataFlowAnalysis().Run(&plan);
    EXPECT_FALSE(TaskFlowAnalysis().Run(&plan));

    EXPECT_EQ(0u, Upstreams(n1).size());
    EXPECT_EQ(AsDataNodeSet(n1), Upstreams(n2));
    EXPECT_EQ(AsDataNodeSet(n1, n2), Upstreams(n3));
    EXPECT_EQ(AsDataNodeSet(n1), Upstreams(n4));
    EXPECT_EQ(AsDataNodeSet(n1, n2, n3, n4), Upstreams(n5));
    EXPECT_EQ(0u, Upstreams(s1).size());
    EXPECT_EQ(AsDataNodeSet(n1, n2), Upstreams(s2));
    EXPECT_EQ(AsDataNodeSet(n1, n2, n3, n4), Upstreams(s3));

    EXPECT_EQ(AsDataNodeSet(n2, n3, n4, n5), Downstreams(n1));
    EXPECT_EQ(AsDataNodeSet(n3, n5), Downstreams(n2));
    EXPECT_EQ(AsDataNodeSet(n5), Downstreams(n3));
    EXPECT_EQ(AsDataNodeSet(n5), Downstreams(n4));
    EXPECT_EQ(0u, Downstreams(n5).size());
    EXPECT_EQ(AsDataNodeSet(n3, n4, n5), Downstreams(s1));
    EXPECT_EQ(AsDataNodeSet(n5), Downstreams(s2));
    EXPECT_EQ(0u, Downstreams(s3).size());

    EXPECT_EQ(0u, InputTasks(s1).size());
    EXPECT_EQ(AsTaskNodeSet(s1), InputTasks(s2));
    EXPECT_EQ(AsTaskNodeSet(s2), InputTasks(s3));
    EXPECT_EQ(AsTaskNodeSet(s2), OutputTasks(s1));
    EXPECT_EQ(AsTaskNodeSet(s3), OutputTasks(s2));
    EXPECT_EQ(0u, OutputTasks(s3).size());
}

} // namespace planner
} // namespace flume
} // namespace baidu

