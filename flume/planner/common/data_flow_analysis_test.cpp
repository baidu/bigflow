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
// Author: Zhou Kai <bigflow-opensource@baidu.com>

#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

typedef DataFlowAnalysis::Info Info;

Info::NodeSet AsNodeSet(Unit* n1, Unit* n2 = NULL, Unit* n3 = NULL,
                        Unit* n4 = NULL, Unit* n5 = NULL, Unit* n6 = NULL) {
    Info::NodeSet units;
    Unit* params[] = {n1, n2, n3, n4, n5, n6};
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (params[i] != NULL) {
            units.insert(params[i]);
        }
    }
    return units;
}

Info::NodeSet& Upstreams(Unit* unit) {
    return unit->get<Info>().upstreams;
}

Info::NodeSet& Downstreams(Unit* unit) {
    return unit->get<Info>().downstreams;
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

    Unit* root = plan.Root();
    plan.AddControl(root, s1);
    plan.AddControl(s1, n1);
    plan.AddControl(s1, n2);
    plan.AddControl(root, s2);
    plan.AddControl(s2, n3);
    plan.AddControl(s2, n4);
    plan.AddControl(root, s3);
    plan.AddControl(s3, n5);

    EXPECT_FALSE(DataFlowAnalysis().Run(&plan));

    EXPECT_EQ(0u, Upstreams(n1).size());
    EXPECT_EQ(AsNodeSet(n1), Upstreams(n2));
    EXPECT_EQ(AsNodeSet(n1, n2), Upstreams(n3));
    EXPECT_EQ(AsNodeSet(n1), Upstreams(n4));
    EXPECT_EQ(AsNodeSet(n1, n2, n3, n4), Upstreams(n5));
    EXPECT_EQ(0u, Upstreams(s1).size());
    EXPECT_EQ(AsNodeSet(n1, n2), Upstreams(s2));
    EXPECT_EQ(AsNodeSet(n1, n2, n3, n4), Upstreams(s3));

    EXPECT_EQ(AsNodeSet(n2, n3, n4, n5), Downstreams(n1));
    EXPECT_EQ(AsNodeSet(n3, n5), Downstreams(n2));
    EXPECT_EQ(AsNodeSet(n5), Downstreams(n3));
    EXPECT_EQ(AsNodeSet(n5), Downstreams(n4));
    EXPECT_EQ(0u, Downstreams(n5).size());
    EXPECT_EQ(AsNodeSet(n3, n4, n5), Downstreams(s1));
    EXPECT_EQ(AsNodeSet(n5), Downstreams(s2));
    EXPECT_EQ(0u, Downstreams(s3).size());
}

} // namespace planner
} // namespace flume
} // namespace baidu

