/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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
// Author: Pan Yuchang (BDG), bigflow-opensource@baidu.com
// Last Modified: 08/07/14 16:02:41
// Description: This class is used to transform a Plan to a dot file. Usually for local debug,
// you'd better not git commit code which contain LocalDrawPlanPass, cause this class will contain
// local path.
//
// Here's a simple usage:
//
//      TEST(DrawPlanPassTest, BasicTest) {
//          PlanBuilder builder;
//          LogicalPlan::Node* node_1 =
//              builder->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
//          LogicalPlan::Node* node_2 =
//              builder->Sink(builder->global_scope(), node_1)->By<MockSinker>();
//          ASSERT_TRUE(node_2 != NULL);
//          PlanDraw drawer("/home/users/panyuchang/output/basic");
//          EXPECT_FALSE(drawer.Draw(builder.Build()));
//      }
//
// drawer.Run can be called multiple time in one scope.
//
// The result dot file will be writen to the directory:
//     /home/users/panyuchang/output/basic/
// just like follows:
//     /home/users/panyuchang/output/basic/000.dot
//     /home/users/panyuchang/output/basic/001.dot
//     ......
//


#ifndef FLUME_PLANNER_TESTING_PLAN_DRAW_HELPER_H
#define FLUME_PLANNER_TESTING_PLAN_DRAW_HELPER_H

#include <string>

#include "flume/planner/common/draw_plan_pass.h"

namespace baidu {
namespace flume {
namespace planner {

class PlanDraw {
public:
    explicit PlanDraw(const std::string& dot_file_dir);
    virtual ~PlanDraw();
    virtual bool Draw(Plan* plan);
    bool InitFileDir();
    void WriteToFile(const std::string& description);

private:
    int m_round_count;
    bool m_is_valid;
    std::string m_output_dir;
    DrawPlanPass m_drawer;
};

} // namespace planner
} // namespace flume
} // namespace baidu

#endif  // FLUME_PLANNER_TESTING_PLAN_DRAW_HELPER_H
