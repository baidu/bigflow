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
// Author: Pan Yuchang(BDG)<bigflow-opensource@baidu.com>
// Description:

#include "boost/foreach.hpp"
#include "gtest/gtest.h"

#include "flume/planner/monitor/build_log_input_pass.h"
#include "flume/planner/monitor/monitor_tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

class BuildLogInputPassTest : public PlanTest {};

TEST_F(BuildLogInputPassTest, NormalTest) {
    AddTasksOf(4);
    Unit* task1 = GetTask(0);
    Unit* task2 = GetTask(1);
    Unit* task3 = GetTask(2);
    Unit* task4 = GetTask(3);

    Unit* default1 = AddNode(task1, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);

    Unit* default2 = AddNode(default1, "default2", Unit::SCOPE);
    Unit* shuffle1 = AddLeaf(default2, "shuffle1", Unit::SHUFFLE_NODE);
    Unit* p_load2 = AddLeaf(default2, "load process2", Unit::PROCESS_NODE);
    Unit* default3 = AddNode(default2, "default3", Unit::SCOPE);
    Unit* shuffle2 = AddLeaf(default3, "shuffle2", Unit::SHUFFLE_NODE);

    Unit* default4 = AddNode(task2, "default4", Unit::SCOPE);
    Unit* default5 = AddNode(default4, "default5", Unit::SCOPE);
    Unit* default6 = AddNode(default5, "default6", Unit::SCOPE);
    Unit* p_load3 = AddLeaf(default6, "load process3", Unit::PROCESS_NODE);

    Unit* p1 = AddLeaf(task3, "process1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(task4, "process2", Unit::PROCESS_NODE);
    Unit* sink = AddLeaf(task4, "sink", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, shuffle1);
    AddDependency(shuffle1, p_load2);
    AddDependency(p_load2, shuffle2);
    AddDependency(shuffle2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p1, p2);
    AddDependency(p2, sink);

    ApplyPass<BuildLogInputPass>();

    EXPECT_EQ(default1->type(), Unit::EXTERNAL_EXECUTOR);
    EXPECT_EQ(default1->get<External>().type, External::LOG_INPUT);
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu
