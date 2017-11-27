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

#include "gtest/gtest.h"

#include "flume/planner/monitor/add_fixed_task_unit_pass.h"
#include "flume/planner/monitor/monitor_tags.h"
#include "flume/planner/monitor/prepared_node_basic_analysis.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

class AddFixedTaskUnitPassTest : public PlanTest {};

TEST_F(AddFixedTaskUnitPassTest, RunBasicTest) {
    Unit* root = GetPlanRoot();
    Unit* default1 = AddNode(root, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load = AddLeaf(default1, "load process", Unit::PROCESS_NODE);
    Unit* p1 = AddLeaf(root, "process 1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(root, "process 2", Unit::PROCESS_NODE);
    Unit* default2 = AddNode(root, "default2", Unit::SCOPE);
    Unit* sink1 = AddLeaf(default2, "sink1", Unit::SINK_NODE);
    Unit* p_sink1 = AddLeaf(default2, "sink1 process", Unit::PROCESS_NODE);
    Unit* sink2 = AddLeaf(root, "sink2", Unit::SINK_NODE);
    Unit* sink3 = AddLeaf(root, "sink3", Unit::SINK_NODE);

    AddDependency(load, p_load);
    AddDependency(p_load, p1);
    AddDependency(p_load, p2);
    AddDependency(p1, p_sink1);
    AddDependency(p_sink1, sink1);
    AddDependency(p1, sink2);
    AddDependency(p2, sink3);

    EXPECT_TRUE(default1->task() == NULL);
    EXPECT_TRUE(load->task() == NULL);
    EXPECT_TRUE(p_load->task() == NULL);
    EXPECT_TRUE(p1->task() == NULL);
    EXPECT_TRUE(p2->task() == NULL);
    EXPECT_TRUE(sink1->task() == NULL);
    EXPECT_TRUE(sink2->task() == NULL);
    EXPECT_TRUE(sink3->task() == NULL);
    EXPECT_TRUE(p_sink1->task() == NULL);
    EXPECT_TRUE(default2->task() == NULL);

    ApplyPass<AddFixedTaskUnitPass>();

    EXPECT_EQ(default1->father(), default1->task());
    EXPECT_EQ(default1->task(), load->task());
    EXPECT_EQ(default1->task(), p_load->task());

    EXPECT_EQ(p1->father(), p1->task());
    EXPECT_EQ(p1->task(), p2->task());

    EXPECT_EQ(default2->father(), default2->task());
    EXPECT_EQ(sink1->task(), default2->task());
    EXPECT_EQ(sink2->task(), default2->task());
    EXPECT_EQ(sink3->task(), default2->task());
    EXPECT_EQ(p_sink1->task(), default2->task());

    EXPECT_NE(default1->task(), p1->task());
    EXPECT_NE(default1->task(), p_sink1->task());
    EXPECT_EQ(p1->task(), p_sink1->task());

    EXPECT_EQ(default1->task()->get<TaskInformation>().type, TaskInformation::WORKER_STREAM);
    EXPECT_EQ(p2->task()->get<TaskInformation>().type, TaskInformation::CLIENT_STREAM);
    EXPECT_EQ(root->type(), Unit::JOB);
    EXPECT_EQ(root->children().size(), 4);
}

TEST_F(AddFixedTaskUnitPassTest, NormalTest) {
    Unit* root = GetPlanRoot();
    Unit* default1 = AddNode(root, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);
    Unit* p_load2 = AddLeaf(default1, "load process2", Unit::PROCESS_NODE);
    Unit* p_load3 = AddLeaf(default1, "load process3", Unit::PROCESS_NODE);
    Unit* p1 = AddLeaf(root, "process1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(root, "process2", Unit::PROCESS_NODE);
    Unit* sink = AddLeaf(root, "sink", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, p_load2);
    AddDependency(p_load1, p_load3);
    AddDependency(p_load2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p1, p2);
    AddDependency(p2, sink);

    p_load1->get<PreparedNode>().prepared_users.insert(p_load2);
    p_load1->get<PreparedNode>().prepared_users.insert(p_load3);
    CHECK_EQ(p_load1->get<PreparedNode>().prepared_users.size(), 2);
    p1->get<PreparedNode>().prepared_users.insert(p2);
    CHECK_EQ(p1->get<PreparedNode>().prepared_users.size(), 1);

    default1->get<PbScope>().set_type(PbScope::INPUT);

    ApplyPass<AddFixedTaskUnitPass>();

    EXPECT_EQ(p_load1->task(), load->task());
    EXPECT_EQ(p_load1->father(), load->father());

    EXPECT_NE(p_load1->task(), p_load2->task());

    EXPECT_EQ(p_load2->father()->get<PbScope>().type(), PbScope::GROUP);

    EXPECT_EQ(p_load2->task(), p_load3->task());
    EXPECT_EQ(p_load2->father(), p_load3->father());

    EXPECT_NE(p1->task(), p2->task());

    EXPECT_EQ(p2->task(), sink->task());
    EXPECT_EQ(p2->father(), sink->father());

    EXPECT_EQ(root->size(), 4);
}

TEST_F(AddFixedTaskUnitPassTest, NoPreparedTest) {
    Unit* root = GetPlanRoot();

    Unit* default1 = AddNode(root, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);
    Unit* p_load2 = AddLeaf(default1, "load process2", Unit::PROCESS_NODE);
    Unit* p_load3 = AddLeaf(default1, "load process3", Unit::PROCESS_NODE);
    Unit* p1 = AddLeaf(root, "process1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(root, "process2", Unit::PROCESS_NODE);
    Unit* sink = AddLeaf(root, "sink", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, p_load2);
    AddDependency(p_load1, p_load3);
    AddDependency(p_load2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p1, p2);
    AddDependency(p2, sink);

    ApplyPass<AddFixedTaskUnitPass>();

    EXPECT_EQ(p_load1->task(), load->task());
    EXPECT_EQ(p_load1->father(), load->father());

    EXPECT_EQ(p_load1->task(), p_load2->task());

    EXPECT_EQ(p_load2->task(), p_load3->task());
    EXPECT_EQ(p_load2->father(), p_load3->father());

    EXPECT_EQ(p1->task(), p2->task());

    EXPECT_EQ(p2->task(), sink->task());
    EXPECT_EQ(p2->father(), sink->father());

    EXPECT_EQ(root->size(), 4);
}

TEST_F(AddFixedTaskUnitPassTest, WorkerSplitTest) {
    Unit* root = GetPlanRoot();

    Unit* default1 = AddNode(root, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);
    Unit* p_load2 = AddLeaf(default1, "load process2", Unit::PROCESS_NODE);
    Unit* p_load3 = AddLeaf(default1, "load process3", Unit::PROCESS_NODE);
    Unit* p1 = AddLeaf(root, "process1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(root, "process2", Unit::PROCESS_NODE);
    Unit* sink = AddLeaf(root, "sink", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, p_load2);
    AddDependency(p_load1, p_load3);
    AddDependency(p_load2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p1, p2);
    AddDependency(p2, sink);

    p_load1->get<PreparedNode>().prepared_users.insert(p_load2);
    p_load1->get<PreparedNode>().prepared_users.insert(p_load3);
    CHECK_EQ(p_load1->get<PreparedNode>().prepared_users.size(), 2);

    ApplyPass<AddFixedTaskUnitPass>();

    EXPECT_EQ(p_load1->task(), load->task());
    EXPECT_EQ(p_load1->father(), load->father());

    EXPECT_NE(p_load1->task(), p_load2->task());

    EXPECT_EQ(p_load2->task(), p_load3->task());
    EXPECT_EQ(p_load2->father(), p_load3->father());

    EXPECT_EQ(p1->task(), p2->task());

    EXPECT_EQ(p2->task(), sink->task());
    EXPECT_EQ(p2->father(), sink->father());

    EXPECT_EQ(root->size(), 4);
}


TEST_F(AddFixedTaskUnitPassTest, ClientSplitTest) {
    Unit* root = GetPlanRoot();

    Unit* default1 = AddNode(root, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);
    Unit* p_load2 = AddLeaf(default1, "load process2", Unit::PROCESS_NODE);
    Unit* p_load3 = AddLeaf(default1, "load process3", Unit::PROCESS_NODE);
    Unit* p1 = AddLeaf(root, "process1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(root, "process2", Unit::PROCESS_NODE);
    Unit* sink = AddLeaf(root, "sink", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, p_load2);
    AddDependency(p_load1, p_load3);
    AddDependency(p_load2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p1, p2);
    AddDependency(p2, sink);

    p1->get<PreparedNode>().prepared_users.insert(p2);
    CHECK_EQ(p1->get<PreparedNode>().prepared_users.size(), 1);

    ApplyPass<AddFixedTaskUnitPass>();

    EXPECT_EQ(p_load1->task(), load->task());
    EXPECT_EQ(p_load1->father(), load->father());

    EXPECT_EQ(p_load1->task(), p_load2->task());

    EXPECT_EQ(p_load2->task(), p_load3->task());
    EXPECT_EQ(p_load2->father(), p_load3->father());

    EXPECT_NE(p1->task(), p2->task());

    EXPECT_EQ(p2->task(), sink->task());
    EXPECT_EQ(p2->father(), sink->father());

    EXPECT_EQ(root->size(), 4);
}

TEST_F(AddFixedTaskUnitPassTest, ComplexTest) {
    Unit* root = GetPlanRoot();

    Unit* default1 = AddNode(root, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);
    Unit* default2 = AddNode(default1, "default2", Unit::SCOPE);
    Unit* p_load2 = AddLeaf(default2, "load process2", Unit::PROCESS_NODE);
    Unit* p_load3 = AddLeaf(default1, "load process3", Unit::PROCESS_NODE);

    Unit* shuffle1 = AddNode(default1, "shuffle", Unit::SHUFFLE_EXECUTOR);
    Unit* shuffle_node1 = AddLeaf(shuffle1, "shuffle node", Unit::SHUFFLE_NODE);
    Unit* shuffle2 = AddNode(shuffle1, "shuffle", Unit::SHUFFLE_EXECUTOR);
    Unit* shuffle_node2 = AddLeaf(shuffle2, "shuffle node", Unit::SHUFFLE_NODE);

    Unit* p_shuffle1 = AddLeaf(shuffle2, "shuffle process1", Unit::PROCESS_NODE);
    Unit* p_shuffle2 = AddLeaf(shuffle1, "shuffle process2", Unit::PROCESS_NODE);
    Unit* p_shuffle3 = AddLeaf(default1, "shuffle process3", Unit::PROCESS_NODE);

    Unit* p1 = AddLeaf(root, "process1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(root, "process2", Unit::PROCESS_NODE);
    Unit* sink1 = AddLeaf(root, "sink1", Unit::SINK_NODE);

    Unit* p3 = AddLeaf(root, "process3", Unit::PROCESS_NODE);
    Unit* p4 = AddLeaf(root, "process4", Unit::PROCESS_NODE);
    Unit* sink2 = AddLeaf(root, "sink2", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, p_load2);
    AddDependency(p_load1, p_load3);
    AddDependency(p_load2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p1, p2);
    AddDependency(p2, sink1);

    AddDependency(load, shuffle_node1);
    AddDependency(shuffle_node1, shuffle_node2);
    AddDependency(shuffle_node2, p_shuffle1);
    AddDependency(p_shuffle1, p_shuffle2);
    AddDependency(p_shuffle2, p_shuffle3);
    AddDependency(p_shuffle3, p3);
    AddDependency(p3, p4);
    AddDependency(p4, sink2);

    p_load1->get<PreparedNode>().prepared_users.insert(p_load2);
    p_load1->get<PreparedNode>().prepared_users.insert(p_load3);

    p_shuffle1->get<PreparedNode>().prepared_users.insert(p_shuffle2);

    p1->get<PreparedNode>().prepared_users.insert(p2);

    ApplyPass<AddFixedTaskUnitPass>();

    EXPECT_TRUE(default2->is_discard());

    EXPECT_EQ(p_shuffle2->task(), p_shuffle3->task());
    EXPECT_NE(p_shuffle1->task(), p_shuffle2->task());
    EXPECT_EQ(p_shuffle2->task(), p_load2->task());

    EXPECT_EQ(p1->task(), p4->task());
    EXPECT_EQ(p4->task(), sink2->task());
    EXPECT_EQ(p4->father(), sink2->father());

    EXPECT_EQ(p_load1->task(), load->task());
    EXPECT_EQ(p_load1->father(), load->father());

    EXPECT_NE(p_load1->task(), p_load2->task());

    EXPECT_EQ(p_load2->task(), p_load3->task());
    EXPECT_NE(p_load2->father(), p_load3->father());

    EXPECT_NE(p1->task(), p2->task());

    EXPECT_EQ(p2->task(), sink1->task());
    EXPECT_EQ(p2->father(), sink1->father());

    EXPECT_EQ(root->size(), 4);
}


}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu


