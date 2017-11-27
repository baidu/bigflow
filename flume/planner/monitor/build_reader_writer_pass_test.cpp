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

#include "flume/planner/monitor/build_reader_writer_pass.h"
#include "flume/planner/monitor/monitor_tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

typedef PbMonitorTask::MonitorWriter MonitorWriter;
typedef PbMonitorTask::MonitorReader MonitorReader;

class ExternalTypeIsFilter : public filter::IFilter {
public:
    explicit ExternalTypeIsFilter(const External::Type& type) :m_type(type) {}

    bool Filte(Unit* unit) const {
        return unit->type() == Unit::EXTERNAL_EXECUTOR
        && unit->has<External>()
        && unit->get<External>().type == m_type;
    }
private:
    External::Type m_type;
};

FLUME_REGISTER_FILTER(ExternalTypeIs);
FLUME_REGISTER_EQUAL_FILTER(ExternalTypeIs, External::Type, external_type);

class BuildReaderWriterPassTest : public PlanTest {};

TEST_F(BuildReaderWriterPassTest, TwoToOneTest) {
    AddTasksOf(4);
    Unit* task1 = GetTask(0);
    Unit* task2 = GetTask(1);
    Unit* task3 = GetTask(2);
    Unit* task4 = GetTask(3);

    task1->get<TaskInformation>().type = TaskInformation::WORKER_STREAM;
    task2->get<TaskInformation>().type = TaskInformation::WORKER_PREPARED;
    task3->get<TaskInformation>().type = TaskInformation::CLIENT_STREAM;
    task4->get<TaskInformation>().type = TaskInformation::CLIENT_PREPARED;

    Unit* default1 = AddNode(task1, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);

    Unit* default2 = AddNode(default1, "default2", Unit::SCOPE);
    Unit* shuffle1 = AddLeaf(default2, "shuffle1", Unit::SHUFFLE_NODE);
    Unit* p_load2 = AddLeaf(default2, "load process2", Unit::PROCESS_NODE);
    Unit* default3 = AddNode(default2, "default3", Unit::SCOPE);
    Unit* shuffle2 = AddLeaf(default3, "shuffle2", Unit::SHUFFLE_NODE);

    Unit* default4 = AddNode(task2, "default4", Unit::SCOPE);
    Unit* default5 = AddNode(default4, "default5", Unit::SHUFFLE_EXECUTOR);
    Unit* channel1 = AddLeaf(default5, "channel1", Unit::CHANNEL);
    Unit* default6 = AddNode(default5, "default6", Unit::SHUFFLE_EXECUTOR);
    Unit* channel2 = AddLeaf(default6, "channel2", Unit::CHANNEL);
    Unit* p_load3 = AddLeaf(default6, "load process3", Unit::PROCESS_NODE);

    Unit* p1 = AddLeaf(task3, "process_same_id", Unit::PROCESS_NODE);
    Unit* p1_1 = AddLeaf(task3, "process_same_id", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(task4, "process2", Unit::PROCESS_NODE);
    Unit* sink = AddLeaf(task4, "sink", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, shuffle1);
    AddDependency(shuffle1, p_load2);
    AddDependency(p_load2, shuffle2);
    AddDependency(shuffle2, channel1);
    AddDependency(channel1, channel2);
    AddDependency(channel2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p_load3, p1_1);
    AddDependency(p1, p2);
    AddDependency(p1_1, p2);
    AddDependency(p2, sink);

    ApplyPass<BuildReaderWriterPass>();

    PlanVisitor visitor(GetPlan());
    PlanVisitor task1_visitor = visitor[task1];
    EXPECT_TRUE(task1_visitor);
    PlanVisitor task1_writer = task1_visitor.Find(
            external_type == External::MONITOR_WRITER);
    EXPECT_EQ(1u, task1_writer.all_units().size());
    EXPECT_EQ(1u, task1_writer->children().size());
    EXPECT_EQ(0u, task1_visitor.Find(
            external_type == External::MONITOR_READER).all_units().size());

    PlanVisitor task2_visitor = visitor[task2];
    EXPECT_TRUE(task2_visitor);
    EXPECT_EQ(1u, task2_visitor.Find(
            external_type == External::MONITOR_WRITER).all_units().size());
    PlanVisitor task2_reader = task2_visitor.Find(
            external_type == External::MONITOR_READER);
    EXPECT_EQ(1u, task2_reader.all_units().size());
    EXPECT_EQ(1u, task2_reader->children().size());

    PlanVisitor task3_visitor = visitor[task3];
    EXPECT_TRUE(task3_visitor);
    PlanVisitor task3_writer = task3_visitor.Find(
            external_type == External::MONITOR_WRITER);
    EXPECT_EQ(2u, task3_writer.all_units().size());
    int rpc_count = 0;
    BOOST_FOREACH(Unit* wu, task3_writer.all_units()) {
        EXPECT_EQ(1u, wu->children().size());
        MonitorWriter& writer = wu->get<MonitorWriter>();
        if (writer.type() == MonitorWriter::RPC) {
            ++rpc_count;
        }
    }
    EXPECT_EQ(0u, rpc_count);
    EXPECT_EQ(1u, task3_visitor.Find(
            external_type == External::MONITOR_READER).all_units().size());

    PlanVisitor task4_visitor = visitor[task4];
    EXPECT_TRUE(task4_visitor);
    EXPECT_EQ(1u, task4_visitor.Find(
            external_type == External::MONITOR_WRITER).all_units().size());
    PlanVisitor task4_reader = task4_visitor.Find(
            external_type == External::MONITOR_READER);
    EXPECT_EQ(1u, task4_reader.all_units().size());
    EXPECT_EQ(1u, task4_reader->children().size());

    EXPECT_TRUE(sink->is_discard());
}

TEST_F(BuildReaderWriterPassTest, NormalTest) {
    AddTasksOf(4);
    Unit* task1 = GetTask(0);
    Unit* task2 = GetTask(1);
    Unit* task3 = GetTask(2);
    Unit* task4 = GetTask(3);

    task1->get<TaskInformation>().type = TaskInformation::WORKER_STREAM;
    task2->get<TaskInformation>().type = TaskInformation::WORKER_PREPARED;
    task3->get<TaskInformation>().type = TaskInformation::CLIENT_STREAM;
    task4->get<TaskInformation>().type = TaskInformation::CLIENT_PREPARED;

    Unit* default1 = AddNode(task1, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);

    Unit* default2 = AddNode(default1, "default2", Unit::SCOPE);
    Unit* shuffle1 = AddLeaf(default2, "shuffle1", Unit::SHUFFLE_NODE);
    Unit* p_load2 = AddLeaf(default2, "load process2", Unit::PROCESS_NODE);
    Unit* default3 = AddNode(default2, "default3", Unit::SCOPE);
    Unit* shuffle2 = AddLeaf(default3, "shuffle2", Unit::SHUFFLE_NODE);

    Unit* default4 = AddNode(task2, "default4", Unit::SCOPE);
    Unit* p_g2 = AddLeaf(task2, "p_g2", Unit::PROCESS_NODE);
    Unit* default5 = AddNode(default4, "default5", Unit::SHUFFLE_EXECUTOR);
    Unit* channel1 = AddLeaf(default5, "channel1", Unit::CHANNEL);
    Unit* default6 = AddNode(default5, "default6", Unit::SHUFFLE_EXECUTOR);
    Unit* channel2 = AddLeaf(default6, "channel2", Unit::CHANNEL);
    Unit* p_load3 = AddLeaf(default6, "load process3", Unit::PROCESS_NODE);

    Unit* p1 = AddLeaf(task3, "process1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(task4, "process2", Unit::PROCESS_NODE);
    Unit* sink = AddLeaf(task4, "sink", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, shuffle1);
    AddDependency(shuffle1, p_load2);
    AddDependency(p_load2, shuffle2);
    AddDependency(shuffle2, channel1);
    AddDependency(channel1, channel2);
    AddDependency(channel2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p1, p2);
    AddDependency(p2, sink);

    AddDependency(p_load1, p_g2);
    AddDependency(p_g2, p1);

    ApplyPass<BuildReaderWriterPass>();

    PlanVisitor visitor(GetPlan());
    PlanVisitor task1_visitor = visitor[task1];
    EXPECT_TRUE(task1_visitor);
    PlanVisitor task1_writer = task1_visitor.Find(
            external_type == External::MONITOR_WRITER);
    EXPECT_EQ(2u, task1_writer.all_units().size());
    EXPECT_EQ(1u, task1_writer->children().size());
    EXPECT_EQ(0u, task1_visitor.Find(
            external_type == External::MONITOR_READER).all_units().size());

    PlanVisitor task2_visitor = visitor[task2];
    EXPECT_TRUE(task2_visitor);
    EXPECT_EQ(2u, task2_visitor.Find(
            external_type == External::MONITOR_WRITER).all_units().size());
    PlanVisitor task2_reader = task2_visitor.Find(
            external_type == External::MONITOR_READER);
    EXPECT_EQ(1u, task2_reader.all_units().size());
    EXPECT_EQ(2u, task2_reader->children().size());

    EXPECT_TRUE(task2_reader->has<MonitorReader>());
    const MonitorReader& reader = task2_reader->get<MonitorReader>();
    EXPECT_EQ(2, reader.tag_size());
    EXPECT_EQ(2, reader.source_size());

    PlanVisitor task3_visitor = visitor[task3];
    EXPECT_TRUE(task3_visitor);
    PlanVisitor task3_writer = task3_visitor.Find(
            external_type == External::MONITOR_WRITER);
    EXPECT_EQ(1u, task3_writer.all_units().size());
    EXPECT_EQ(1u, task3_writer->children().size());
    EXPECT_EQ(1u, task3_visitor.Find(
            external_type == External::MONITOR_READER).all_units().size());

    PlanVisitor task4_visitor = visitor[task4];
    EXPECT_TRUE(task4_visitor);
    EXPECT_EQ(1u, task4_visitor.Find(
            external_type == External::MONITOR_WRITER).all_units().size());
    PlanVisitor task4_reader = task4_visitor.Find(
            external_type == External::MONITOR_READER);
    EXPECT_EQ(1u, task4_reader.all_units().size());
    EXPECT_EQ(1u, task4_reader->children().size());

    EXPECT_TRUE(sink->is_discard());
}

TEST_F(BuildReaderWriterPassTest, SpecialTest) {
    AddTasksOf(4);
    Unit* task1 = GetTask(0);
    Unit* task2 = GetTask(1);
    Unit* task3 = GetTask(2);
    Unit* task4 = GetTask(3);

    task1->get<TaskInformation>().type = TaskInformation::WORKER_STREAM;
    task2->get<TaskInformation>().type = TaskInformation::WORKER_PREPARED;
    task3->get<TaskInformation>().type = TaskInformation::CLIENT_STREAM;
    task4->get<TaskInformation>().type = TaskInformation::CLIENT_PREPARED;

    Unit* default1 = AddNode(task1, "default1", Unit::SCOPE);
    Unit* load = AddLeaf(default1, "load", Unit::LOAD_NODE);
    Unit* p_load1 = AddLeaf(default1, "load process1", Unit::PROCESS_NODE);

    Unit* default2 = AddNode(default1, "default2", Unit::SCOPE);
    Unit* shuffle1 = AddLeaf(default2, "shuffle1", Unit::SHUFFLE_NODE);
    Unit* p_load2 = AddLeaf(default2, "load process2", Unit::PROCESS_NODE);
    Unit* default3 = AddNode(default2, "default3", Unit::SCOPE);
    Unit* shuffle2 = AddLeaf(default3, "shuffle2", Unit::SHUFFLE_NODE);

    Unit* default4 = AddNode(task2, "default4", Unit::SCOPE);
    Unit* p_t_2 = AddLeaf(task2, "p_t_2", Unit::PROCESS_NODE);
    Unit* default5 = AddNode(default4, "default5", Unit::SHUFFLE_EXECUTOR);
    Unit* channel1 = AddLeaf(default5, "channel1", Unit::CHANNEL);
    Unit* default6 = AddNode(default5, "default6", Unit::SHUFFLE_EXECUTOR);
    Unit* channel2 = AddLeaf(default6, "channel2", Unit::CHANNEL);
    Unit* p_load3 = AddLeaf(default6, "load process3", Unit::PROCESS_NODE);

    Unit* p1 = AddLeaf(task3, "process1", Unit::PROCESS_NODE);
    Unit* p2 = AddLeaf(task4, "process2", Unit::PROCESS_NODE);
    Unit* sink = AddLeaf(task4, "sink", Unit::SINK_NODE);

    AddDependency(load, p_load1);
    AddDependency(p_load1, shuffle1);
    AddDependency(shuffle1, p_load2);
    AddDependency(p_load2, shuffle2);
    AddDependency(shuffle2, channel1);
    AddDependency(channel1, channel2);
    AddDependency(channel2, p_load3);
    AddDependency(p_load3, p1);
    AddDependency(p1, p2);
    AddDependency(p2, sink);

    AddDependency(p_load1, p_t_2);
    AddDependency(p_load1, p1);
    AddDependency(p_load1, p2);
    AddDependency(p_t_2, p2);

    ApplyPass<BuildReaderWriterPass>();

    PlanVisitor visitor(GetPlan());
    PlanVisitor task1_visitor = visitor[task1];
    EXPECT_TRUE(task1_visitor);
    PlanVisitor task1_writer = task1_visitor.Find(
            external_type == External::MONITOR_WRITER);
    EXPECT_EQ(4u, task1_writer.all_units().size());
    int rpc_count = 0;
    BOOST_FOREACH(Unit* wu, task1_writer.all_units()) {
        EXPECT_EQ(1u, wu->children().size());
        MonitorWriter& writer = wu->get<MonitorWriter>();
        if (writer.type() == MonitorWriter::RPC) {
            ++rpc_count;
        }
    }
    EXPECT_EQ(2u, rpc_count);
    EXPECT_EQ(0u, task1_visitor.Find(
            external_type == External::MONITOR_READER).all_units().size());

    PlanVisitor task2_visitor = visitor[task2];
    EXPECT_TRUE(task2_visitor);
    PlanVisitor task2_writer = task2_visitor.Find(
            external_type == External::MONITOR_WRITER);
    EXPECT_EQ(2u, task2_writer.all_units().size());
    rpc_count = 0;
    BOOST_FOREACH(Unit* wu, task2_writer.all_units()) {
        EXPECT_EQ(1u, wu->children().size());
        MonitorWriter& writer = wu->get<MonitorWriter>();
        if (writer.type() == MonitorWriter::RPC) {
            ++rpc_count;
        }
    }
    EXPECT_EQ(2u, rpc_count);
    PlanVisitor task2_reader = task2_visitor.Find(
            external_type == External::MONITOR_READER);
    EXPECT_EQ(1u, task2_reader.all_units().size());
    EXPECT_EQ(2u, task2_reader->children().size());

    PlanVisitor task3_visitor = visitor[task3];
    EXPECT_TRUE(task3_visitor);
    PlanVisitor task3_writer = task3_visitor.Find(
            external_type == External::MONITOR_WRITER);
    EXPECT_EQ(3u, task3_writer.all_units().size());
    rpc_count = 0;
    BOOST_FOREACH(Unit* wu, task3_writer.all_units()) {
        EXPECT_EQ(1u, wu->children().size());
        MonitorWriter& writer = wu->get<MonitorWriter>();
        if (writer.type() == MonitorWriter::RPC) {
            ++rpc_count;
        }
    }
    EXPECT_EQ(0u, rpc_count);
    PlanVisitor task3_reader = task3_visitor.Find(
            external_type == External::MONITOR_READER);
    EXPECT_EQ(1u, task3_reader.all_units().size());
    EXPECT_EQ(4u, task3_reader->children().size());

    PlanVisitor task4_visitor = visitor[task4];
    EXPECT_TRUE(task4_visitor);
    EXPECT_EQ(1u, task4_visitor.Find(
            external_type == External::MONITOR_WRITER).all_units().size());
    PlanVisitor task4_reader = task4_visitor.Find(
            external_type == External::MONITOR_READER);
    EXPECT_EQ(1u, task4_reader.all_units().size());
    EXPECT_EQ(3u, task4_reader->children().size());

    EXPECT_TRUE(sink->is_discard());
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu


