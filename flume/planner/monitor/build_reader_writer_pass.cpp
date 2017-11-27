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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
// Description:
//   This pass should run after AddFixedTaskUnitPass, and is used to add reader/writer unit by
// following steps:
//      1. Add reader unit.
//      2. Add writer unit.
//      3. If there's edge from first/second to forth task, make it through the third task.
//      4. Add tag for reader proxy.
//      5. Add tag for writer proxy.
//      6. Add writer for SINK unit.

#include "flume/planner/monitor/build_reader_writer_pass.h"

#include <vector>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/monitor/monitor_tags.h"
#include "flume/planner/monitor/prepared_node_basic_analysis.h"
#include "flume/planner/plan.h"
#include "flume/planner/task_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

namespace {

typedef PbMonitorTask::MonitorWriter MonitorWriter;
typedef PbMonitorTask::MonitorReader MonitorReader;

bool IsStreamUnit(Unit* unit) {
    Unit* task = unit->task();
    return (task->get<TaskInformation>().type == TaskInformation::WORKER_STREAM
        || task->get<TaskInformation>().type == TaskInformation::CLIENT_STREAM);
}

bool IsWorkerUnit(Unit* unit) {
    Unit* task = unit->task();
    return (task->get<TaskInformation>().type == TaskInformation::WORKER_STREAM
        || task->get<TaskInformation>().type == TaskInformation::WORKER_PREPARED);
}

Unit* NewExternalUnit(Plan* plan, Unit* scope, External::Type type) {
    Unit* external = plan->NewUnit(false);
    plan->AddControl(scope, external);
    external->set_type(Unit::EXTERNAL_EXECUTOR);
    external->get<External>().type = type;
    DrawPlanPass::UpdateLabel(external, "10-external-type", External::TypeString(type));
    return external;
}

Unit* FindReaderInTask(Plan* plan, Unit* task) {
    BOOST_FOREACH(Unit* child, task->children()) {
        if (child->type() == Unit::EXTERNAL_EXECUTOR
            && child->get<External>().type == External::MONITOR_READER) {
            return child;
        }
    }
    return NewExternalUnit(plan, task, External::MONITOR_READER);
}

Unit* FindTaskByType(Plan* plan, TaskInformation::Type type) {
    Unit* root = plan->Root();
    BOOST_FOREACH(Unit* child, root->children()) {
        CHECK(child->has<TaskInformation>());
        if (child->get<TaskInformation>().type == type) {
            return child;
        }
    }
    LOG(FATAL) << "do not find task by type:" << TaskInformation::TypeString(type);
    return NULL;
}

class AddReaderUnitRule : public RuleDispatcher::Rule {
private:
    struct TagInfo {
        std::map<std::string, Unit*> proxys;
        std::map<std::string, std::string> objector_names;
    };

public:
    AddReaderUnitRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return IsExistNeedInDiffTask(unit);
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* task = unit->task();
        Unit* reader = FindReaderInTask(plan, task);

        std::map<std::string, Unit*>& proxys = reader->get<TagInfo>().proxys;
        std::map<std::string, std::string>& objector_names
                    = reader->get<TagInfo>().objector_names;

        bool is_changed = false;

        BOOST_FOREACH(Unit* source, unit->direct_needs()) {
            if (source->task() == task) {
                continue;
            }
            std::string source_id = source->identity();
            CHECK(source->has<PbLogicalPlanNode>());
            std::string objector_name = GetObjectorName(source);
            Unit* proxy = proxys[source_id];
            if (proxy == NULL) {
                proxy = plan->NewUnit(true);
                proxy->set_type(Unit::CHANNEL);
                plan->AddControl(reader, proxy);
                proxys[source_id] = proxy;
                objector_names[source_id] = objector_name;
            }
            plan->ReplaceDependency(source, unit, proxy);
            CHECK(objector_names[source_id] == objector_name);
            is_changed = true;
        }
        return is_changed;
    }

protected:
    std::string GetObjectorName(Unit* unit) {
        return unit->get<PbLogicalPlanNode>().objector().name();
    }

    bool IsExistNeedInDiffTask(Unit* unit) {
        Unit* task = unit->task();
        BOOST_FOREACH(Unit* need, unit->direct_needs()) {
            if (need->task() != task) {
                return true;
            }
        }
        return false;
    }
};

class AddWriterUnitRule : public RuleDispatcher::Rule {
public:
    AddWriterUnitRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return IsExistUserInDiffTask(unit);
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* task = unit->task();
        bool is_changed = false;
        BOOST_FOREACH(Unit* user, unit->direct_users()) {
            if (user->task() == task) {
                continue;
            }
            Unit* writer = NewExternalUnit(plan, unit->father(), External::MONITOR_WRITER);
            Unit* proxy = plan->NewUnit(true);
            proxy->set_type(Unit::CHANNEL);
            plan->ReplaceDependency(unit, user, proxy);
            plan->AddControl(writer, proxy);
            is_changed = true;
        }
        return is_changed;
    }

protected:
    bool IsExistUserInDiffTask(Unit* unit) {
        Unit* task = unit->task();
        BOOST_FOREACH(Unit* user, unit->direct_users()) {
            if (user->task() != task) {
                return true;
            }
        }
        return false;
    }
};

class ReviseReaderWriterUnitRule : public RuleDispatcher::Rule {
public:
    ReviseReaderWriterUnitRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return IsWorkerUnit(unit)
            && unit->direct_users()[0]->task()
                    == FindTaskByType(plan, TaskInformation::CLIENT_PREPARED);
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* source = unit;
        CHECK_EQ(source->type(), Unit::CHANNEL);
        CHECK_EQ(source->father()->type(), Unit::EXTERNAL_EXECUTOR);

        Unit* target = unit->direct_users()[0];

        Unit* task = FindTaskByType(plan, TaskInformation::CLIENT_STREAM);
        std::string source_id = source->identity();

        Unit* writer = NewExternalUnit(plan, task, External::MONITOR_WRITER);
        Unit* writer_proxy = plan->NewUnit(true);
        writer_proxy->set_type(Unit::CHANNEL);

        Unit* reader = FindReaderInTask(plan, task);
        Unit* reader_proxy = plan->NewUnit(true);
        reader_proxy->set_type(Unit::CHANNEL);

        plan->ReplaceDependency(source, target, writer_proxy);
        plan->ReplaceDependency(source, writer_proxy, reader_proxy);
        plan->AddControl(writer, writer_proxy);
        plan->AddControl(reader, reader_proxy);

        return true;
    }
};

struct ProxyTag {
    int value;
};

class AssignTagForReaderRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::EXTERNAL_EXECUTOR
            && unit->get<External>().type == External::MONITOR_READER;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        PbExecutor &executor = unit->get<PbExecutor>();
        executor.set_type(PbExecutor::EXTERNAL);
        executor.mutable_external_executor()->set_id(unit->identity());

        MonitorReader& reader = unit->get<MonitorReader>();
        reader.set_id(unit->identity());
        reader.clear_source();
        reader.clear_tag();

        int tag = 1;
        BOOST_FOREACH(Unit* child, unit->children()) {
            reader.add_source(child->identity());
            reader.add_tag(tag);
            child->get<ProxyTag>().value = tag;
            ++tag;
        }
        return false;
    }
};

class AssignTagForWriterRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::CHANNEL
            && unit->father()->type() == Unit::EXTERNAL_EXECUTOR
            && unit->father()->get<External>().type == External::MONITOR_WRITER;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* father = unit->father();
        PbExecutor &executor = father->get<PbExecutor>();
        executor.set_type(PbExecutor::EXTERNAL);
        executor.mutable_external_executor()->set_id(unit->identity());

        MonitorWriter& writer = father->get<MonitorWriter>();
        writer.set_id(unit->identity());

        CHECK_EQ(unit->direct_users().size(), 1);
        Unit* user = unit->direct_users().front();

        writer.set_tag(user->get<ProxyTag>().value);
        if (IsStreamUnit(unit) && !IsStreamUnit(user)) {
            writer.set_type(MonitorWriter::LOCAL);
            DrawPlanPass::UpdateLabel(unit, "10-writer-type", "Local Writer");
        } else {
            writer.set_type(MonitorWriter::RPC);
            DrawPlanPass::UpdateLabel(unit, "10-writer-type", "Rpc Writer");
        }
        return true;
    }
};

class AddFirstReaderAndInfoRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::LOAD_NODE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* reader = NewExternalUnit(plan, unit->task(), External::MONITOR_READER);
        Unit* proxy = plan->NewUnit(true);
        proxy->set_type(Unit::CHANNEL);
        plan->AddControl(reader, proxy);
        plan->AddDependency(proxy, unit);

        PbExecutor &executor = reader->get<PbExecutor>();
        executor.set_type(PbExecutor::EXTERNAL);
        executor.mutable_external_executor()->set_id(reader->identity());

        std::string proxy_id = proxy->identity();

        MonitorReader& reader_pb = reader->get<MonitorReader>();
        reader_pb.set_id(reader->identity());
        reader_pb.add_tag(0);
        reader_pb.add_source(proxy_id);

        return true;
    }
};

class AddLastWriterAndInfoRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::SINK_NODE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        CHECK_EQ(unit->direct_needs().size(), 1);
        Unit* writer = NewExternalUnit(plan, unit->father(), External::MONITOR_WRITER);
        Unit* writer_proxy = plan->NewUnit(true);
        writer_proxy->set_type(Unit::CHANNEL);
        plan->AddControl(writer, writer_proxy);
        plan->AddDependency(unit->direct_needs().front(), writer_proxy);

        PbExecutor &executor = writer->get<PbExecutor>();
        executor.set_type(PbExecutor::EXTERNAL);
        executor.mutable_external_executor()->set_id(writer->identity());
        MonitorWriter& writer_pb = writer->get<MonitorWriter>();
        writer_pb.set_id(writer->identity());

        CHECK(unit->has<PbLogicalPlanNode>());
        PbLogicalPlanNode& logical_node = unit->get<PbLogicalPlanNode>();
        CHECK(logical_node.has_sink_node());
        CHECK(logical_node.sink_node().sinker().has_config());

        writer_pb.set_tag(
            boost::lexical_cast<int16_t>(logical_node.sink_node().sinker().config()));
        writer_pb.set_type(MonitorWriter::RPC);
        DrawPlanPass::UpdateLabel(writer, "10-writer-type", "Rpc Writer");

        plan->RemoveUnit(unit);

        return true;
    }
};

} // namespace

BuildReaderWriterPass::~BuildReaderWriterPass() {}

bool BuildReaderWriterPass::Run(Plan* plan) {
    TopologicalDispatcher reader_dispatcher(TopologicalDispatcher::TOPOLOGICAL_ORDER);
    reader_dispatcher.AddRule(new AddReaderUnitRule());
    bool change = reader_dispatcher.Run(plan);

    TopologicalDispatcher writer_dispatcher(TopologicalDispatcher::TOPOLOGICAL_ORDER);
    writer_dispatcher.AddRule(new AddWriterUnitRule());
    change |= writer_dispatcher.Run(plan);

    TopologicalDispatcher revise_dispatcher(TopologicalDispatcher::TOPOLOGICAL_ORDER);
    revise_dispatcher.AddRule(new ReviseReaderWriterUnitRule());
    change |= revise_dispatcher.Run(plan);

    DepthFirstDispatcher fresh_reader_info_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    fresh_reader_info_dispatcher.AddRule(new AssignTagForReaderRule());
    change |= fresh_reader_info_dispatcher.Run(plan);

    DepthFirstDispatcher fresh_writer_info_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    fresh_writer_info_dispatcher.AddRule(new AssignTagForWriterRule());
    change |= fresh_writer_info_dispatcher.Run(plan);

/*
    DepthFirstDispatcher first_reader_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    first_reader_dispatcher.AddRule(new AddFirstReaderAndInfoRule());
    change |= first_reader_dispatcher.Run(plan);
*/

    DepthFirstDispatcher last_writer_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    last_writer_dispatcher.AddRule(new AddLastWriterAndInfoRule());
    change |= last_writer_dispatcher.Run(plan);

    return change;
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu

