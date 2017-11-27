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
//   This pass will add four fixed task to root unit by following steps:
//      1. Divide the scope which contains LOAD node to the first task.
//      2. Move other node to the third task.
//      3. Add the second and forth task without any children.
//      4. Copy scopes from first/third to second/forth task.
//      5. Divid unit, by prepared edge, from first/third to second/forth task.
//      6. Remove all units which do not has children, expect TASK unit and above.

#include "flume/planner/monitor/add_fixed_task_unit_pass.h"

#include <vector>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/monitor/monitor_tags.h"
#include "flume/planner/monitor/prepared_node_basic_analysis.h"
#include "flume/planner/plan.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

namespace {

struct CopyTarget {
    CopyTarget() : target(NULL) {}
    Unit* target;
};

Unit* FindTaskByType(Plan* plan, TaskInformation::Type type) {
    Unit* root = plan->Root();
    BOOST_FOREACH(Unit* child, root->children()) {
        CHECK(child->has<TaskInformation>());
        if (child->get<TaskInformation>().type == type) {
            return child;
        }
    }
    CHECK(false);
    return NULL;
}

Unit* FindStreamTaskByPreparedType(Plan* plan, TaskInformation::Type type) {
    TaskInformation::Type stream_type = TaskInformation::CLIENT_STREAM;
    if (type == TaskInformation::WORKER_PREPARED) {
        stream_type = TaskInformation::WORKER_STREAM;
    } else if (type == TaskInformation::CLIENT_PREPARED) {
        stream_type = TaskInformation::CLIENT_STREAM;
    } else {
        CHECK(false);
    }
    return FindTaskByType(plan, stream_type);
}

Unit* AddTaskByType(Plan* plan, TaskInformation::Type type) {
    Unit* task = plan->NewUnit(false);
    task->get<TaskInformation>().type = type;
    task->set_type(Unit::TASK);
    Unit* root = plan->Root();
    plan->AddControl(root, task);
    DrawPlanPass::UpdateLabel(task, "10-task-type", TaskInformation::TypeString(type));
    return task;
}

void AddCopyRelationship(Unit* copy_source, Unit* copy_target) {
    copy_source->get<CopyTarget>().target = copy_target;
    DrawPlanPass::UpdateLabel(copy_target, "09-scope", "is copied");
}

void InitPreparedTask(Plan* plan, TaskInformation::Type prepared_type) {
    Unit* task_prepared = AddTaskByType(plan, prepared_type);
    Unit* task_stream = FindStreamTaskByPreparedType(plan, prepared_type);
    AddCopyRelationship(task_stream, task_prepared);
}

class AddWorkerStreamTaskUnitRule : public RuleDispatcher::Rule {
public:
    AddWorkerStreamTaskUnitRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::LOAD_NODE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        CHECK(unit->task() == NULL);
        Unit* task = AddTaskByType(plan, TaskInformation::WORKER_STREAM);
        Unit* replace_unit = unit;
        Unit* root = plan->Root();
        while (replace_unit->father() != root) {
            replace_unit = replace_unit->father();
            CHECK(replace_unit != NULL);
        }
        plan->RemoveControl(root, replace_unit);
        plan->AddControl(task, replace_unit);
        return true;
    }
};

class AddClientStreamTaskUnitRule : public RuleDispatcher::Rule {
public:
    AddClientStreamTaskUnitRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit == plan->Root();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* task = AddTaskByType(plan, TaskInformation::CLIENT_STREAM);
        std::vector<Unit*> children = unit->children();
        for (size_t i = 0; i < children.size(); ++i) {
            Unit* child = children[i];
            if (child->type() == Unit::TASK) {
                continue;
            }
            plan->RemoveControl(unit, child);
            plan->AddControl(task, child);
        }
        return true;
    }
};

class CopyUnitForPreparedTaskRule : public RuleDispatcher::Rule {
public:
    CopyUnitForPreparedTaskRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return !unit->is_leaf()
                && unit->type() > Unit::TASK;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* target_scope = unit->father()->get<CopyTarget>().target;
        CHECK(target_scope);
        Unit* new_unit = unit->clone();
        if (new_unit->get<PbScope>().type() == PbScope::INPUT) {
            new_unit->get<PbScope>().set_type(PbScope::GROUP);
        }
        plan->AddControl(target_scope, new_unit);
        AddCopyRelationship(unit, new_unit);
        return true;
    }
};

struct TagToSplit {};

class TagAllSplitUnitRule : public RuleDispatcher::Rule {
public:
    TagAllSplitUnitRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->is_leaf();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        BOOST_FOREACH(Unit* need, unit->direct_needs()) {
            if (CheckIsNeedSplit(need, unit)) {
                unit->get<TagToSplit>();
                DrawPlanPass::UpdateLabel(unit, "09-prepared", "TagToSplit");
                break;
            }
        }
        return false;
    }

protected:
    bool CheckIsNeedSplit(Unit* need, Unit* unit) {
        if (need->task() != unit->task()) {
            return false;
        }
        if (need->has<TagToSplit>()) {
            return true;
        }
        if (need->has<PreparedNode>()) {
            const std::set<Unit*>& nodes = need->get<PreparedNode>().prepared_users;
            return nodes.find(unit) != nodes.end();
        }
        return false;
    }
};

class SplitUnitRule : public RuleDispatcher::Rule {
public:
    SplitUnitRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->has<TagToSplit>();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* target_scope = unit->father()->get<CopyTarget>().target;
        plan->RemoveControl(unit->father(), unit);
        plan->AddControl(target_scope, unit);
        return true;
    }
};

class RemoveNoLeafUnitRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return !unit->is_leaf() && unit->type() != Unit::TASK;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        if (unit->children().size() < 1) {
            plan->RemoveControl(unit->father(), unit);
            plan->RemoveUnit(unit);
            return true;
        }
        return false;
    }
};

} // namespace

AddFixedTaskUnitPass::~AddFixedTaskUnitPass() {}

bool AddFixedTaskUnitPass::Run(Plan* plan) {
    Unit* root = plan->Root();
    root->set_type(Unit::JOB);

    DepthFirstDispatcher worker_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    worker_dispatcher.AddRule(new AddWorkerStreamTaskUnitRule());
    bool change = worker_dispatcher.Run(plan);

    DepthFirstDispatcher client_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    client_dispatcher.AddRule(new AddClientStreamTaskUnitRule());
    change |= client_dispatcher.Run(plan);

    InitPreparedTask(plan, TaskInformation::WORKER_PREPARED);
    InitPreparedTask(plan, TaskInformation::CLIENT_PREPARED);

    DepthFirstDispatcher revise_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    revise_dispatcher.AddRule(new CopyUnitForPreparedTaskRule());
    change |= revise_dispatcher.Run(plan);

    TopologicalDispatcher tag_split_dispatcher(TopologicalDispatcher::TOPOLOGICAL_ORDER);
    tag_split_dispatcher.AddRule(new TagAllSplitUnitRule);
    change = tag_split_dispatcher.Run(plan);

    TopologicalDispatcher split_dispatcher(TopologicalDispatcher::TOPOLOGICAL_ORDER);
    split_dispatcher.AddRule(new SplitUnitRule);
    change |= split_dispatcher.Run(plan);

    DepthFirstDispatcher remove_dispatcher(DepthFirstDispatcher::POST_ORDER);
    remove_dispatcher.AddRule(new RemoveNoLeafUnitRule());
    change |= remove_dispatcher.Run(plan);

    return change;
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu

