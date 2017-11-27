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

#include "flume/planner/testing/plan_test_helper.h"

#include "boost/lexical_cast.hpp"

#include "flume/planner/common/tags.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

PlanTest::PlanTest() : m_is_first_draw(true), m_dir_path("./dot/") {
    m_dir_path.append(::testing::UnitTest::GetInstance()->current_test_info()->name());
    m_drawer.reset(new PlanDraw(m_dir_path));
    m_plan.reset(new Plan());
    m_tasks.clear();
    GetPlanRoot()->set_type(Unit::JOB);
    GetPlanRoot()->get<Session>().Assign(&m_session);
}

void PlanTest::InitFromLogicalPlan(core::LogicalPlan* logical_plan) {
    LoadLogicalPlanPass loader;
    loader.Initialize(logical_plan->ToProtoMessage());
    loader.Run(m_plan.get());
}

void PlanTest::AddTasksOf(int32_t task_number) {
    size_t start_index = m_tasks.size();
    m_tasks.resize(m_tasks.size() + task_number);
    Unit* root = m_plan->Root();
    for (size_t i = start_index; i < m_tasks.size(); ++i) {
        std::string task_name = boost::lexical_cast<std::string>(i);
        m_tasks[i] = m_plan->NewUnit(std::string("task").append(task_name), false);
        m_tasks[i]->set_type(Unit::TASK);
        m_plan->AddControl(root, m_tasks[i]);
    }
}

PlanVisitor PlanTest::GetPlanVisitor() {
    return PlanVisitor(m_plan.get());
}

Plan* PlanTest::GetPlan() {
    return m_plan.get();
}

Unit* PlanTest::GetPlanRoot() {
    return GetPlan()->Root();
}

Unit* PlanTest::GetTask(int index) {
    return m_tasks[index];
}

Unit* PlanTest::AddLeafBelowTask(int task_index, std::string leaf_name, Unit::Type type) {
    CHECK_LT(task_index, m_tasks.size());
    return AddLeaf(m_tasks[task_index], leaf_name, type);
}

Unit* PlanTest::AddLeaf(Unit* father, std::string leaf_name, Unit::Type type) {
    return AddNode(father, leaf_name, type, true);
}

Unit* PlanTest::AddNodeBelowTask(int task_index, std::string node_name, Unit::Type type) {
    CHECK_LT(task_index, m_tasks.size());
    return AddNode(m_tasks[task_index], node_name, type);
}

Unit* PlanTest::AddNode(Unit* father, std::string node_name, Unit::Type type) {
    return AddNode(father, node_name, type, false);
}

Unit* PlanTest::AddNodeBelowTask(int task_index, std::string node_name,
                                    Unit::Type type, bool is_leaf) {
    return AddNode(m_tasks[task_index], node_name, type, is_leaf);
}

Unit* PlanTest::AddNode(Unit* father, std::string node_name, Unit::Type type, bool is_leaf) {
    Unit* node = m_plan->NewUnit(node_name, is_leaf);
    node->set_type(type);
    m_plan->AddControl(father, node);
    AddDefaultLogicalPlanNode(node, type);
    return node;
}

void PlanTest::SetProcess(Unit* unit) {
    SetProcess(unit, std::vector<std::string>());
}

void PlanTest::SetProcess(Unit* unit, const std::vector<std::string>& froms) {
    unit->set_type(Unit::PROCESS_NODE);
    unit->get<PbLogicalPlanNode>().set_type(PbLogicalPlanNode::PROCESS_NODE);
    unit->get<PbLogicalPlanNode>().set_id(unit->identity());
    for (size_t i = 0; i < froms.size(); ++i) {
        *(unit->get<PbLogicalPlanNode>().mutable_process_node()->add_input()->mutable_from())
                            = froms[i];
    }
}

void PlanTest::SetShuffle(Unit* unit, std::string from) {
    unit->set_type(Unit::SHUFFLE_NODE);
    unit->get<PbLogicalPlanNode>().set_type(PbLogicalPlanNode::SHUFFLE_NODE);
    if (!from.empty()) {
        *(unit->get<PbLogicalPlanNode>().mutable_shuffle_node()->mutable_from()) = from;
    }
}

void PlanTest::AddInput(Unit* input, Unit* to, const std::string& id, bool is_partial) {
    switch (to->type()) {
    case Unit::PROCESS_NODE: {
        PbProcessNode::Input* in =
                to->get<PbLogicalPlanNode>().mutable_process_node()->add_input();
        *(in->mutable_from()) = id;
        in->set_is_partial(is_partial);
        break;
    }
    case Unit::SHUFFLE_NODE: {
        to->get<PbLogicalPlanNode>().mutable_shuffle_node()->set_from(id);
        break;
    }
    default :
        break;
    }

    AddDependency(input, to);
}

bool PlanTest::IsNodeContain(Unit* father, Unit* node) {
    for (Unit::iterator it = father->begin(); it != father->end(); ++it) {
        if (*it == node) {
            return true;
        }
    }
    return false;
}

std::set<std::string> PlanTest::GetFromIds(Unit* unit, Unit::Type type) {
    std::set<std::string> froms;
    if (type == Unit::PROCESS_NODE) {
        const PbProcessNode& node = unit->get<PbLogicalPlanNode>().process_node();
        for (int i = 0; i < node.input_size(); ++i) {
            froms.insert(node.input(i).from());
        }
    } else if (type == Unit::UNION_NODE) {
        const PbUnionNode& node = unit->get<PbLogicalPlanNode>().union_node();
        for (int i = 0; i < node.from_size(); ++i) {
            froms.insert(node.from(i));
        }
    }
    return froms;
}

std::vector<Unit*> PlanTest::GetTypeNodes(Unit* father, Unit::Type type) {
    std::vector<Unit*> matched_nodes;
    for (Unit::iterator it =  father->begin(); it != father->end(); ++it) {
        if ((*it)->type() == type) {
            matched_nodes.push_back(*it);
        }
    }
    return matched_nodes;
}

void PlanTest::AddDependency(Unit* u1, Unit* u2) {
    m_plan->AddDependency(u1, u2);
    UpdateInputOutput(&u1->get<Info>().outputs, u1, u2);
    UpdateInputOutput(&u2->get<Info>().inputs, u1, u2);
    if (u1->task() != u2->task()) {
        UpdateInputOutput(&u1->task()->get<Info>().outputs, u1, u2);
        UpdateInputOutput(&u1->father()->get<Info>().outputs, u1, u2);
        UpdateInputOutput(&u2->task()->get<Info>().inputs, u1, u2);
        UpdateInputOutput(&u2->father()->get<Info>().inputs, u1, u2);
    }
}

void PlanTest::AddDefaultLogicalPlanNode(Unit* unit, Unit::Type type) {
    static PbEntity entity;
    entity.set_name("test_serde");
    entity.set_config("0");
    PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
    node.set_id(unit->identity());
    node.set_type(PbLogicalPlanNode::UNION_NODE);
    node.set_scope("");
    *(node.mutable_objector()) = entity;

    switch (type) {
    case Unit::PROCESS_NODE: {
        node.set_type(PbLogicalPlanNode::PROCESS_NODE);
        PbProcessNode process_node;
        *process_node.mutable_processor() = entity;
        *node.mutable_process_node() = process_node;
        break;
    }
    case Unit::SHUFFLE_NODE: {
        node.set_type(PbLogicalPlanNode::SHUFFLE_NODE);
        PbShuffleNode shuffle_node;
        shuffle_node.set_from("");
        shuffle_node.set_type(PbShuffleNode::KEY);
        *node.mutable_shuffle_node() = shuffle_node;
        break;
    }
    case Unit::UNION_NODE: {
        PbUnionNode union_node;
        *node.mutable_union_node() = union_node;
        break;
    }
    case Unit::SINK_NODE: {
        node.set_type(PbLogicalPlanNode::SINK_NODE);
        PbSinkNode sink_node;
        sink_node.set_from("");
        *sink_node.mutable_sinker() = entity;
        *node.mutable_sink_node() = sink_node;
        break;
    }
    default:
        break;
    }
}

void PlanTest::UpdateInputOutput(Info::DependencyArray* depends_array, Unit* u1, Unit* u2) {
    for (Info::DependencyArray::iterator it = depends_array->begin();
            it != depends_array->end(); ++it) {
        if (it->first == u1 && it->second == u2) {
            return;
        }
    }
    depends_array->push_back(std::make_pair(u1, u2));
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu


