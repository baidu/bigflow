/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
// Author: Pan Yuchang<panyuchang@baidu.com>

#include "flume/planner/common/split_union_unit_pass.h"

#include "boost/foreach.hpp"

#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

class SplitUnionUnitPassRule : public RuleDispatcher::Rule {
public:
    virtual ~SplitUnionUnitPassRule();

    virtual bool Accept(Plan* plan, Unit* unit);

    virtual bool Run(Plan* plan, Unit* unit);

private:
    // Get id-unit map of direct_needs from unit, it can make it
    // easy to find unit by its id, or from
    std::map<std::string, Unit*> GetNeedsMapFrom(Unit* unit);

    bool AddUnionUnitForUnionNode(Plan* plan, Unit* unit);

    bool AddUnionUnitForProcessNode(Plan* plan, Unit* unit);

    // Add union unit, and init union pb information.
    Unit* AddUnionUnit(Plan* plan, Unit* from_unit);
};

SplitUnionUnitPassRule::~SplitUnionUnitPassRule() {}

bool SplitUnionUnitPassRule::Accept(Plan* plan, Unit* unit) {
    // The added union node has the tag of <MustKeep>, which mean should not be delete
    // in the RemoveUselessUnionPass
    if (unit->has<MustKeep>()) {
        return false;
    }

    return unit->type() == Unit::UNION_NODE || unit->type() == Unit::PROCESS_NODE;
}

bool SplitUnionUnitPassRule::Run(Plan* plan, Unit* unit) {
    CHECK(unit->has<PbLogicalPlanNode>());
    bool is_changed = false;
    if (unit->type() == Unit::PROCESS_NODE) {
        is_changed = AddUnionUnitForProcessNode(plan, unit);
    } else if (unit->type() == Unit::UNION_NODE) {
        is_changed = AddUnionUnitForUnionNode(plan, unit);
    }
    return is_changed;
}

std::map<std::string, Unit*> SplitUnionUnitPassRule::GetNeedsMapFrom(Unit* unit) {
    std::map<std::string, Unit*> from_unit_map;
    BOOST_FOREACH(Unit* need, unit->direct_needs()) {
        from_unit_map[need->identity()] = need;
    }
    return from_unit_map;
}

bool SplitUnionUnitPassRule::AddUnionUnitForUnionNode(Plan* plan, Unit* unit) {
    PbUnionNode* union_node = unit->get<PbLogicalPlanNode>().mutable_union_node();
    std::map<std::string, Unit*> needs_map = GetNeedsMapFrom(unit);
    std::set<std::string> froms;
    bool is_changed = false;
    for (int i = 0; i < union_node->from_size(); ++i) {
        std::string* from = union_node->mutable_from(i);
        if (froms.find(*from) != froms.end()) {
            Unit* union_unit = AddUnionUnit(plan, needs_map[*from]);
            plan->AddDependency(union_unit, unit);
            // change from information
            *from = union_unit->identity();
            is_changed = true;
        }
        froms.insert(*from);
    }
    return is_changed;
}

bool SplitUnionUnitPassRule::AddUnionUnitForProcessNode(Plan* plan, Unit* unit) {
    PbProcessNode* process_node = unit->get<PbLogicalPlanNode>().mutable_process_node();
    std::map<std::string, Unit*> needs_map = GetNeedsMapFrom(unit);
    std::set<std::string> froms;
    bool is_changed = false;
    for (int i = 0; i < process_node->input_size(); ++i) {
        std::string* from = process_node->mutable_input(i)->mutable_from();
        if (froms.find(*from) != froms.end()) {
            Unit* union_unit = AddUnionUnit(plan, needs_map[*from]);
            plan->AddDependency(union_unit, unit);
            // change from information
            *from = union_unit->identity();
            is_changed = true;
        }
        froms.insert(*from);
    }
    return is_changed;
}

Unit* SplitUnionUnitPassRule::AddUnionUnit(Plan* plan, Unit* from_unit) {
    CHECK_NOTNULL(from_unit);
    Unit* union_unit = plan->NewUnit(/* is_leaf = */true);
    union_unit->set_type(Unit::UNION_NODE);
    union_unit->set<MustKeep>();

    plan->AddControl(from_unit->father(), union_unit);
    plan->AddDependency(from_unit, union_unit);

    // init pb information from from_unit
    PbLogicalPlanNode& pb_node = union_unit->get<PbLogicalPlanNode>();
    pb_node.set_id(union_unit->identity());
    pb_node.set_type(PbLogicalPlanNode::UNION_NODE);
    pb_node.set_scope(from_unit->get<PbLogicalPlanNode>().scope());
    *pb_node.mutable_objector() = from_unit->get<PbLogicalPlanNode>().objector();
    PbUnionNode* pb_union_node = pb_node.mutable_union_node();
    pb_union_node->add_from(from_unit->identity());

    return union_unit;
}

} // namespace

SplitUnionUnitPass::~SplitUnionUnitPass() {}

bool SplitUnionUnitPass::Run(Plan* plan) {
    TopologicalDispatcher topo_dispatcher(false);
    topo_dispatcher.AddRule(new SplitUnionUnitPassRule());
    return topo_dispatcher.Run(plan);
}


}  // namespace planner
}  // namespace flume
}  // namespace baidu
