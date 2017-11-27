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
// Author: Zhou Kai <zhoukai01@baidu.com>

#include "flume/planner/plan.h"

#include <iterator>

#include "boost/graph/topological_sort.hpp"
#include "boost/foreach.hpp"
#include "glog/logging.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

Plan::Plan() {
    m_root = NewUnit("", false);
}

Unit* Plan::Root() const {
    return m_root;
}

Unit* Plan::NewUnit(bool is_leaf) {
    std::string uuid = toft::CreateCanonicalUUIDString();
    return NewUnit(uuid, is_leaf);
}

Unit* Plan::NewUnit(const std::string& id, bool is_leaf) {
    Unit* unit = new Unit(this, is_leaf);
    unit->set_identity(id);
    Vertex vertex = boost::add_vertex(m_dag);
    m_dag[vertex].reset(unit);
    m_vertices[unit] = vertex;
    return unit;
}

void Plan::AddControl(Unit* father, Unit* child) {
    CHECK_NOTNULL(father);
    CHECK_NOTNULL(child);
    CHECK(!father->is_discard());
    CHECK(!child->is_discard());
    father->m_childs.insert(child);
    child->m_father = father;
}

void Plan::RemoveControl(Unit* father, Unit* child) {
    CHECK_NOTNULL(father);
    CHECK_NOTNULL(child);
    CHECK_EQ(father, child->father());
    father->m_childs.erase(child);
    child->m_father = NULL;
}

void Plan::ReplaceControl(Unit* father, Unit* child, Unit* unit) {
    RemoveControl(father, child);
    AddControl(father, unit);
    AddControl(unit, child);
}

void Plan::AddDependency(Unit* source, Unit* target) {
    CHECK_EQ(1u, m_vertices.count(source));
    CHECK_EQ(1u, m_vertices.count(target));
    CHECK(!source->is_discard());
    CHECK(!target->is_discard());
    Vertex source_vertex = m_vertices[source];
    Vertex target_vertex = m_vertices[target];
    boost::add_edge(source_vertex, target_vertex, m_dag);
}

bool Plan::HasDependency(Unit* source, Unit* target) {
    CHECK_EQ(1u, m_vertices.count(source));
    CHECK_EQ(1u, m_vertices.count(target));
    Vertex source_vertex = m_vertices[source];
    Vertex target_vertex = m_vertices[target];
    std::pair<Edge, bool> pair = boost::edge(source_vertex, target_vertex, m_dag);
    return pair.second;
}

void Plan::RemoveDependency(Unit* source, Unit* target) {
    CHECK_EQ(1u, m_vertices.count(source));
    CHECK_EQ(1u, m_vertices.count(target));
    Vertex source_vertex = m_vertices[source];
    Vertex target_vertex = m_vertices[target];
    std::pair<Edge, bool> pair = boost::edge(source_vertex, target_vertex, m_dag);
    CHECK(pair.second)
        << "edge not exist source " << source->identity()
        << ", target " << target->identity();
    boost::remove_edge(pair.first, m_dag);
}

void Plan::ReplaceDependency(Unit* source, Unit* target, Unit* unit) {
    RemoveDependency(source, target);
    AddDependency(source, unit);
    AddDependency(unit, target);
}

void Plan::RemoveUnit(Unit* unit) {
    if (unit->father() != NULL) {
        RemoveControl(unit->father(), unit);
    }
    boost::clear_vertex(m_vertices[unit], m_dag);
    unit->m_is_discard = true;
}

std::vector<Unit*> Plan::GetAllUnits() const {
    std::vector<Unit*> units;
    for (VertexMap::const_iterator it = m_vertices.begin(); it != m_vertices.end(); ++it) {
        Unit* unit = m_dag[it->second].get();
        if (!unit->is_discard()) {
            units.push_back(unit);
        }
    }
    return units;
}

std::vector<Unit*> Plan::GetTopologicalOrder(bool include_control_unit) const {
    std::list<Vertex> topo_order;
    boost::topological_sort(m_dag, std::front_inserter(topo_order));

    std::vector<Unit*> units;
    for (std::list<Vertex>::iterator it = topo_order.begin(); it != topo_order.end(); ++it) {
        Unit* unit = m_dag[*it].get();
        if ((include_control_unit || unit->is_leaf()) && !unit->is_discard()) {
            units.push_back(unit);
        }
    }
    return units;
}

std::string Plan::DebugString() const {
    std::string text;
    AppendToString(Root(), &text, "");
    return text;
}

std::vector<Unit*> Plan::GetDirectUsers(const Unit* unit) const {
    std::vector<Unit*> users;
    Vertex vertex = m_vertices.at(const_cast<Unit*>(unit));
    boost::graph_traits<Dag>::out_edge_iterator ptr, end;
    boost::tie(ptr, end) = boost::out_edges(vertex, m_dag);
    while (ptr != end) {
        Vertex target = boost::target(*ptr, m_dag);
        users.push_back(m_dag[target].get());
        ++ptr;
    }
    return users;
}

std::vector<Unit*> Plan::GetDirectNeeds(const Unit* unit) const {
    std::vector<Unit*> needs;
    Vertex vertex = m_vertices.at(const_cast<Unit*>(unit));
    boost::graph_traits<Dag>::in_edge_iterator ptr, end;
    boost::tie(ptr, end) = boost::in_edges(vertex, m_dag);
    while (ptr != end) {
        Vertex source = boost::source(*ptr, m_dag);
        needs.push_back(m_dag[source].get());
        ++ptr;
    }
    return needs;
}

void Plan::AppendToString(Unit* unit, std::string* text, const std::string& prefix) {
    *text += "\n" + prefix + unit->type_string() + ": " + unit->identity();
    for (Unit::iterator it = unit->begin(); it != unit->end(); ++it) {
        AppendToString(*it, text, prefix + "    ");
    }
}

void Plan::ReplaceFrom(Unit* unit,
                       const std::string& old_id,
                       const std::string& new_id) {
    // Do nothing for cache node.
    if (unit->father() != NULL && unit->father()->type() == Unit::CACHE_WRITER) {
        return;
    }
    CHECK(unit->has<PbLogicalPlanNode>())
            << "Not a logical node, unit type: " << unit->type()
            << ", unit id: " << unit->identity();

    PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
    switch (node.type()) {
        case PbLogicalPlanNode::UNION_NODE: {
            bool found = false;
            for (int i = 0; i < node.union_node().from_size(); ++i) {
                if (node.union_node().from(i) == old_id) {
                    *node.mutable_union_node()->mutable_from(i) = new_id;
                    found = true;
                }
            }
            if (found) return;
            LOG(FATAL) << "old id not found " << old_id << ", unit " << unit->identity();
        }
        case PbLogicalPlanNode::SINK_NODE: {
            CHECK_EQ(old_id, node.sink_node().from());
            *node.mutable_sink_node()->mutable_from() = new_id;
            return;
        }
        case PbLogicalPlanNode::PROCESS_NODE: {
            bool found = false;
            for (int i = 0; i < node.process_node().input_size(); ++i) {
                if (node.process_node().input(i).from() == old_id) {
                    *node.mutable_process_node()->mutable_input(i)->mutable_from() = new_id;
                    found = true;
                }
            }
            if (found) return;
            LOG(FATAL) << "old id not found " << old_id << ", unit " << unit->identity();
        }
        case PbLogicalPlanNode::SHUFFLE_NODE: {
            CHECK_EQ(old_id, node.shuffle_node().from());
            *node.mutable_shuffle_node()->mutable_from() = new_id;
            return;
        }
        default: {
            LOG(FATAL) << "unexpected node type " << node.type();
        }
    }
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
