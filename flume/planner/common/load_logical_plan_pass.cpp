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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/planner/common/load_logical_plan_pass.h"

#include <map>

#include "boost/graph/adjacency_list.hpp"
#include "boost/graph/topological_sort.hpp"
#include "glog/logging.h"
#include "toft/base/unordered_map.h"
#include "toft/base/scoped_ptr.h"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/graph_helper.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"
#include "flume/core/empty_environment.h"
#include "flume/core/environment.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

using flume::core::EmptyEnvironment;

class LoadLogicalPlanPassImpl {
public:
    LoadLogicalPlanPassImpl(const PbLogicalPlan& message, Plan* plan)
        : m_message(message), m_plan(plan) {
        m_units[""] = plan->Root();
        if (message.has_environment()) {
            m_env.reset(NULL);
            plan->Root()->get<Environment>().Assign(const_cast<PbEntity*>(&message.environment()));
        } else {
            m_env.reset(new PbEntity());
            m_env->set_name(Reflection<flume::core::Environment>::TypeName<EmptyEnvironment>());
            plan->Root()->get<Environment>().Assign(const_cast<PbEntity*>(m_env.get()));
        }
    }

    // ~LoadLogicalPlanPassImpl() {
    //     if (m_env) {
    //         delete m_env;
    //     }
    // }

    void Run() {
        BuildUnitForScopes();
        BuildUnitForNodes();
        CheckInitialized();
    }

private:
    void BuildUnitForScopes() {
        typedef boost::adjacency_list<
            boost::listS, boost::vecS, boost::directedS, PbScope> ScopeTree;
        typedef boost::graph_traits<ScopeTree>::vertex_descriptor ScopeVertex;
        typedef std::map<std::string, ScopeVertex> ScopeMap;

        ScopeTree tree;
        ScopeMap scopes;
        BuildScopeTree(m_message, &tree, &scopes);

        for (ScopeMap::iterator i = scopes.begin(); i != scopes.end(); ++i) {
            ScopeVertex v = i->second;
            const PbScope& scope = tree[v];

            Unit* unit = ControlUnit(scope.id());
            unit->set_type(Unit::SCOPE);
            unit->get<PbScope>() = scope;
            if (scope.is_infinite()) {
                unit->set<IsInfinite>();
            }

            boost::graph_traits<ScopeTree>::adjacency_iterator ptr, end;
            boost::tie(ptr, end) = boost::adjacent_vertices(v, tree);
            while (ptr != end) {
                const PbScope& sub_scope = tree[*ptr];
                m_plan->AddControl(unit, ControlUnit(sub_scope.id()));

                ++ptr;
            }
        }
    }

    void BuildUnitForNodes() {
        typedef boost::adjacency_list<
            boost::listS, boost::vecS, boost::bidirectionalS, PbLogicalPlanNode> NodeDag;
        typedef boost::graph_traits<NodeDag>::vertex_descriptor NodeVertex;
        typedef std::map<std::string, NodeVertex> NodeMap;

        NodeDag dag;
        NodeMap nodes;
        BuildLogicalDag(m_message, &dag, &nodes);

        for (NodeMap::iterator i = nodes.begin(); i != nodes.end(); ++i) {
            NodeVertex v = i->second;
            const PbLogicalPlanNode& node = dag[v];

            static const Unit::Type kTypes[] = {
                Unit::UNION_NODE,  // UNION_NODE,
                Unit::LOAD_NODE,  // LOAD_NODE,
                Unit::SINK_NODE,  //  SINK_NODE,
                Unit::PROCESS_NODE,  // PROCESS_NODE
                Unit::SHUFFLE_NODE,  // SHUFFLE_NODE
            };
            Unit* unit = LeafUnit(node.id());
            unit->set_type(kTypes[node.type()]);
            unit->get<PbLogicalPlanNode>() = node;
            if (node.is_infinite()) {
                unit->set<IsInfinite>();
            }

            m_plan->AddControl(ControlUnit(node.scope()), unit);

            boost::graph_traits<NodeDag>::adjacency_iterator ptr, end;
            boost::tie(ptr, end) = boost::adjacent_vertices(v, dag);
            while (ptr != end) {
                const PbLogicalPlanNode& target = dag[*ptr];
                m_plan->AddDependency(unit, LeafUnit(target.id()));

                ++ptr;
            }
        }
    }

    Unit* LeafUnit(const std::string& id) {
        return FindOrNewUnit(id, true);
    }

    Unit* ControlUnit(const std::string& id) {
        return FindOrNewUnit(id, false);
    }

    Unit* FindOrNewUnit(const std::string& id, bool is_leaf) {
        Unit* unit = NULL;

        std::unordered_map<std::string, Unit*>::iterator ptr = m_units.find(id);
        if (ptr == m_units.end()) {
            unit = m_plan->NewUnit(id, is_leaf);
            m_units[id] = unit;
        } else {
            unit = ptr->second;
            CHECK_EQ(is_leaf, unit->is_leaf());
        }

        return unit;
    }

    void CheckInitialized() {
        typedef std::unordered_map<std::string, Unit*>::iterator Iterator;
        for (Iterator ptr = m_units.begin(); ptr != m_units.end(); ++ptr) {
            Unit* unit = ptr->second;
            if (unit->is_leaf()) {
                CHECK(unit->has<PbLogicalPlanNode>());
            } else {
                CHECK(unit->has<PbScope>());
                PbScope& scope = unit->get<PbScope>();
                if (scope.type() == PbScope::BUCKET && !scope.has_bucket_scope()) {
                    scope.mutable_bucket_scope();
                }
            }
        }
    }

private:
    PbLogicalPlan m_message;
    Plan* m_plan;
    toft::scoped_ptr<PbEntity> m_env;

    std::unordered_map<std::string, Unit*> m_units;
};

}  // namespace

void LoadLogicalPlanPass::Initialize(const PbLogicalPlan& logical_plan) {
    m_logical_plan = logical_plan;
}

bool LoadLogicalPlanPass::Run(Plan* plan) {
    LoadLogicalPlanPassImpl impl(m_logical_plan, plan);
    impl.Run();
    return true;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
