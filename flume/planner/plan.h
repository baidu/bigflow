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
// Author: Zhou Kai <bigflow-opensource@baidu.com>

#ifndef FLUME_PLANNER_PLAN_H_
#define FLUME_PLANNER_PLAN_H_

#include <string>
#include <vector>

#include "boost/graph/adjacency_list.hpp"
#include "toft/base/shared_ptr.h"
#include "toft/base/unordered_map.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

class Unit;

class Plan {
public:
    friend class Unit;

    Plan();

    Unit* Root() const;

    Unit* NewUnit(bool is_leaf);

    Unit* NewUnit(const std::string& id, bool is_leaf);

    // "control" represents scope/subscope or scope/node relation
    void AddControl(Unit* father, Unit* child);

    void RemoveControl(Unit* father, Unit* child);

    void ReplaceControl(Unit* father, Unit* child, Unit* unit);

    // "dependency" represents data produce/consume relation between nodes
    void AddDependency(Unit* source, Unit* target);

    bool HasDependency(Unit* source, Unit* target);

    void RemoveDependency(Unit* source, Unit* target);

    void ReplaceDependency(Unit* source, Unit* target, Unit* unit);

    // replace unit all "from" field in pb from old_id to new_id
    void ReplaceFrom(Unit* unit, const std::string& old_id, const std::string& new_id);

    // set unit discard but the pointer is still valid
    // this will remove all existing control/dependency on unit
    void RemoveUnit(Unit* unit);

    // all non-discard units
    std::vector<Unit*> GetAllUnits() const;

    // topological order of non-discard units (default only includes leaf units)
    std::vector<Unit*> GetTopologicalOrder(bool include_control_units = false) const;

    std::string DebugString() const;

private:
    typedef boost::adjacency_list<
        boost::setS, boost::vecS, boost::bidirectionalS, std::shared_ptr<Unit> > Dag;
    typedef boost::vertex_bundle_type<Dag>::type VertexInfo;
    typedef boost::graph_traits<Dag>::vertex_descriptor Vertex;
    typedef boost::graph_traits<Dag>::edge_descriptor Edge;
    typedef std::unordered_map<Unit*, Vertex> VertexMap;

    static void AppendToString(Unit* unit, std::string* text, const std::string& prefix);

    std::vector<Unit*> GetDirectUsers(const Unit* unit) const;
    std::vector<Unit*> GetDirectNeeds(const Unit* unit) const;

    Dag m_dag;
    VertexMap m_vertices;
    Unit* m_root;
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_PLAN_H_
