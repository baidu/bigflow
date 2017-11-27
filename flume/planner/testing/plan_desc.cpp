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
//         Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/planner/testing/plan_desc.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include "boost/algorithm/string.hpp"
#include "boost/bimap.hpp"
#include "boost/bimap/multiset_of.hpp"
#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/make_shared.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/range/adaptors.hpp"
#include "boost/range/algorithm.hpp"
#include "boost/shared_ptr.hpp"

#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/testing/edge_desc.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

PlanDesc::PlanDesc() {
}

PlanDesc::PlanDesc(bool is_leaf) {
    NewVertex(is_leaf, toft::CreateCanonicalUUIDString(), /*is_anonymous =*/ true);
}

PlanDesc::PlanDesc(bool is_leaf, const std::string& identity) {
    NewVertex(is_leaf, identity, /*is_anonymous =*/ false);
}

PlanDesc::Vertex PlanDesc::NewVertex(bool is_leaf,
                                     const std::string& identity, bool is_anonymous) {
    Vertex vertex(new VertexInfo());
    vertex->is_leaf = is_leaf;
    vertex->identity = identity;
    vertex->is_anonymous = is_anonymous;

    m_vertices.insert(vertex);
    m_recent_vertices.push_back(vertex);

    return vertex;
}

std::string PlanDesc::identity() const {
    this->vertex()->is_anonymous = false;
    return this->vertex()->identity;
}

PlanDesc::Vertex PlanDesc::vertex() const {
    CHECK_EQ(m_recent_vertices.size(), 1);
    return m_recent_vertices.front();
}

PlanDesc PlanDesc::MergeFrom(const PlanDesc& other) const {
    PlanDesc result;

    result.m_vertices = m_vertices;
    result.m_vertices.insert(other.m_vertices.begin(), other.m_vertices.end());

    result.m_controls = m_controls;
    result.m_controls.insert(other.m_controls.begin(), other.m_controls.end());

    // use set to erase duplicated edges
    std::set<Edge> deps(m_deps.begin(), m_deps.end());
    deps.insert(other.m_deps.begin(), other.m_deps.end());
    result.m_deps.insert(deps.begin(), deps.end());

    // use tags to erase duplicated tag desc
    std::set< std::pair<Vertex, TagDescRef> > tags;
    typedef std::pair< Vertex, std::list<TagDescRef> > TagDescPair;
    BOOST_FOREACH(TagDescPair pair, m_tag_descs) {
        BOOST_FOREACH(TagDescRef desc, pair.second) {
            tags.insert(std::make_pair(pair.first, desc));
        }
    }

    // merge tag desces
    result.m_tag_descs = m_tag_descs;
    BOOST_FOREACH(TagDescPair pair, other.m_tag_descs) {
        BOOST_FOREACH(TagDescRef desc, pair.second) {
            if (tags.count(std::make_pair(pair.first, desc)) == 0) {
                tags.insert(std::make_pair(pair.first, desc));
                result.m_tag_descs[pair.first].push_back(desc);
            }
        }
    }

    // use edges to erase duplicated edge desc
    std::set< std::pair<Edge, EdgeDescRef> > edges;
    typedef std::pair< Edge, std::list<EdgeDescRef> > EdgeDescPair;
    BOOST_FOREACH(EdgeDescPair pair, m_edge_descs) {
        BOOST_FOREACH(EdgeDescRef desc, pair.second) {
            edges.insert(std::make_pair(pair.first, desc));
        }
    }

    // merge edge desces
    result.m_edge_descs = m_edge_descs;
    BOOST_FOREACH(EdgeDescPair pair, other.m_edge_descs) {
        BOOST_FOREACH(EdgeDescRef desc, pair.second) {
            if (edges.count(std::make_pair(pair.first, desc)) == 0) {
                edges.insert(std::make_pair(pair.first, desc));
                result.m_edge_descs[pair.first].push_back(desc);
            }
        }
    }

    return result;
}

PlanDesc PlanDesc::operator[](const PlanDesc& child) const {
    CHECK(!this->vertex()->is_leaf);

    PlanDesc result = MergeFrom(child);
    result.m_recent_vertices.push_back(this->vertex());
    BOOST_FOREACH(Vertex vertex, child.m_recent_vertices) {
        result.m_controls.insert(std::make_pair(vertex, this->vertex()));
    }

    return result;
}

PlanDesc PlanDesc::operator|(const PlanDesc& other) const {
    PlanDesc result = MergeFrom(other);
    result.m_recent_vertices = m_recent_vertices;
    return result;
}

PlanDesc PlanDesc::operator,(const PlanDesc& brother) const {  // NOLINT(whitespace/comma)
    PlanDesc result = MergeFrom(brother);
    std::copy(m_recent_vertices.begin(), m_recent_vertices.end(),
              std::back_inserter(result.m_recent_vertices));
    std::copy(brother.m_recent_vertices.begin(), brother.m_recent_vertices.end(),
              std::back_inserter(result.m_recent_vertices));
    return result;
}

PlanDesc PlanDesc::operator>>(const PlanDesc& peer) const {
    PlanDesc result = MergeFrom(peer);
    result.m_recent_vertices.push_back(peer.vertex());

    Edge edge(peer.vertex(), this->vertex());
    BOOST_FOREACH(EdgeDescRef desc, m_recent_edge_descs) {
        result.m_edge_descs[edge].push_back(desc);
    }

    BOOST_FOREACH(Edge dep, m_deps.equal_range(edge.first)) {
        if (dep == edge) {
            LOG(WARNING) << "Try to describe duplicated edges in plan!";
            return result;
        }
    }
    result.m_deps.insert(edge);

    return result;
}

PlanDesc PlanDesc::operator>>(EdgeDescRef edge_desc) const {
    PlanDesc result = *this;
    result.m_recent_edge_descs.push_back(edge_desc);
    return result;
}

PlanDesc PlanDesc::operator+(TagDescRef tag) const {
    PlanDesc result = *this;
    result.m_tag_descs[this->vertex()].push_back(tag);
    return result;
}

void PlanDesc::to_plan(Plan* plan) const {
    std::map<Vertex, Unit*> vertex_to_units;

    std::set<Vertex> roots = m_vertices;
    BOOST_FOREACH(Edge edge, m_controls) {
        roots.erase(edge.first);  // remote child from candidates
    }
    CHECK_EQ(roots.size(), 1u);
    vertex_to_units[*roots.begin()] = plan->Root();

    BOOST_FOREACH(Vertex vertex, m_vertices) {
        if (vertex_to_units.count(vertex) != 0) {
            continue;
        }

        vertex_to_units[vertex] = plan->NewUnit(vertex->is_leaf);
    }

    typedef std::pair<Vertex, Unit*> VertexUnitPair;
    BOOST_FOREACH(VertexUnitPair pair, vertex_to_units) {
        pair.first->is_anonymous = false;
        pair.second->set_identity(pair.first->identity);
        pair.second->get<Vertex>() = pair.first;
    }

    BOOST_FOREACH(Edge edge, m_controls) {
        plan->AddControl(vertex_to_units[edge.second], vertex_to_units[edge.first]);
    }

    BOOST_FOREACH(Edge edge, m_deps) {
        plan->AddDependency(vertex_to_units[edge.second], vertex_to_units[edge.first]);
    }

    // always apply tags to father first
    std::list<Unit*> next_units(1, plan->Root());
    while (!next_units.empty()) {
        Unit* unit = next_units.front();
        Vertex vertex = unit->get<Vertex>();

        if (m_tag_descs.count(vertex) != 0) {
            BOOST_FOREACH(TagDescRef desc, m_tag_descs.find(vertex)->second) {
                desc->Set(unit);
            }
        }

        next_units.pop_front();
        BOOST_FOREACH(Unit* child, unit->children()) {
            next_units.push_back(child);
        }
    }

    typedef std::pair< Edge, std::list<EdgeDescRef> > EdgeDescPair;
    BOOST_FOREACH(EdgeDescPair pair, m_edge_descs) {
        Edge edge = pair.first;
        BOOST_FOREACH(EdgeDescRef desc, pair.second) {
            desc->Set(vertex_to_units[edge.second], vertex_to_units[edge.first]);
        }
    }
}

bool PlanDesc::TestTagDescs(Vertex vertex, Unit* unit) const {
    if (m_tag_descs.count(vertex) == 0) {
        return true;
    }

    BOOST_FOREACH(TagDescRef desc, m_tag_descs.find(vertex)->second) {
        if (!desc->Test(unit)) {
            return false;
        }
    }
    return true;
}

bool PlanDesc::TestEdgeDescs(Edge edge, Unit* from, Unit* to) const {
    if (m_edge_descs.count(edge) == 0) {
        return true;
    }

    BOOST_FOREACH(EdgeDescRef desc, m_edge_descs.find(edge)->second) {
        if (!desc->Test(from, to)) {
            return false;
        }
    }
    return true;
}

struct PlanDesc::MatchContext {
    std::multimap<Unit*, Vertex> candidates;

    boost::bimap<
        boost::bimaps::set_of<Unit*>,
        boost::bimaps::set_of<Vertex>
    > matches;
};

bool PlanDesc::is_plan(Plan* plan) const {
    if (plan->GetAllUnits().size() != m_vertices.size()) {
        LOG(WARNING) << "Unit number mismatch!"
        << ": vertices in result: "<< plan->GetAllUnits().size()
            << " in expect: "<< m_vertices.size();
        return false;
    }

    MatchContext context;
    if (!InitializeMatchContext(plan, &context)) {
        return false;
    }
    return MatchNext(&context);
}

bool PlanDesc::InitializeMatchContext(Plan* plan, MatchContext* context) const {
    boost::bimap<
        boost::bimaps::set_of<Vertex>,
        boost::bimaps::multiset_of<Vertex>
    > child_father_map;
    child_father_map.left.insert(m_controls.begin(), m_controls.end());

    boost::bimap<
        boost::bimaps::multiset_of<Vertex>,
        boost::bimaps::multiset_of<Vertex>
    > user_need_map;
    user_need_map.left.insert(m_deps.begin(), m_deps.end());

    std::set<Vertex> free_vertices = m_vertices;
    BOOST_FOREACH(Unit* unit, plan->GetAllUnits()) {
        if (unit->has<Vertex>()) {
            free_vertices.erase(unit->get<Vertex>());
        }
    }

    BOOST_FOREACH(Unit* unit, plan->GetAllUnits()) {
        std::set<Vertex> candidates = free_vertices;  // free vertices may match any unit
        if (unit->has<Vertex>() && m_vertices.count(unit->get<Vertex>()) != 0) {
            candidates.insert(unit->get<Vertex>());
        }

        BOOST_FOREACH(Vertex candidate, candidates) {
            // if have same number of childs / users / needs
            if ((!candidate->is_anonymous && candidate->identity != unit->identity())
                    || (child_father_map.left.count(candidate) == 0 && unit->father() != NULL)
                    || (child_father_map.left.count(candidate) != 0 && unit->father() == NULL)
                    || child_father_map.right.count(candidate) != unit->children().size()
                    || user_need_map.left.count(candidate) != unit->direct_needs().size()
                    || user_need_map.right.count(candidate) != unit->direct_users().size()
                    || !TestTagDescs(candidate, unit)) {
                continue;
            }
            context->candidates.insert(std::make_pair(unit, candidate));
        }

        if (context->candidates.count(unit) == 0) {
            LOG(WARNING) << "can not find candidate for " << unit->identity();
            return false;
        }
    }

    return true;
}

bool PlanDesc::MatchNext(MatchContext* context) const {
    using boost::adaptors::map_keys;
    using boost::adaptors::uniqued;
    typedef std::pair<Unit*, Vertex> Match;

    std::vector<Unit*> unmatched_units;
    boost::set_difference(context->candidates | map_keys | uniqued,
                          context->matches.left | map_keys,
                          std::back_inserter(unmatched_units));
    if (unmatched_units.size() == 0) {
        return true;
    }

    Unit* next_unit = NULL;
    std::set<Vertex> next_vertices;
    BOOST_FOREACH(Unit* unit, unmatched_units) {
        if (unit->father() != NULL && context->matches.left.count(unit->father()) == 0) {
            continue;
        }

        std::set<Unit*> needs;  // boost::includes needs sorted range
        boost::copy(unit->direct_needs(), std::inserter(needs, needs.end()));
        if (!boost::includes(context->matches.left | map_keys, needs)) {
            continue;
        }

        std::set<Vertex> vertices;
        BOOST_FOREACH(Match match, context->candidates.equal_range(unit)) {
            if (context->matches.right.count(match.second) == 0) {
                vertices.insert(match.second);
            }
        }

        if (vertices.size() == 0) {
            return false;
        }

        if (next_unit == NULL || vertices.size() < next_vertices.size()) {
            next_unit = unit;
            next_vertices = vertices;
        }
    }

    BOOST_FOREACH(Vertex next_vertex, next_vertices) {
        if (TryMatch(next_unit, next_vertex, context)) {
            return true;
        }
    }
    return false;
}

bool PlanDesc::TryMatch(Unit* unit, Vertex vertex, MatchContext* context) const {
    Unit* expected_father = NULL;
    BOOST_FOREACH(Edge edge, m_controls.equal_range(vertex)) {
        if (context->matches.right.count(edge.second) == 0) {
            return false;
        }
        expected_father = context->matches.right.at(edge.second);
    }

    if (unit->father() != expected_father) {
        return false;
    }

    std::set<Unit*> expected_deps;
    BOOST_FOREACH(Edge edge, m_deps.equal_range(vertex)) {
        if (context->matches.right.count(edge.second) == 0) {
            return false;
        }
        expected_deps.insert(context->matches.right.at(edge.second));
    }

    BOOST_FOREACH(Unit* dep, unit->direct_needs()) {
        if (expected_deps.count(dep) == 0) {
            return false;
        }

        Edge edge(vertex, context->matches.left.at(dep));
        if (!TestEdgeDescs(edge, dep, unit)) {
            return false;
        }
    }

    context->matches.left.insert(std::make_pair(unit, vertex));
    if (MatchNext(context)) {
        return true;
    } else {
        context->matches.left.erase(unit);
        return false;
    }
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

