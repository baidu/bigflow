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

#ifndef FLUME_PLANNER_TESTING_PLAN_DESC_H_
#define FLUME_PLANNER_TESTING_PLAN_DESC_H_

#include <list>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

#include "flume/planner/pass.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/edge_desc.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

class PlanDesc {
public:
    PlanDesc();

    // with one new vertex
    explicit PlanDesc(bool is_leaft);
    PlanDesc(bool is_leaf, const std::string& identity);

    /**
     * @brief   Add child(control relation) to current PlanDesc, merge child's vertex and
     *          edge information to current PlanDesc.
     * @param   PlanDesc of child
     * @return  Copy of current PlanDesc.
     * @usage   father
     *          [
     *              child
     *          ];
    **/
    PlanDesc operator[](const PlanDesc& child) const;

    /**
     * @brief   Merge brother's vertex and edge information to current PlanDesc.
     * @param   PlanDesc of brother
     * @return  Copy of current PlanDesc.
     * @usage   father
     *          [
     *              current,
     *              brother
     *          ];
    **/
    PlanDesc operator,(const PlanDesc& brother) const;  // NOLINT(whitespace/comma)

    /**
     * @brief   Merge data flow(dependency relation) edge information to current PlanDesc,
     *          also CHECK the node appare in the data flow must appare in the control
     *          tree first.
     * @param   PlanDesc of brother
     * @return  Copy of current PlanDesc.
     * @usage   father
     *          [
     *              current,
     *              brother
     *          ]
     *          | current >> brother;
    **/
    PlanDesc operator|(const PlanDesc& brother) const;

    /**
     * @brief   Add data flow(dependency relation) edge information to user PlanDesc,
     *          also merge edge information in current PlanDesc to user PlanDesc.
     * @param   PlanDesc of user
     * @return  Copy of user PlanDesc.
     * @usage   father
     *          [
     *              current,
     *              user
     *          ]
     *          | current >> user;
    **/
    PlanDesc operator>>(const PlanDesc& user) const;

    /**
     * @brief   Add EdgeDesc to (current PlanDesc, user PlanDesc).
     * @param   Reference of EdgeDesc
     * @return  Copy of current PlanDesc.
     * @usage   father
     *          [
     *              current,
     *              user
     *          ]
     *          | current >> &prepared >> brother;
    **/
    PlanDesc operator>>(EdgeDescRef edge_desc) const;

    /**
     * @brief   Add TagDesc to current PlanDesc.
     * @param   Reference of TagDesc
     * @return  Copy of current PlanDesc.
     * @usage   father
     *          [
     *              current,
     *              user +PbScopeDesc(PbScope::BUCKET, 1)
     *          ];
    **/
    PlanDesc operator+(TagDescRef tag) const;

    std::string identity() const;

    void to_plan(Plan* plan) const;

    bool is_plan(Plan* plan) const;

private:
    struct VertexInfo;
    typedef boost::shared_ptr<VertexInfo> Vertex;
    typedef std::pair<Vertex, Vertex> Edge;  // <user, need> or <child, father>

    struct VertexInfo {
        bool is_leaf;
        std::string identity;
        bool is_anonymous;
    };

    struct MatchContext;

    Vertex NewVertex(bool is_leaf, const std::string& identity, bool is_anonymous);

    Vertex vertex() const;

    PlanDesc MergeFrom(const PlanDesc& other) const;

    bool TestTagDescs(Vertex vertex, Unit* unit) const;

    bool TestEdgeDescs(Edge edge, Unit* from, Unit* to) const;

    bool InitializeMatchContext(Plan* plan, MatchContext* context) const;

    bool MatchNext(MatchContext* context) const;

    bool TryMatch(Unit* unit, Vertex vertex, MatchContext* context) const;

private:
    std::set<Vertex> m_vertices;
    std::map<Vertex, Vertex> m_controls;  // child -> father map
    std::multimap<Vertex, Vertex> m_deps;  //  user -> need map

    std::map< Vertex, std::list<TagDescRef> > m_tag_descs;
    std::map< Edge, std::list<EdgeDescRef> > m_edge_descs;

    std::list<Vertex> m_recent_vertices;
    std::list<EdgeDescRef> m_recent_edge_descs;
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_TESTING_PLAN_DESC_H_
