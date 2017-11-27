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
//
// Build boost::graph for logical plan and physical plan. See implemention details in
// flume/planner/graph_helper_impl.h.
// See also http://www.boost.org/doc/libs/1_55_0/libs/graph/doc/index.html

#ifndef FLUME_PLANNER_GRAPH_HELPER_H_
#define FLUME_PLANNER_GRAPH_HELPER_H_

#include <map>
#include <string>
#include <vector>

#include "flume/planner/graph_helper_impl.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

// Given a logical plan and a boost::graph instance, build graph according to data flow
// of all logical plan nodes.
// An extra map is needed as output param, where caller can look up graph vertex by
// logical plan node id.
template<typename Graph>
bool BuildLogicalDag(const PbLogicalPlan& message, Graph* graph,
                     std::map<std::string,
                              typename LogicalDagBuilder<Graph>::Vertex>* vertices) {
    LogicalDagBuilder<Graph> builder(message, graph, vertices);
    return builder.Build();
}

// Same as BuildLogicalDag, excepts that accept iterator as inputs
template<typename InputIterator, typename Graph>
bool BuildLogicalDag(InputIterator begin, InputIterator end, Graph* graph,
                     std::map<std::string,
                     typename LogicalDagBuilder<Graph>::Vertex>* vertices) {
    LogicalDagBuilder<Graph> builder(begin, end, graph, vertices);
    return builder.Build();
}

// Given a logical plan and a boost::graph instance, build graph according to scope
// hierarchy in logical plan.
// An extra map is needed as output param, where caller can look up graph vertex by
// logical plan scope id.
template<typename Graph>
bool BuildScopeTree(const PbLogicalPlan& message, Graph* graph,
                    std::map<std::string,
                             typename ScopeTreeBuilder<Graph>::Vertex>* vertices) {
    ScopeTreeBuilder<Graph> builder(message, graph, vertices);
    return builder.Build();
}

// Given a executor proto message and boost::Graph instance, build graph according to
// source-handle relationships between child executors.
// Each child executor deserve a vertex in the builded graph, besides that two more
// vertice will be added into graph, one represents all sources provided by father
// executor, the other represents all sources needed by father executor.
// An extra vector is needed as output param, the virtual vertex of input will be put at
// vertices->front(), while the output vertex will be put at vertices->back().
template<typename Graph>
bool BuildExecutorDag(const PbExecutor& message, Graph* graph,
                      std::vector<typename ExecutorDagBuilder<Graph>::Vertex>* vertices) {
    ExecutorDagBuilder<Graph> builder(message, graph, vertices);
    return builder.Build();
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_GRAPH_HELPER_H_
