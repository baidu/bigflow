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
// Author: Wen Xiang <wenxiang@baidu.com>
//
// Build boost::graph for logical plan and physical plan.

#ifndef FLUME_PLANNER_GRAPH_HELPER_IMPL_H_
#define FLUME_PLANNER_GRAPH_HELPER_IMPL_H_

#include <map>
#include <string>
#include <vector>

#include "boost/graph/adjacency_list.hpp"
#include "glog/logging.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

template<typename Graph>
class LogicalDagBuilder {
public:
    typedef typename boost::vertex_bundle_type<Graph>::type VertexInfo;
    typedef typename boost::edge_bundle_type<Graph>::type EdgeInfo;
    typedef typename boost::graph_traits<Graph>::vertex_descriptor Vertex;
    typedef typename boost::graph_traits<Graph>::edge_descriptor Edge;

public:
    LogicalDagBuilder(const PbLogicalPlan& message,
                      Graph* graph, std::map<std::string, Vertex>* vertices)
            : m_nodes(message.node().begin(), message.node().end()),
              m_graph(*graph), m_vertices(*vertices) {}

    template<typename InputIterator>
    LogicalDagBuilder(InputIterator begin, InputIterator end,
                      Graph* graph, std::map<std::string, Vertex>* vertices)
            : m_nodes(begin, end), m_graph(*graph), m_vertices(*vertices) {}

    bool Build() {
        for (size_t i = 0; i < m_nodes.size(); ++i) {
            const PbLogicalPlanNode& message = m_nodes[i];
            Vertex node = FindOrInsertVertex(message.id());
            m_graph[node] = message;

            ParseDependency(node, message);
        }
        return true;
    }

private:
    Vertex FindOrInsertVertex(const std::string& id) {
        typename std::map<std::string, Vertex>::iterator ptr = m_vertices.find(id);
        if (ptr != m_vertices.end()) {
            return ptr->second;
        }

        Vertex v = boost::add_vertex(m_graph);
        m_vertices[id] = v;

        return v;
    }

    void ParseDependency(const Vertex& target, const PbLogicalPlanNode& node) {
        switch (node.type()) {
            case PbLogicalPlanNode::UNION_NODE:
                for (int i = 0; i < node.union_node().from_size(); ++i) {
                    boost::add_edge(FindOrInsertVertex(node.union_node().from(i)),
                                    target, m_graph);
                }
                break;
            case PbLogicalPlanNode::SINK_NODE:
                boost::add_edge(FindOrInsertVertex(node.sink_node().from()),
                                target, m_graph);
                break;
            case PbLogicalPlanNode::PROCESS_NODE:
                for (int i = 0; i < node.process_node().input_size(); ++i) {
                    boost::add_edge(FindOrInsertVertex(node.process_node().input(i).from()),
                                    target, m_graph);
                }
                break;
            case PbLogicalPlanNode::SHUFFLE_NODE:
                boost::add_edge(FindOrInsertVertex(node.shuffle_node().from()),
                                target, m_graph);
                break;
            default:
                // LOAD_NODE
                break;
        }
    }

private:
    std::vector<PbLogicalPlanNode> m_nodes;
    Graph& m_graph;
    std::map<std::string, Vertex>& m_vertices;
};

template<typename Graph>
class ScopeTreeBuilder {
public:
    typedef typename boost::vertex_bundle_type<Graph>::type VertexInfo;
    typedef typename boost::edge_bundle_type<Graph>::type EdgeInfo;
    typedef typename boost::graph_traits<Graph>::vertex_descriptor Vertex;
    typedef typename boost::graph_traits<Graph>::edge_descriptor Edge;

public:
    ScopeTreeBuilder(const PbLogicalPlan& message,
                     Graph* graph, std::map<std::string, Vertex>* vertices)
            : m_message(message), m_graph(*graph), m_vertices(*vertices) {}

    bool Build() {
        for (int i = 0; i < m_message.scope_size(); ++i) {
            const PbScope& message = m_message.scope(i);

            Vertex scope = FindOrInsertVertex(message.id());
            m_graph[scope] = message;

            if (message.has_father()) {
                Vertex father = FindOrInsertVertex(message.father());
                boost::add_edge(father, scope, m_graph);
            }
        }
        return true;
    }

private:
    Vertex FindOrInsertVertex(const std::string& id) {
        typename std::map<std::string, Vertex>::iterator ptr = m_vertices.find(id);
        if (ptr != m_vertices.end()) {
            return ptr->second;
        }

        Vertex v = boost::add_vertex(m_graph);
        m_vertices[id] = v;

        return v;
    }

private:
    const PbLogicalPlan& m_message;
    Graph& m_graph;
    std::map<std::string, Vertex>& m_vertices;
};

template<typename Graph>
class ExecutorDagBuilder {
public:
    typedef typename boost::vertex_bundle_type<Graph>::type VertexInfo;
    typedef typename boost::edge_bundle_type<Graph>::type EdgeInfo;
    typedef typename boost::graph_traits<Graph>::vertex_descriptor Vertex;
    typedef typename boost::graph_traits<Graph>::edge_descriptor Edge;

public:
    ExecutorDagBuilder(const PbExecutor& message, Graph* graph, std::vector<Vertex>* vertices)
            : m_message(message), m_graph(*graph), m_vertices(*vertices) {}

    bool Build() {
        m_in = boost::add_vertex(m_graph);
        m_vertices.push_back(m_in);

        typedef std::map<Vertex, const PbExecutor*> Childs;
        Childs childs;
        for (int i = 0; i < m_message.child_size(); ++i) {
            Vertex v = boost::add_vertex(m_graph);
            m_vertices.push_back(v);

            const PbExecutor& child = m_message.child(i);
            childs[v] = &child;
            m_graph[v] = child;

            for (int j = 0; j < child.output_size(); ++j) {
                if (!AddSource(child.output(j), v)) {
                    return false;
                }
            }
        }

        for (typename Childs::iterator it = childs.begin(); it != childs.end(); ++it) {
            Vertex v = it->first;
            const PbExecutor* child = it->second;
            for (int i = 0; i < child->input_size(); ++i) {
                AddSourceUsage(child->input(i), v);
            }
        }

        m_out = boost::add_vertex(m_graph);
        m_vertices.push_back(m_out);
        for (int i = 0; i < m_message.output_size(); ++i) {
            AddSourceUsage(m_message.output(i), m_out);
        }

        return true;
    }

private:
    bool AddSource(const std::string& id, Vertex v) {
        if (m_sources.count(id) != 0) {
            LOG(WARNING) << "Found duplicated source[" << id << "]: " << m_message.DebugString();
            return false;
        }
        m_sources[id] = v;
        return true;
    }

    void AddSourceUsage(const std::string& source, Vertex to) {
        // no child provides this source, it must be provided by executor itself.
        typename Sources::iterator ptr = m_sources.find(source);
        Vertex from = ptr != m_sources.end() ? ptr->second : m_in;

        // in graphs which allow parallel edges, it may create multi-edges
        Edge e = boost::add_edge(from, to, m_graph).first;
        m_graph[e] = source;
    }

private:
    const PbExecutor& m_message;
    Graph& m_graph;
    std::vector<Vertex>& m_vertices;

    Vertex m_in;
    Vertex m_out;

    typedef std::map<std::string, Vertex> Sources;
    Sources m_sources;
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_GRAPH_HELPER_IMPL_H_
