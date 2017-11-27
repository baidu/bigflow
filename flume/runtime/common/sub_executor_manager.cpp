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

#include "flume/runtime/common/sub_executor_manager.h"

#include <algorithm>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "boost/graph/topological_sort.hpp"
#include "boost/tuple/tuple.hpp"
#include "glog/logging.h"

#include "flume/planner/graph_helper.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace {

struct ExecutorInfo {
    PbExecutor message;
    Executor* executor;

    ExecutorInfo() : executor(NULL) {}

    ExecutorInfo& operator=(const PbExecutor& message) {
        this->message = message;
        return *this;
    }
};

struct Dependency {
    std::string id;

    Dependency& operator=(const std::string& source) {
        this->id = source;
        return *this;
    }
};

typedef boost::adjacency_list<
    boost::vecS, boost::vecS, boost::bidirectionalS, ExecutorInfo, Dependency
> Dag;
typedef boost::graph_traits<Dag>::vertex_descriptor Vertex;
typedef boost::graph_traits<Dag>::edge_descriptor Edge;

}  // namespace

void SubExecutorManager::Initialize(const PbExecutor& message,
                                    const std::vector<Executor*>& childs) {
    CHECK_EQ(message.child_size(), static_cast<int>(childs.size()));

    m_message = message;
    m_childs = childs;
}

void SubExecutorManager::Setup(const std::map<std::string, Source*>& inputs) {
    // anyalize dependencies between sub-executors
    Dag dag;
    std::vector<Vertex> vertices;
    CHECK_EQ(true, planner::BuildExecutorDag(m_message, &dag, &vertices));
    for (size_t i = 1; i <= m_childs.size(); ++i) {
        dag[vertices[i]].executor = m_childs[i - 1];
    }
    std::list<Vertex> topo_order;
    boost::topological_sort(dag, std::front_inserter(topo_order));

    // Setup sub-executor in topological order
    m_message.clear_child();
    m_childs.clear();
    for (std::list<Vertex>::iterator it = topo_order.begin(); it != topo_order.end(); ++it) {
        Vertex v = *it;
        if (v == vertices.front() || v == vertices.back()) {
            continue;
        }

        // reorder childs
        m_childs.push_back(dag[v].executor);
        *m_message.add_child() = dag[v].message;

        // setup child
        std::map<std::string, Source*> sources;
        boost::graph_traits<Dag>::in_edge_iterator ptr, end;
        boost::tie(ptr, end) = boost::in_edges(v, dag);
        while (ptr != end) {
            Vertex from = boost::source(*ptr, dag);
            const std::string& id = dag[*ptr].id;
            CHECK_EQ(0u, sources.count(id)) << "source id conflict! " << id;
            if (from == vertices.front()) {
                sources[id] = inputs.find(id)->second;
            } else {
                sources[id] = dag[from].executor->GetSource(id, m_message.scope_level());
            }
            ++ptr;
        }
        dag[v].executor->Setup(sources);
    }

    // register output sources
    {
        boost::graph_traits<Dag>::in_edge_iterator ptr, end;
        boost::tie(ptr, end) = boost::in_edges(vertices.back(), dag);
        while (ptr != end) {
            Vertex from = boost::source(*ptr, dag);
            const std::string& id = dag[*ptr].id;
            if (from != vertices.front()) {
                m_outputs[id] = dag[from].executor;
            }
            ++ptr;
        }
    }
}

Source* SubExecutorManager::GetSource(const std::string& id, int scope_level) {
    CHECK_NE(0u, m_outputs.count(id));
    return m_outputs[id]->GetSource(id, scope_level);
}

void SubExecutorManager::BeginGroup(const toft::StringPiece& key) {
    typedef std::vector<Executor*>::iterator Iterator;
    for (Iterator ptr = m_childs.begin(); ptr != m_childs.end(); ++ptr) {
        (*ptr)->BeginGroup(key);
    }
}

void SubExecutorManager::FinishGroup() {
    typedef std::vector<Executor*>::reverse_iterator Iterator;
    for (Iterator ptr = m_childs.rbegin(); ptr != m_childs.rend(); ++ptr) {
        (*ptr)->FinishGroup();
    }
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
