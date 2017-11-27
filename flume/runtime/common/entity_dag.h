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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//

#ifndef FLUME_RUNTIME_COMMON_ENTITY_DAG_H_
#define FLUME_RUNTIME_COMMON_ENTITY_DAG_H_

#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/graph/topological_sort.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/emitter.h"
#include "flume/planner/graph_helper.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/util/bitset.h"

namespace baidu {
namespace flume {
namespace runtime {

class EntityDag {
public:
    typedef toft::Closure<void (const std::vector<toft::StringPiece>&, void*)> Listener;

    class Instance {
    public:
        virtual ~Instance() {}

        virtual bool Run(int input, void* object) = 0;

        virtual void Done() = 0;
    };

    EntityDag() {}
    virtual ~EntityDag() {}

    // nodes shoulded be ordered by reverse topological order
    void Initialize(const std::map<std::string, PbScope>& scopes,
                    const std::vector<PbLogicalPlanNode>& nodes,
                    const std::vector<int32_t>& scope_levels,
                    const std::vector<std::string>& inputs,
                    boost::ptr_multimap<std::string, Listener>* listeners);

    // an easy-to-use version of Initialize. accept iterators from various containers,
    // while need not to order nodes in advance.
    template<typename ScopeIterator,
             typename NodeIterator,
             typename ScopeLevelIterator,
             typename InputIterator>
    void Initialize(ScopeIterator scope_begin, ScopeIterator scope_end,
                    NodeIterator node_begin, NodeIterator node_end,
                    ScopeLevelIterator level_begin, ScopeLevelIterator level_end,
                    InputIterator input_begin, InputIterator input_end,
                    boost::ptr_multimap<std::string, Listener>* listeners);

    // make virtual for mock
    virtual Instance* GetInstance(const std::vector<toft::StringPiece>& keys);

private:
    typedef boost::dynamic_bitset<> Bitset;

    class InstanceImpl;
    class RunnerFactory;
    class LoaderRunnerFactory;
    class PrimaryProcessorRunnerFactory;
    class SideProcessorRunnerFactory;
    class KeyReaderRunnerFactory;
    class PartitionerRunnerFactory;
    class HashRunnerFactory;
    class SinkerRunnerFactory;
    class ListenerRunnerFactory;

    struct Record {
        std::vector<toft::StringPiece> keys;
        void* object;
    };

    class Runner : public core::Emitter {
    public:
        explicit Runner(const RunnerFactory* factory, InstanceImpl* instance) :
                m_factory(factory), m_instance(instance), m_current_keys(NULL) {}
        virtual ~Runner() {}

        virtual void Finish() {}

        const Bitset& mask() const { return m_factory->mask(); }

        const Bitset& downstream() const { return m_factory->downstream(); }

        bool Dispatch(Record* record);

        void DoRun(Record* record);
        void DoPrepare(const std::vector<toft::StringPiece>& keys);

        const std::vector<toft::StringPiece>* current_keys() const { return m_current_keys; }
        Record* tmp_record() { return &m_tmp_record; }

    protected:
        virtual void Run(Record* record) {}
        virtual void Prepare(const std::vector<toft::StringPiece>& keys) {}

    private:
        void set_current_keys(const std::vector<toft::StringPiece>* keys) { m_current_keys = keys; }

        // implements core::Emitter
        virtual bool Emit(void *object);
        virtual void Done();

        const RunnerFactory* m_factory;
        InstanceImpl* m_instance;
        const std::vector<toft::StringPiece>* m_current_keys;
        Record m_tmp_record;
    };

    class RunnerFactory {
    public:
        virtual ~RunnerFactory() {}

        void Initialize(uint32_t index, const std::string& identity,
                        const Bitset& mask, const Bitset& downstream) {
            m_index = index;
            m_identity = identity;
            m_mask = mask;
            m_downstream = downstream;

            DLOG(INFO) << m_downstream << " " << m_identity << " " << m_index;
        }

        const std::string& identity() const { return m_identity; }

        const Bitset& mask() const { return m_mask; }

        const Bitset& downstream() const { return m_downstream; }

        virtual Runner* NewRunner(InstanceImpl* instance) = 0;

    private:
        uint32_t m_index;
        std::string m_identity;
        Bitset m_mask;
        Bitset m_downstream;
    };

    InstanceImpl* NewInstance();

    Bitset ToMask(const std::vector<std::string>& outputs,
                  const std::string& identity);

    Bitset ToDownstream(const std::multimap<std::string, uint32_t>& downstreams,
                        const std::string& identity);

    void SetupForLoadNode(const PbLoadNode& node);

    void SetupForProcessNode(const PbProcessNode& node,
                             std::multimap<std::string, uint32_t>* downstreams);

    void SetupForShuffleNode(const PbScope& scope,
                             const PbShuffleNode& node, const PbEntity& objector,
                             std::multimap<std::string, uint32_t>* downstreams,
                             int32_t scope_level);

    void SetupForUnionNode(const std::string& identity, const PbUnionNode& node,
                           std::multimap<std::string, uint32_t>* downstreams);

    void SetupForSinkNode(const PbSinkNode& node,
                          std::multimap<std::string, uint32_t>* downstreams);

    void SetupForListener(const std::string& identity, Listener* listener,
                          std::multimap<std::string, uint32_t>* downstreams);

private:
    boost::ptr_multimap<std::string, Listener> m_listeners;
    boost::ptr_vector<RunnerFactory> m_runner_factories;
    std::vector<Bitset> m_input_downstreams;
};

template<typename ScopeIterator,
         typename NodeIterator,
         typename ScopeLevelIterator,
         typename InputIterator>
void EntityDag::Initialize(ScopeIterator scope_begin, ScopeIterator scope_end,
                           NodeIterator node_begin, NodeIterator node_end,
                           ScopeLevelIterator level_begin, ScopeLevelIterator level_end,
                           InputIterator input_begin, InputIterator input_end,
                           boost::ptr_multimap<std::string, Listener>* listeners) {
    typedef boost::adjacency_list<
        boost::setS, boost::vecS, boost::bidirectionalS, PbLogicalPlanNode
    > Dag;
    typedef Dag::vertex_descriptor Vertex;

    // for scopes
    std::map<std::string, PbScope> scopes;
    for (ScopeIterator ptr = scope_begin; ptr != scope_end; ++ptr) {
        scopes[ptr->id()] = *ptr;
    }

    std::vector<PbLogicalPlanNode> node_vec(node_begin, node_end);
    std::vector<int32_t> scope_levels(level_begin, level_end);
    std::map<std::string, int32_t> scope_levels_map;
    CHECK_EQ(node_vec.size(), scope_levels.size());
    for (size_t i = 0; i != node_vec.size(); ++i) {
        scope_levels_map[node_vec[i].id()] = scope_levels[i];
    }

    // for nodes
    Dag dag;
    std::map<std::string, Vertex> vertices;
    planner::BuildLogicalDag(node_begin, node_end, &dag, &vertices);

    std::vector<Vertex> reverse_topo_order;
    boost::topological_sort(dag, std::back_inserter(reverse_topo_order));

    std::vector<PbLogicalPlanNode> nodes;
    for (size_t i = 0; i < reverse_topo_order.size(); ++i) {
        PbLogicalPlanNode node = dag[reverse_topo_order[i]];
        if (node.IsInitialized()) {
            nodes.push_back(node);
        }
    }

    CHECK_EQ(nodes.size(), std::distance(node_begin, node_end));

    // for inputs
    std::vector<std::string> inputs(input_begin, input_end);
    for (size_t i = 0; i != nodes.size(); ++i) {
        scope_levels[i] = scope_levels_map[nodes[i].id()];
    }

    Initialize(scopes, nodes, scope_levels, inputs, listeners);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_ENTITY_DAG_H_

