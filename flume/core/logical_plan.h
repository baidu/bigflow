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
// API for creating logical execution plan. See wing/doc/backend.rst for design
// considerations, see flume/proto/logical_plan.proto for data structure and
// serializion, see flume/core/logical_plan_test.cpp for usage.

#ifndef FLUME_CORE_LOGICAL_PLAN_H_
#define FLUME_CORE_LOGICAL_PLAN_H_

#include <list>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "toft/base/uncopyable.h"

#include "flume/core/entity.h"
#include "flume/core/key_reader.h"
#include "flume/core/loader.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/core/processor.h"
#include "flume/core/sinker.h"
#include "flume/core/time_reader.h"
#include "flume/proto/config.pb.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/runtime/backend.h"

namespace baidu {
namespace flume {

namespace runtime {
class Backend;
class CounterSession;
class Resource;
}  // namespace runtime

namespace core {

// LogicalPlan is used to construct PbLogicalPlan in flexible and expressive way.
// LogicalPlan is composed by a serial of scopes and nodes. Scopes is organized as a tree,
// while nodes is organized as a dag. Every node belongs to an particular scope, while
// only Load and Shuffle operation can create new scope.
// LogicalPlan provides basic methods for creating different kinds of Node, while each
// Node may contain some short-cut methods for convenience.
class LogicalPlan {
    TOFT_DECLARE_UNCOPYABLE(LogicalPlan);

public:
    class Scope;
    class ShuffleGroup;

    class Node;
    class UnionNode;
    class LoadNode;
    class SinkNode;
    class ProcessNode;
    class ShuffleNode;

    typedef runtime::Backend::Status Status;

public:
    LogicalPlan();

    int node_size() const { return m_nodes.size(); }
    const Node* node(int i) const { return &m_nodes[i]; }

    int scope_size() const { return m_scopes.size(); }
    const Scope* scope(int i) const { return &m_scopes[i]; }
    const Scope* global_scope() const { return scope(0); }

    int shuffle_size() const { return m_shuffles.size(); }
    const ShuffleGroup* shuffle(int i) const { return &m_shuffles[i]; }
    ShuffleGroup* mutable_shuffle(int i) { return &m_shuffles[i]; }

    // create load node
    LoadNode* RepeatedlyLoad(const std::vector<std::string>& uri_list);
    // convenient method to create load node
    LoadNode* Load(const std::vector<std::string>& uri_list);
    LoadNode* Load(const std::string& uri1, const std::string& uri2 = "",
                   const std::string& uri3 = "", const std::string& uri4 = "");

    // create sink node
    SinkNode* Sink(const Scope* scope, const Node* from);

    // create process node
    ProcessNode* Process(const Scope* scope, const std::vector<const Node*>& from);
    // convenient method to create process node
    ProcessNode* Process(const Scope* scope, const Node* n1,
                         const Node* n2 = NULL, const Node* n3 = NULL, const Node* n4 = NULL);
    // convenient method to create union node
    // omitting scope parameter, using the nearest common scope
    ProcessNode* Process(const Node* n1,
                         const Node* n2 = NULL, const Node* n3 = NULL, const Node* n4 = NULL);

    // create union node
    UnionNode* Union(const Scope* scope, const std::vector<const Node*>& from);
    // convenient method to create union node
    UnionNode* Union(const Scope* scope, const Node* n1,
                     const Node* n2 = NULL, const Node* n3 = NULL, const Node* n4 = NULL);
    // convenient method to create union node
    // omitting scope parameter, using the nearest common scope
    UnionNode* Union(const Node* n1, const Node* n2,
                     const Node* n3 = NULL, const Node* n4 = NULL);

    // create shuffle method
    ShuffleGroup* Shuffle(const Scope* scope, const std::vector<const Node*>& from);
    // convenient method to create shuffle node
    ShuffleGroup* Shuffle(const Scope* scope, const Node* n1,
                          const Node* n2 = NULL, const Node* n3 = NULL, const Node* n4 = NULL);

    // Broadcast the source_node into target_scope by adding BROADCAST ShuffleNodes
    // in each scope on the path. A UnionNode is generated from the last ShuffleNode
    // and returned.
    //
    // The target_scope cannot be ancestor of the scope source_node belongs to.
    UnionNode* BroadcastTo(const Node* source_node, const Scope* target_scope);

    PbLogicalPlan ToProtoMessage() const;

    // Run LogicalPlan in current execution environment
    void RunLocally() const;

    // Run in backend, update global counters
    Status Run(runtime::Backend* backend, runtime::Resource* resource) const;

    // Run in backend, update counters in given counter session.
    Status Run(runtime::Backend* backend,
               runtime::Resource* resource, runtime::CounterSession* counters) const;

private:
    // Add a BROADCAST shuffle node in target_scope whose 'from' is source_node.
    // The source_node must belongs to target_scope's father.
    ShuffleNode* AddShuffleNode(const Scope* target_scope, const Node* source_node);

private:
    boost::ptr_vector<Node> m_nodes;
    boost::ptr_vector<Scope> m_scopes;
    boost::ptr_vector<ShuffleGroup> m_shuffles;

};

// Scope represents sub-dag. Every LogicalPlan contains a global scope, while Load and
// Shuffle operation can create a new scope(sub-dag), which represents a collection of
// homogeneous data groups, each group contains multi-set of records.
// An unique key within scope is attached with a particular group. If the scope is sorted,
// then the processing results of each data group is ordered by that key.
class LogicalPlan::Scope {
    TOFT_DECLARE_UNCOPYABLE(Scope);

    friend class LogicalPlan;
    friend class ShuffleGroup;
    friend class ShuffleNode;
    friend class LoadNode;
public:
    // return the nearest scope, which can cover all passed-in scopes.
    static const Scope* CommonScope(const std::vector<const Scope*>& scopes);
    static const Scope* CommonScope(const std::vector<const Node*>& nodes);
    static const Scope* CommonScope(const Node* n1, const Node* n2,
                                    const Node* scope3 = NULL, const Node* scope4 = NULL);

public:
    const std::string& identity() const { return m_message.id(); }
    const Scope* father() const { return m_father; }
    bool is_sorted() const { return m_message.is_sorted(); }
    bool is_infinite() const { return m_message.is_infinite(); }
    uint32_t concurrency() const { return m_message.concurrency(); }

    // test if scope is equal or greater than this
    bool IsCoverBy(const Scope* scope) const;

    // return a list of nested scopes, from this to gloabal scope
    std::list<const Scope*> ScopeStack() const;

    PbScope ToProtoMessage() const;

private:
    Scope(const Scope* father, const LogicalPlan* plan);

private:
    const Scope* m_father;
    const LogicalPlan* m_plan;
    mutable PbScope m_message;  // TODO(wenxiang): refactor class hierarchy to remove mutable
};

// Result of Shuffle operation, contains a list of ShuffleNode, each node is corresponding
// to an input of Shuffle operation.
class LogicalPlan::ShuffleGroup {
    friend class LogicalPlan;
    friend class ShuffleNode;
public:
    const Scope* scope() const { return m_scope; }

    const std::string& identity() const { return m_scope->identity(); }

    int node_size() const { return m_nodes.size(); }
    ShuffleNode* node(int i) const { return m_nodes[i]; }

    ShuffleGroup* Sort() {
        m_scope->m_message.set_is_sorted(true);
        return this;
    }

    ShuffleGroup* WithConcurrency(uint32_t concurrency) {
        m_scope->m_message.set_concurrency(concurrency);
        return this;
    }

private:
    explicit ShuffleGroup(Scope* scope) : m_scope(scope) {}
    void add_node(ShuffleNode* node) { m_nodes.push_back(node); }

private:
    std::vector<ShuffleNode*> m_nodes;
    Scope* m_scope;
};

// A node represents an operation along with its result. Every node in logical plan
// (except sink node) must specify a SerDe class called Objector.
// A lot of convenient methons is provided to create new operation node.
class LogicalPlan::Node {
    TOFT_DECLARE_UNCOPYABLE(Node);
    friend class LogicalPlan;

public:
    // return the infinite.
    static bool IfInfinite(const std::vector<const Node*>& from, const Scope* target_scope);
    static bool IfInfinite(const Node* n1, const Node* n2, const Scope* target_scope);

public:
    virtual ~Node() {}

    const std::string& identity() const { return m_id; }
    PbLogicalPlanNode::Type type() const { return m_type; }
    const Scope* scope() const { return m_scope; }
    LogicalPlan* plan() const { return m_plan; }

    const std::string& debug_info() const { return m_debug_info; }
    void set_debug_info(const std::string& debug_info) { m_debug_info = debug_info; }

    const Entity<Objector>& objector() const { return m_objector; }
    void set_objector(const Entity<Objector>& entity) { m_objector = entity; }

    const bool is_infinite() const { return m_is_infinite; }
    void set_infinite() { m_is_infinite = true; }

    // return a new Node, whose result is the same as source, while its scope is
    // changed accordingly.
    Node* LeaveScope();  // returned node is in father scope
    Node* RemoveScope(); // returned node is in global scope

    // Sink this node in GLOBAL scope
    template<typename SinkerType>
    SinkNode* SinkBy(const std::string& config);

    // Sink this node in GLOBAL scope
    template<typename SinkerType>
    SinkNode* SinkBy() { return this->SinkBy<SinkerType>(""); }

    // process this node in CURRENT scope
    template<typename ProcessorType>
    ProcessNode* ProcessBy(const std::string& config);

    // process this node in CURRENT scope
    template<typename ProcessorType>
    ProcessNode* ProcessBy() { return this->template ProcessBy<ProcessorType>(""); }

    // process this node in COMMON scope
    ProcessNode* CombineWith(const Node* node) {
        return m_plan->Process(this, node);
    }

    // sort this node in CURRENT scope
    template<typename KeyReaderType>
    Node* SortBy(const std::string& config);

    // sort this node in CURRENT scope
    template<typename KeyReaderType>
    Node* SortBy() { return this->template SortBy<KeyReaderType>(""); }

    // sort and group this node in CURRENT scope, returned node is in NESTED scope
    template<typename KeyReaderType>
    Node* GroupBy(const std::string& config);

    // group this node in CURRENT scope, returned node is in NESTED scope
    template<typename KeyReaderType>
    Node* GroupBy() { return this->GroupBy<KeyReaderType>(""); }

    // sort and group this node in CURRENT scope, returned node is in NESTED scope
    template<typename KeyReaderType>
    Node* SortAndGroupBy(const std::string& config);

    // sort and group this node in CURRENT scope, returned node is in NESTED scope
    template<typename KeyReaderType>
    Node* SortAndGroupBy() { return this->template SortAndGroupBy<KeyReaderType>(""); }

    // distribute datas into N bucket
    Node* DistributeInto(uint32_t bucket_number);

    PbLogicalPlanNode ToProtoMessage() const;

    void Cache() { m_cache = true; }

protected:
    Node(const PbLogicalPlanNode::Type& type, const Scope* scope, LogicalPlan* plan);
    virtual void SetSpecificField(PbLogicalPlanNode* message) const = 0;

    std::string m_id;
    PbLogicalPlanNode::Type m_type;
    std::string m_debug_info;
    Entity<Objector> m_objector;

    const Scope* m_scope;
    LogicalPlan* m_plan;
    bool m_cache;
    bool m_is_infinite;
};

// Load node whill create a new scope inside global scope, use split as key.
class LogicalPlan::LoadNode : public LogicalPlan::Node {
    friend class LogicalPlan;
public:
    int uri_size() const { return m_uri_list.size(); }
    std::string uri(int i) const { return m_uri_list[i]; }

    const Entity<Loader>& loader() const { return m_loader; }
    void set_loader(const Entity<Loader>& entity) { m_loader = entity; }

    // expressive way of setting loader
    template<typename LoaderType>
    LoadNode* By(const std::string& config) {
        Entity<Loader> loader = Entity<Loader>::Of<LoaderType>(config);
        set_loader(Entity<Loader>::Of<LoaderType>(config));

        PbScope* scope_message = &m_scope->m_message;
        CHECK_EQ(scope_message->type(), PbScope::INPUT);
        *scope_message->mutable_input_scope()->mutable_spliter() = loader.ToProtoMessage();

        return this;
    }

    template<typename LoaderType>
    LoadNode* By() { return this->By<LoaderType>(""); }

    // expressive way to set objector
    template<typename ObjectorType>
    LoadNode* As(const std::string& config) {
        Entity<Objector> objector = Entity<Objector>::Of<ObjectorType>(config);
        set_objector(objector);

        return this;
    }

    template<typename ObjectorType>
    LoadNode* As() { return this->As<ObjectorType>(""); }

private:
    LoadNode(const std::vector<std::string>& uri_list, const Scope* scope, LogicalPlan* plan);
    virtual void SetSpecificField(PbLogicalPlanNode* message) const;

private:
    std::vector<std::string> m_uri_list;
    Entity<Loader> m_loader;
};

class LogicalPlan::SinkNode : public LogicalPlan::Node {
    friend class LogicalPlan;
public:
    const Node* from() const { return m_from; }

    const Entity<Sinker>& sinker() const { return m_sinker; }
    void set_sinker(const Entity<Sinker>& entity) { m_sinker = entity; }

    // expressive way of setting loader
    template<typename SinkerType>
    SinkNode* By(const std::string& config) {
        set_sinker(Entity<Sinker>::Of<SinkerType>(config));
        return this;
    }

    template<typename SinkerType>
    SinkNode* By() { return this->By<SinkerType>(""); }

private:
    SinkNode(const Node* from, const Scope* scope, LogicalPlan* plan);
    virtual void SetSpecificField(PbLogicalPlanNode* message) const;

private:
    const Node* m_from;
    Entity<Sinker> m_sinker;
};

// An ProcessNode must belongs to an existing scope. ProcessNode may have multiple
// inputs, each input must come from an node whose scope can be covered this ProcessNode.
// Each input has two properities:
//  is_partial:  tell physical planner that the node can be fold into source's scope in
//               excuation.
//  is_prepared: tell flume runtime that the whole input should be loaded into memory
//               before processing of all other non-prepared inputs.
class LogicalPlan::ProcessNode : public LogicalPlan::Node {
    friend class LogicalPlan;
public:
    class Input {
    public:
        explicit Input(const Node* from, ProcessNode* node)
                : m_from(from), m_node(node), m_is_partial(false), m_is_prepared(false) {}

        const Node* from() const { return m_from; }

        bool is_partial() const { return m_is_partial; }
        void set_is_partial(bool value) { m_is_partial = value; }

        bool is_prepared() const { return m_is_prepared; }
        void set_is_prepared(bool value) { m_is_prepared = value; }

        // sibling inputs
        int input_size() const { return m_node->input_size(); }
        Input* input(int i) const { return m_node->input(i); }

        // tricks for enable using expression chain to construct ProcessNode
        ProcessNode* done() const {
            return m_node;
        }

        // expressive methods
        Input* AllowPartialProcessing() {
            set_is_partial(true);
            return this;
        }

        Input* PrepareBeforeProcessing() {
            set_is_prepared(true);
            return this;
        }

        PbProcessNode::Input ToProtoMessage() const;

    private:
        const Node* m_from;
        ProcessNode* m_node;
        bool m_is_partial;
        bool m_is_prepared;
    };

public:
    const Entity<Processor>& processor() const { return m_processor; }
    void set_processor(const Entity<Processor>& entity) { m_processor = entity; }

    int input_size() const { return m_inputs.size(); }
    Input* input(int i) const { return const_cast<Input*>(&m_inputs[i]); }

    int least_prepared_inputs() const { return m_least_prepared_inputs; }
    void set_least_prepared_inputs(int value) { m_least_prepared_inputs = value; }

    bool is_ignore_group() const { return m_is_ignore_group; }
    void set_is_ignore_group(bool value) { m_is_ignore_group = value; }

    int effective_key_num() const { return m_effective_key_num; }
    void set_effective_key_num(int value) { m_effective_key_num = value; }

    bool allow_massive_instance() const { return m_allow_massive_instance; }
    void set_allow_massive_instance(bool value) { m_allow_massive_instance = value; }

    // expressive way of setting processor
    template<typename ProcessorType>
    ProcessNode* By(const std::string& config) {
        set_processor(Entity<Processor>::Of<ProcessorType>(config));
        return this;
    }

    template<typename ProcessorType>
    ProcessNode* By() { return this->By<ProcessorType>(""); }

    // expressive way to set objector
    template<typename ObjectorType>
    ProcessNode* As(const std::string& config) {
        set_objector(Entity<Objector>::Of<ObjectorType>(config));
        return this;
    }

    template<typename ObjectorType>
    ProcessNode* As() { return this->As<ObjectorType>(""); }

    ProcessNode* PrepareAtLeast(int number) {
        set_least_prepared_inputs(number);
        return this;
    }

    ProcessNode* IgnoreGroup() {
        set_is_ignore_group(true);
        return this;
    }

    ProcessNode* AllowMassiveInstance() {
        set_allow_massive_instance(true);
        return this;
    }

    ProcessNode* EffectiveKeyNum(int number) {
        set_effective_key_num(number);
        return this;
    }

private:
    ProcessNode(const std::vector<const Node*>& from, const Scope* scope, LogicalPlan* plan);
    virtual void SetSpecificField(PbLogicalPlanNode* message) const;

private:
    std::vector<Input> m_inputs;
    Entity<Processor> m_processor;
    int32_t m_least_prepared_inputs;
    bool m_is_ignore_group;
    int32_t m_effective_key_num;
    bool m_allow_massive_instance;
};

// Unify multiple nodes into a single logical node. Can also used to create a virtual node
// with different scope than sources.
class LogicalPlan::UnionNode : public LogicalPlan::Node {
    friend class LogicalPlan;
public:
    int from_size() const { return m_from.size(); }
    const Node* from(int i) const { return m_from[i]; }

    // expressive way to set objector
    template<typename ObjectorType>
    UnionNode* As(const std::string& config) {
        set_objector(Entity<Objector>::Of<ObjectorType>(config));
        return this;
    }

    template<typename ObjectorType>
    UnionNode* As() { return this->As<ObjectorType>(""); }

private:
    UnionNode(const std::vector<const Node*>& from, const Scope* scope, LogicalPlan* plan);
    virtual void SetSpecificField(PbLogicalPlanNode* message) const;

private:
    std::vector<const Node*> m_from;
};

// A ShuffleNode represents an virtual input of corresponding scope.
class LogicalPlan::ShuffleNode : public LogicalPlan::Node {
    friend class LogicalPlan;
public:
    const Node* from() const { return m_from; }

    PbShuffleNode::Type shuffle_type() const { return m_shuffle_type; }
    void set_shuffle_type(PbShuffleNode::Type value) { m_shuffle_type = value; }

    // every line is in a different group
    void set_distribute_every(bool value = true) {
        PbScope* scope_message = &m_scope->m_message;
        scope_message->set_distribute_every(value);
    }

    const Entity<KeyReader>& key_reader() const { return m_key_reader; }
    void set_key_reader(const Entity<KeyReader>& entity) { m_key_reader = entity; }

    const Entity<Partitioner>& partitioner() const { return m_partitioner; }
    void set_partitioner(const Entity<Partitioner>& entity) { m_partitioner = entity; }

    const Entity<TimeReader>& time_reader() const { return m_time_reader; }
    void set_time_reader(const Entity<TimeReader>& entity) { m_time_reader = entity; }

    // sibling nodes
    int node_size() const { return m_group->node_size(); }
    ShuffleNode* node(int i) const { return m_group->node(i); }

    // tricks for enable using expression chain to construct ProcessNode
    ShuffleGroup* done() const {
        return m_group;
    }

    // expressive method to set shuffle_type to BROADCAST
    ShuffleNode* MatchAny() {
        set_scope_type(PbScope::GROUP);
        set_shuffle_type(PbShuffleNode::BROADCAST);
        return this;
    }

    // expressive method to set shuffle_type to KEY
    template<typename KeyReaderType>
    ShuffleNode* MatchBy(const std::string& config) {
        set_scope_type(PbScope::GROUP);
        set_shuffle_type(PbShuffleNode::KEY);
        set_key_reader(Entity<KeyReader>::Of<KeyReaderType>(config));
        if (m_from->is_infinite()) {
            set_infinite();
            m_group->scope()->m_message.set_is_infinite(true);
            m_group->scope()->m_message.set_is_stream(true);
        }
        return this;
    }

    // expressive method to set shuffle_type to KEY
    template<typename KeyReaderType>
    ShuffleNode* MatchBy() {
        return this->template MatchBy<KeyReaderType>("");
    }

    // distribute all records to every buckets
    // expressive method to set shuffle_type to BROADCAST
    ShuffleNode* DistributeAll() {
        set_scope_type(PbScope::BUCKET);
        set_shuffle_type(PbShuffleNode::BROADCAST);
        return this;
    }

    // distribute each record to arbitrary bucket
    // expressive method to set shuffle_type to SEQUENCE
    ShuffleNode* DistributeByDefault() {
        set_scope_type(PbScope::BUCKET);
        set_shuffle_type(PbShuffleNode::SEQUENCE);
        if (m_from->is_infinite()) {
            set_infinite();
            m_group->scope()->m_message.set_is_infinite(true);
            m_group->scope()->m_message.set_is_stream(true);
        }
        return this;
    }

    // distribute each record to a differenct bucket
    ShuffleNode* DistributeEvery() {
        set_scope_type(PbScope::BUCKET);
        set_shuffle_type(PbShuffleNode::SEQUENCE);
        set_distribute_every(true);
        return this;
    }

    // expressive method to set shuffle_type to SEQUENCE
    template<typename PartitionerType>
    ShuffleNode* DistributeBy(const std::string& config) {
        set_scope_type(PbScope::BUCKET);
        set_shuffle_type(PbShuffleNode::SEQUENCE);
        set_partitioner(Entity<Partitioner>::Of<PartitionerType>(config));
        if (m_from->is_infinite()) {
            set_infinite();
            m_group->scope()->m_message.set_is_infinite(true);
            m_group->scope()->m_message.set_is_stream(true);
        }
        return this;
    }

    // expressive method to set shuffle_type to SEQUENCE
    template<typename PartitionerType>
    ShuffleNode* DistributeBy() {
        return this->template DistributeBy<PartitionerType>("");
    }

    // distribute as batch
    ShuffleNode* DistributeAsBatch() {
        set_scope_type(PbScope::BUCKET);
        set_shuffle_type(PbShuffleNode::SEQUENCE);
        if (m_from->is_infinite()) {
            m_group->scope()->m_message.set_is_infinite(true);
        }
        return this;
    }

    template<typename TimeReaderType, typename WindowType>
    ShuffleNode* WindowBy(const std::string& config) {
        set_scope_type(PbScope::WINDOW);
        set_shuffle_type(PbShuffleNode::WINDOW);
        set_time_reader(Entity<TimeReader>::Of<TimeReaderType>(config));
        return this;
    }

    template<typename TimeReaderType, typename WindowType>
    ShuffleNode* WindowBy() {
        return this->template WindowBy<TimeReaderType, WindowType>("");
    }

private:
    void set_scope_type(PbScope::Type type) {
        PbScope* scope_message = &m_scope->m_message;
        CHECK(scope_message->type() == PbScope::DEFAULT || scope_message->type() == type);
        scope_message->set_type(type);
    }

    ShuffleNode(const Node* from, ShuffleGroup* group, LogicalPlan* plan);
    virtual void SetSpecificField(PbLogicalPlanNode* message) const;

private:
    const Node* m_from;
    ShuffleGroup* m_group;
    PbShuffleNode::Type m_shuffle_type;
    Entity<KeyReader> m_key_reader;
    Entity<Partitioner> m_partitioner;
    Entity<TimeReader> m_time_reader;
};

// Template function definitions.
template<typename SinkerType>
inline LogicalPlan::SinkNode* LogicalPlan::Node::SinkBy(const std::string& config) {
    return m_plan->Sink(this->scope(), this)->template By<SinkerType>(config);
}

template<typename ProcessorType>
inline LogicalPlan::ProcessNode* LogicalPlan::Node::ProcessBy(const std::string& config) {
    return m_plan->Process(this)->template By<ProcessorType>(config);
}

template<typename KeyReaderType>
inline LogicalPlan::Node* LogicalPlan::Node::SortBy(const std::string& config) {
    // shuffle will return node with nested scope. if we only need to sort, we should
    // return to source's scope
    return m_plan->Shuffle(this->scope(), this)->Sort()
            ->node(0)->template MatchBy<KeyReaderType>(config)->LeaveScope();
}

template<typename KeyReaderType>
inline LogicalPlan::Node* LogicalPlan::Node::GroupBy(const std::string& config) {
    ShuffleGroup* shuffle_group = m_plan->Shuffle(this->scope(), this);
    return shuffle_group->node(0)->template MatchBy<KeyReaderType>(config);
}


template<typename KeyReaderType>
inline LogicalPlan::Node* LogicalPlan::Node::SortAndGroupBy(const std::string& config) {
    return m_plan->Shuffle(this->scope(), this)->Sort()
            ->node(0)->template MatchBy<KeyReaderType>(config);
}

inline LogicalPlan::Node* LogicalPlan::Node::DistributeInto(uint32_t bucket_number) {
    return m_plan->Shuffle(this->scope(), this)->WithConcurrency(bucket_number)
        ->node(0)->DistributeByDefault();
}

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_LOGICAL_PLAN_H_
