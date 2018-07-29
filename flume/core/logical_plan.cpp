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

#include "flume/core/logical_plan.h"

#include <algorithm>
#include <functional>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "toft/base/array_size.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/path/path.h"

#include "flume/core/empty_environment.h"
#include "flume/core/environment.h"
#include "flume/flags.h"
#include "flume/planner/local/local_planner.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/local/local_backend.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/counter.h"
#include "flume/runtime/local/local_executor_factory.h"
#include "flume/runtime/resource.h"
#include "flume/runtime/session.h"
#include "flume/runtime/task.h"
#include "flume/util/config_util.h"

namespace baidu {
namespace flume {
namespace core {

using flume::core::EmptyEnvironment;

LogicalPlan::LogicalPlan() {
    m_scopes.push_back(new Scope(NULL, this));
}

LogicalPlan::UnionNode* LogicalPlan::BroadcastTo(const LogicalPlan::Node* source_node,
                                                 const LogicalPlan::Scope* target_scope) {
    const Scope* source_scope = source_node->scope();
    std::vector<const LogicalPlan::Scope*> scopes;
    scopes.push_back(source_scope);
    scopes.push_back(target_scope);
    const Scope* common_scope = Scope::CommonScope(scopes);

    CHECK(target_scope != common_scope || target_scope == source_node->scope())
        << "Up-forward broadcast is forbidden.";

    scopes.clear();
    for (const Scope* scope = target_scope;
        scope != NULL && scope != common_scope;
        scope = scope->father()) {
        CHECK_NOTNULL(scope);
        scopes.push_back(scope);
    }

    const Node* current_node = source_node;
    std::vector<const LogicalPlan::Scope*>::reverse_iterator r_it =
        scopes.rbegin();
    for (; r_it != scopes.rend(); ++r_it) {
        current_node = AddShuffleNode(*r_it, current_node);
    }

    std::vector<const Node*> from;
    from.push_back(current_node);
    UnionNode* union_node = new UnionNode(from, target_scope, this);
    m_nodes.push_back(union_node);

    return union_node;
}

PbLogicalPlan LogicalPlan::ToProtoMessage() const {
    PbLogicalPlan message;

    for (size_t i = 0; i < m_nodes.size(); ++i) {
        *message.add_node() = m_nodes[i].ToProtoMessage();
    }

    for (size_t i = 0; i < m_scopes.size(); ++i) {
        *message.add_scope() = m_scopes[i].ToProtoMessage();
    }

    message.CheckInitialized();

    if (!message.has_environment()) {
        PbEntity* env = message.mutable_environment();
        env->set_name(Reflection<flume::core::Environment>::TypeName<EmptyEnvironment>());
    }
    CHECK(message.has_environment()) << "PbLogicalPlan does not have environment.";

    return message;
}

void LogicalPlan::RunLocally() const {
    // PbLogicalPlan logical_plan = this->ToProtoMessage();

    // PbJobConfig job_config;
    // job_config.set_hadoop_client_path(util::DefaultHadoopClientPath());
    // job_config.set_hadoop_config_path(util::DefaultHadoopConfigPath());
    // std::string path =
    //     toft::Path::ToAbsolute(flume::util::DefaultTempLocalPath("test_locally"));
    // job_config.set_tmp_data_path_input(path);
    // job_config.set_tmp_data_path_output(path);
    // PbLocalConfig local_config;
    // local_config.set_hadoop_client_path(util::DefaultHadoopClientPath());
    // local_config.set_hadoop_config_path(util::DefaultHadoopConfigPath());

    // runtime::Session session;
    // planner::local::LocalPlanner planner(&session, &job_config);
    // PbPhysicalPlan physical_plan = planner.Plan(logical_plan);

    // const PbJob& job = physical_plan.job(0);
    // runtime::local::LocalBackend local_backend(local_config); // only to create cache manager
    // runtime::MemoryDatasetManager dataset_manager;
    // runtime::local::LocalExecutorFactory factory(&local_backend);
    // factory.Initialize(job.local_job(), &dataset_manager);

    // runtime::Task task;
    // task.Initialize(job.local_job().task().root(), &factory);
    // task.Run("");
}

LogicalPlan::Status LogicalPlan::Run(runtime::Backend* backend,
                                     runtime::Resource* resource) const {
    return Run(backend, resource, runtime::CounterSession::GlobalCounterSession());
}

LogicalPlan::Status LogicalPlan::Run(runtime::Backend* backend,
                                     runtime::Resource* resource,
                                     runtime::CounterSession* counters) const {
    return backend->Launch(this->ToProtoMessage(), resource, counters);
}

LogicalPlan::ShuffleNode* LogicalPlan::AddShuffleNode(const LogicalPlan::Scope* target_scope,
                                                      const LogicalPlan::Node* source_node) {

    CHECK(target_scope->father() == source_node->scope())
        << "source_node should only belong to target_scope's father";

    // Find ShuffleGroup for Scope
    ShuffleGroup* shuffle_group = NULL;
    for (size_t i = 0; i < m_shuffles.size(); ++i) {
        ShuffleGroup* group = &m_shuffles[i];
        if (group->scope() == target_scope) {
            shuffle_group = group;
            break;
        }
    }
    CHECK(shuffle_group != NULL) << "Unable to find corresponding ShuffleGroup for target_scope:"
                                 << target_scope->identity();

    ShuffleNode* shuffle_node = (new ShuffleNode(source_node, shuffle_group, this))->MatchAny();
    m_nodes.push_back(shuffle_node);

    return shuffle_node;
}

const LogicalPlan::Scope* LogicalPlan::Scope::CommonScope(
        const std::vector<const Scope*>& scopes) {
    CHECK_NE(0u, scopes.size());

    std::vector<std::list<const Scope*> > stacks(scopes.size());
    for (size_t i = 0; i < scopes.size(); ++i) {
        stacks[i] = scopes[i]->ScopeStack();
    }

    const Scope* result = NULL;
    while (true) {
        std::vector<const Scope*> top_scopes;
        for (size_t i = 0; i < stacks.size(); ++i) {
            if (stacks[i].empty()) {
                CHECK_NOTNULL(result);
                return result;
            }

            top_scopes.push_back(stacks[i].back());
            stacks[i].pop_back();
        }

        unsigned count = std::count(top_scopes.begin(), top_scopes.end(), top_scopes.front());
        if (count == top_scopes.size()) {
            result = top_scopes.front();
        } else {
            break;
        }
    }
    CHECK_NOTNULL(result);
    return result;
}

const LogicalPlan::Scope* LogicalPlan::Scope::CommonScope(const std::vector<const Node*>& nodes) {
    std::vector<const Scope*> scopes;
    for (size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i]) {
            scopes.push_back(nodes[i]->scope());
        }
    }
    return CommonScope(scopes);
}

const LogicalPlan::Scope* LogicalPlan::Scope::CommonScope(const Node* n1, const Node* n2,
                                                          const Node* n3, const Node* n4) {
    const Node* params[] = {n1, n2, n3, n4};

    std::vector<const Scope*> scopes;
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (params[i] != NULL) {
            scopes.push_back(params[i]->scope());
        }
    }
    return CommonScope(scopes);
}

LogicalPlan::Scope::Scope(const Scope* father, const LogicalPlan* plan)
        : m_father(father), m_plan(plan) {
    if (m_father != NULL) {
        m_message.set_id(toft::CreateCanonicalUUIDString());
        m_message.set_father(m_father->identity());
    } else {
        m_message.set_id("");
    }
}

bool LogicalPlan::Scope::IsCoverBy(const Scope* scope) const {
    const Scope* ancestor = this;
    while (ancestor != NULL && ancestor != scope) {
        ancestor = ancestor->father();
    }
    return ancestor == scope;
}

std::list<const LogicalPlan::Scope*> LogicalPlan::Scope::ScopeStack() const {
    std::list<const Scope*> ancestors;

    const Scope* scope = this;
    while (scope != NULL) {
        ancestors.push_back(scope);
        scope = scope->father();
    }

    return ancestors;
}

PbScope LogicalPlan::Scope::ToProtoMessage() const {
    if (m_message.type() == PbScope::BUCKET && m_message.has_concurrency()) {
        // TODO(wenxiang): provide separate interface to set bucket size
        m_message.mutable_bucket_scope()->set_bucket_size(m_message.concurrency());
    }
    return m_message;
}

LogicalPlan::Node::Node(const PbLogicalPlanNode::Type& type,
                        const Scope* scope, LogicalPlan* plan)
        : m_type(type), m_scope(scope), m_plan(plan), m_cache(false), m_is_infinite(false) {
    m_id = toft::CreateCanonicalUUIDString();
}

bool LogicalPlan::Node::IfInfinite(const std::vector<const Node*>& from,
                                   const Scope* target_scope) {
    bool result = false;
    for (size_t i = 0; i < from.size(); ++i) {
        if (target_scope == from[i]->scope()) {
            result = result || from[i]->is_infinite();
        } else {
            const Scope* scope = from[i]->scope();
            while (true) {
                CHECK_NOTNULL(scope->father());
                if (target_scope == scope->father()) {
                    result = result || scope->is_infinite();
                    break;
                }
                scope = scope->father();
            }
        }
    }
    return result;
}

bool LogicalPlan::Node::IfInfinite(const Node* n1, const Node* n2, const Scope* target_scope) {
    const Node* params[] = {n1, n2};

    std::vector<const Node*> from;
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (params[i] != NULL) {
            from.push_back(params[i]);
        }
    }
    return IfInfinite(from, target_scope);
}

PbLogicalPlanNode LogicalPlan::Node::ToProtoMessage() const {
    PbLogicalPlanNode message;

    message.set_id(m_id);
    message.set_type(m_type);
    message.set_debug_info(m_debug_info);
    message.set_cache(m_cache);
    message.set_is_infinite(m_is_infinite);
    if (m_objector.empty() && m_type != PbLogicalPlanNode::SINK_NODE) {
        LOG(FATAL) << "non sink node must have objector!";
    }
    if (!m_objector.empty()) {
        *message.mutable_objector() = m_objector.ToProtoMessage();
    }
    message.set_scope(m_scope->identity());
    SetSpecificField(&message);

    return message;
}

LogicalPlan::LoadNode* LogicalPlan::RepeatedlyLoad(const std::vector<std::string>& uri_list) {
    using google::protobuf::RepeatedFieldBackInserter;

    // every load node create a new scope under global scope.
    Scope* scope = new Scope(global_scope(), this);
    m_scopes.push_back(scope);

    PbScope* scope_message = &scope->m_message;
    scope_message->set_type(PbScope::INPUT);
    scope_message->set_is_infinite(true);
    std::copy(uri_list.begin(), uri_list.end(),
              RepeatedFieldBackInserter(scope_message->mutable_input_scope()->mutable_uri()));

    LoadNode* node = new LoadNode(uri_list, scope, this);
    node->set_infinite();
    m_nodes.push_back(node);

    return node;
}

LogicalPlan::LoadNode* LogicalPlan::Load(const std::vector<std::string>& uri_list) {
    using google::protobuf::RepeatedFieldBackInserter;

    // every load node create a new scope under global scope.
    Scope* scope = new Scope(global_scope(), this);
    m_scopes.push_back(scope);

    PbScope* scope_message = &scope->m_message;
    scope_message->set_type(PbScope::INPUT);
    scope_message->set_is_infinite(false);
    std::copy(uri_list.begin(), uri_list.end(),
              RepeatedFieldBackInserter(scope_message->mutable_input_scope()->mutable_uri()));

    LoadNode* node = new LoadNode(uri_list, scope, this);
    m_nodes.push_back(node);

    return node;
}

LogicalPlan::LoadNode* LogicalPlan::Load(const std::string& uri1, const std::string& uri2,
                                         const std::string& uri3, const std::string& uri4) {
    std::string params[] = {uri1, uri2, uri3, uri4};

    std::vector<std::string> uri_list;
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (!params[i].empty()) {
            uri_list.push_back(params[i]);
        }
    }
    return Load(uri_list);
}

LogicalPlan::SinkNode* LogicalPlan::Sink(const Scope* scope, const Node* from) {
    CHECK_NE(PbLogicalPlanNode::SINK_NODE, from->type());
    CHECK(from->scope()->IsCoverBy(scope));

    SinkNode* node = new SinkNode(from, scope, this);
    if (LogicalPlan::Node::IfInfinite(from, NULL, scope)) {
        node->set_infinite();
    }
    m_nodes.push_back(node);
    return node;
}

LogicalPlan::ProcessNode* LogicalPlan::Process(const Scope* scope,
                                               const std::vector<const Node*>& from) {
    CHECK_NE(0u, from.size());

    for (size_t i = 0; i < from.size(); ++i) {
        CHECK(from[i]->scope()->IsCoverBy(scope));
    }

    ProcessNode* node = new ProcessNode(from, scope, this);
    if (LogicalPlan::Node::IfInfinite(from, scope)) {
        node->set_infinite();
    }
    m_nodes.push_back(node);
    return node;
}

LogicalPlan::ProcessNode* LogicalPlan::Process(const Scope* scope,
                                               const Node* n1, const Node* n2,
                                               const Node* n3, const Node* n4) {
    const Node* nodes[] = {n1, n2, n3, n4};

    std::vector<const Node*> from;
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(nodes); ++i) {
        if (nodes[i] != NULL) {
            from.push_back(nodes[i]);
        }
    }

    return Process(scope, from);
}

LogicalPlan::ProcessNode* LogicalPlan::Process(const Node* n1, const Node* n2,
                                               const Node* n3, const Node* n4) {
    return Process(Scope::CommonScope(n1, n2, n3, n4), n1, n2, n3, n4);
}

LogicalPlan::UnionNode* LogicalPlan::Union(const Scope* scope,
                                           const std::vector<const Node*>& from) {
    CHECK_NE(0u, from.size());

    for (size_t i = 0; i < from.size(); ++i) {
        CHECK(from[i]->scope()->IsCoverBy(scope));
    }

    UnionNode* node = new UnionNode(from, scope, this);
    if (LogicalPlan::Node::IfInfinite(from, scope)) {
        node->set_infinite();
    }
    m_nodes.push_back(node);
    return node;
}

LogicalPlan::UnionNode* LogicalPlan::Union(const Scope* scope,
                                           const Node* n1, const Node* n2,
                                           const Node* n3, const Node* n4) {
    const Node* nodes[] = {n1, n2, n3, n4};

    std::vector<const Node*> from;
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(nodes); ++i) {
        if (nodes[i] != NULL) {
            from.push_back(nodes[i]);
        }
    }

    return Union(scope, from);
}

LogicalPlan::UnionNode* LogicalPlan::Union(const Node* n1, const Node* n2,
                                           const Node* n3, const Node* n4) {
    return Union(Scope::CommonScope(n1, n2, n3, n4), n1, n2, n3, n4);
}

LogicalPlan::ShuffleGroup* LogicalPlan::Shuffle(const Scope* scope,
                                                const std::vector<const Node*>& from) {
    CHECK_NE(0u, from.size());

    Scope* shuffle_scope = new Scope(scope, this);
    m_scopes.push_back(shuffle_scope);

    ShuffleGroup *shuffle = new ShuffleGroup(shuffle_scope);
    m_shuffles.push_back(shuffle);

    for (size_t i = 0; i < from.size(); ++i) {
        ShuffleNode* node = new ShuffleNode(from[i], shuffle, this);
        m_nodes.push_back(node);
    }

    return shuffle;
}

LogicalPlan::ShuffleGroup* LogicalPlan::Shuffle(const Scope* scope,
                                                const Node* n1, const Node* n2,
                                                const Node* n3, const Node* n4) {
    const Node* nodes[] = {n1, n2, n3, n4};

    std::vector<const Node*> from;
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(nodes); ++i) {
        if (nodes[i] != NULL) {
            from.push_back(nodes[i]);
        }
    }

    return Shuffle(scope, from);
}

LogicalPlan::Node* LogicalPlan::Node::LeaveScope() {
    CHECK_NOTNULL(m_scope->father());

    UnionNode* node = m_plan->Union(m_scope->father(), this);
    node->set_objector(this->objector());

    return node;
}

LogicalPlan::Node* LogicalPlan::Node::RemoveScope() {
    UnionNode* node = m_plan->Union(m_plan->global_scope(), this);
    node->set_objector(this->objector());
    return node;
}

LogicalPlan::LoadNode::LoadNode(const std::vector<std::string>& uri_list,
                                const Scope* scope, LogicalPlan* plan)
        : Node(PbLogicalPlanNode::LOAD_NODE, scope, plan), m_uri_list(uri_list) {}

void LogicalPlan::LoadNode::SetSpecificField(PbLogicalPlanNode* message) const {
    CHECK(!m_loader.empty());

    PbLoadNode* load_node = message->mutable_load_node();
    std::copy(m_uri_list.begin(), m_uri_list.end(),
              google::protobuf::RepeatedFieldBackInserter(load_node->mutable_uri()));
    *load_node->mutable_loader() = m_loader.ToProtoMessage();
}

LogicalPlan::SinkNode::SinkNode(const Node* from, const Scope* scope, LogicalPlan* plan)
        : Node(PbLogicalPlanNode::SINK_NODE, scope, plan), m_from(from) {}

void LogicalPlan::SinkNode::SetSpecificField(PbLogicalPlanNode* message) const {
    CHECK(!m_sinker.empty());

    PbSinkNode* sink_node = message->mutable_sink_node();
    sink_node->set_from(m_from->identity());
    *sink_node->mutable_sinker() = m_sinker.ToProtoMessage();
}

LogicalPlan::ProcessNode::ProcessNode(const std::vector<const Node*>& from,
                                      const Scope* scope, LogicalPlan* plan)
        : Node(PbLogicalPlanNode::PROCESS_NODE, scope, plan),
        m_least_prepared_inputs(0),
        m_is_ignore_group(false),
        m_effective_key_num(-1),
        m_allow_massive_instance(false) {
    for (size_t i = 0; i < from.size(); ++i) {
        m_inputs.push_back(Input(from[i], this));
    }
}

PbProcessNode::Input LogicalPlan::ProcessNode::Input::ToProtoMessage() const {
    PbProcessNode::Input message;
    message.set_from(m_from->identity());
    message.set_is_partial(m_is_partial);
    message.set_is_prepared(m_is_prepared);
    return message;
}

void LogicalPlan::ProcessNode::SetSpecificField(PbLogicalPlanNode* message) const {
    CHECK(!m_processor.empty());

    PbProcessNode* process_node = message->mutable_process_node();
    for (size_t i = 0; i < m_inputs.size(); ++i) {
        *process_node->add_input() = m_inputs[i].ToProtoMessage();
    }
    *process_node->mutable_processor() = m_processor.ToProtoMessage();
    process_node->set_least_prepared_inputs(m_least_prepared_inputs);
    process_node->set_is_ignore_group(m_is_ignore_group);
    process_node->set_effective_key_num(m_effective_key_num);
}

LogicalPlan::UnionNode::UnionNode(const std::vector<const Node*>& from,
                                  const Scope* scope, LogicalPlan* plan)
        : Node(PbLogicalPlanNode::UNION_NODE, scope, plan), m_from(from) {
    Entity<Objector> objector = from[0]->objector();
    for (size_t i = 1; i < from.size(); ++i) {
        if (from[i]->objector() != objector) {
            LOG(WARNING) << "Union sources with different objectors. "
                         << "User must set objector manually";
            return;
        }
    }
    set_objector(objector);
}

void LogicalPlan::UnionNode::SetSpecificField(PbLogicalPlanNode* message) const {
    PbUnionNode* union_node = message->mutable_union_node();
    for (size_t i = 0; i < m_from.size(); ++i) {
        *union_node->add_from() = m_from[i]->identity();
    }
}

LogicalPlan::ShuffleNode::ShuffleNode(const Node* from,
                                      ShuffleGroup* group, LogicalPlan* plan)
        : Node(PbLogicalPlanNode::SHUFFLE_NODE, group->scope(), plan),
          m_from(from), m_group(group), m_shuffle_type(PbShuffleNode::BROADCAST) {
    set_objector(from->objector());
    group->add_node(this);
}

void LogicalPlan::ShuffleNode::SetSpecificField(PbLogicalPlanNode* message) const {
    PbShuffleNode* shuffle_node = message->mutable_shuffle_node();
    shuffle_node->set_from(m_from->identity());
    shuffle_node->set_type(m_shuffle_type);
    if (m_shuffle_type == PbShuffleNode::KEY) {
        CHECK(!m_key_reader.empty());
        *shuffle_node->mutable_key_reader() = m_key_reader.ToProtoMessage();
    }
    if (m_shuffle_type == PbShuffleNode::SEQUENCE && !m_partitioner.empty()) {
        *shuffle_node->mutable_partitioner() = m_partitioner.ToProtoMessage();
    }

    if (m_shuffle_type == PbShuffleNode::WINDOW && !m_time_reader.empty()) {
        *shuffle_node->mutable_time_reader() = m_time_reader.ToProtoMessage();
    }
}

}  // namespace core
}  // namespace flume
}  // namespace baidu
