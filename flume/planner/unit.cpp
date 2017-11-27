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

#include "boost/static_assert.hpp"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

static const char* const kTypeStrings[] = {
    "GENERAL",

    "PLAN",
    "JOB",

    "TASK",
    "SCOPE",
    "EXTERNAL_EXECUTOR",
    "LOGICAL_EXECUTOR",
    "SHUFFLE_EXECUTOR",
    "CACHE_WRITER", // flush
    "CACHE_READER", // load
    "PARTIAL_EXECUTOR",
    "CREATE_EMPTY_RECORD_EXECUTOR",
    "FILTER_EMPTY_RECORD_EXECUTOR",

    "STREAM_EXTERNAL_EXECUTOR",
    "STREAM_LOGICAL_EXECUTOR",
    "STREAM_SHUFFLE_EXECUTOR",

    "DUMMY",
    "CHANNEL",
    "LOAD_NODE",
    "SINK_NODE",
    "PROCESS_NODE",
    "UNION_NODE",
    "SHUFFLE_NODE",

    "EMPTY_GROUP_FIXER"
};

BOOST_STATIC_ASSERT(sizeof(kTypeStrings) / sizeof(const char* const) == Unit::TYPE_COUNT);

Unit::Unit(Plan* plan, bool is_leaf)
    : m_is_leaf(is_leaf), m_is_discard(false), m_type(GENERAL),
      m_father(NULL), m_plan(plan) {}

Unit::~Unit() {}

Plan* Unit::plan() {
    return m_plan;
}

Unit* Unit::task() {
    Unit* unit = this;
    while (unit != NULL && unit->type() != Unit::TASK) {
        unit = unit->father();
    }

    return unit;
}

Unit* Unit::clone() {
    Unit* clone_node = m_plan->NewUnit(m_identity, m_is_leaf);
    clone_node->m_type = this->m_type;

    // clone_node->m_infos = this->m_infos.clone();
    for (InfoMap::iterator it = m_infos.begin(); it != m_infos.end(); ++it) {
        const void* id = it->first;
        clone_node->m_infos.insert(id, it->second->clone());
    }

    /*
    std::vector<Unit*> needs = this->direct_needs();
    for (size_t i = 0; i < needs.size(); ++i) {
        m_plan->AddDependency(needs[i], clone_node);
    }
    std::vector<Unit*> users = this->direct_users();
    for (size_t i = 0; i < users.size(); ++i) {
        m_plan->AddDependency(clone_node, users[i]);
    }
    */

    return clone_node;
}

bool Unit::is_descendant_of(Unit* ancestor) {
    Unit* unit = this;
    while (unit != NULL) {
        unit = unit->father();
        if (ancestor == unit) {
            return true;
        }
    }
    return false;
}

bool Unit::is_leaf() const {
    return m_is_leaf;
}

bool Unit::is_discard() const {
    return m_is_discard;
}

Unit::Type Unit::type() const {
    return m_type;
}

void Unit::set_type(Type type) {
    m_type = type;
}

const char* Unit::type_string() const {
    return kTypeStrings[m_type];
}

Unit* Unit::father() const {
    return m_father;
}

bool Unit::empty() const {
    return m_childs.empty();
}

std::size_t Unit::size() const {
    return m_childs.size();
}

Unit::iterator Unit::begin() const {
    return m_childs.begin();
}

Unit::iterator Unit::end() const {
    return m_childs.end();
}

std::string Unit::identity() const {
    return m_identity;
}

void Unit::set_identity(const std::string& identity) {
    m_identity = identity;
}

std::string Unit::change_identity() {
    m_identity = toft::CreateCanonicalUUIDString();
    return m_identity;
}

std::vector<Unit*> Unit::children() const {
    return std::vector<Unit*>(m_childs.begin(), m_childs.end());
}

std::vector<Unit*> Unit::direct_users() const {
    return m_plan->GetDirectUsers(this);
}

std::vector<Unit*> Unit::direct_needs() const {
    return m_plan->GetDirectNeeds(this);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
