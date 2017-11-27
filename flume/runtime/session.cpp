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
// Author: Wang Cong <wangcong09@baidu.com>

#include "flume/runtime/session.h"

#include "boost/foreach.hpp"
#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace runtime {

// TODO Refactor Session class
Session::Session() {
}

Session::~Session() {
}

const Session::NodeIdCollection& Session::GetSunkNodeIds() const {
    return m_sunk_node_ids;
}

const Session::NodeIdCollection& Session::GetCachedNodeIds() const {
    return m_cached_node_ids;
}

void Session::AddSunkNodeId(const std::string& node_id) {
    m_sunk_node_ids.insert(node_id);
}

void Session::AddCachedNodeId(const std::string& node_id) {
    m_cached_node_ids.insert(node_id);
}

bool Session::IsCachedNodeIn(const std::string &node_id) {
    return m_cached_node_ids.find(node_id) != m_cached_node_ids.end();
}

std::string Session::GetCachePathFromId(const std::string& node_id) const {
    CHECK(m_cached_node_ids.find(node_id) != m_cached_node_ids.end())
        << "Cannot find node ID [" << node_id << "] in current session";
    IdToPath::const_iterator it = m_id_to_path.find(node_id);
    CHECK(it != m_id_to_path.end())
        << "Cannot find path for node ID [" << node_id << "] in current session";
    return it->second;
}

void Session::SetCachePathForId(const std::string& node_id, const std::string& path) {
    CHECK(m_cached_node_ids.find(node_id) != m_cached_node_ids.end())
        << "Cannot find node ID [" << node_id << "] in current session";

    typedef std::pair<std::string, std::string> StringPair;
    typedef std::pair<IdToPath::iterator, bool> RetType;

    RetType ret = m_id_to_path.insert(StringPair(node_id, path));
    CHECK(ret.second || path == GetCachePathFromId(node_id))
        << "Failed: trying to replace an existing path for node_id: ["
        << node_id
        << "] with a new path: ["
        << path
        << "].";
}

void Session::ResetAll() {
    m_sunk_node_ids.clear();
    m_cached_node_ids.clear();
    m_id_to_path.clear();
}

Session* Session::Clone() const {
    Session* cloned = new Session();

    BOOST_FOREACH(const std::string& node_id, m_sunk_node_ids) {
        cloned->AddSunkNodeId(node_id);
    }
    BOOST_FOREACH(const std::string& node_id, m_cached_node_ids) {
        cloned->AddCachedNodeId(node_id);
    }
    BOOST_FOREACH(const IdToPath::value_type& string_pair, m_id_to_path) {
        cloned->SetCachePathForId(string_pair.first, string_pair.second);
    }
    cloned->m_nodes_map = m_nodes_map;
    return cloned;
}

const Session::NodesMap& Session::GetNodesMap() const {
    return m_nodes_map;
}

Session::NodesMap& Session::GetNodesMap() {
    return m_nodes_map;
}

/*void Session::Merge(const Session& other) {
    const Session::NodeIdCollection& merge_cached_ids = other.GetCachedNodeIds();
    m_cached_node_ids.insert(merge_cached_ids.begin(), merge_cached_ids.end());

    const Session::NodeIdCollection& merge_sunk_ids = other.GetSunkNodeIds();
    m_sunk_node_ids.insert(merge_sunk_ids.begin(), merge_sunk_ids.end());

    BOOST_FOREACH(const std::string& id, merge_cached_ids) {
        SetCachePath(id, other.GetCachePath(id));
    }
}*/

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
