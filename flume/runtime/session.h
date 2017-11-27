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
//
// Session maintains status changes of LogicalPlan in runtime,
// such as IDs of the nodes be cached/sunk.

#ifndef FLUME_RUNTIME_SESSION_H
#define FLUME_RUNTIME_SESSION_H

#include <set>
#include <map>
#include <string>
#include <utility>

#include "flume/proto/logical_plan.pb.h"
#include "toft/base/uncopyable.h"

namespace baidu {
namespace flume {
namespace runtime {

class Session {
public:
    typedef std::set<std::string> NodeIdCollection;
    typedef std::map<std::string, std::string> IdToPath;
    typedef std::map<std::string, PbLogicalPlanNode> NodesMap;

    Session();
    virtual ~Session();

    // Get IDs of all nodes to be sunk in a run.
    virtual const NodeIdCollection& GetSunkNodeIds() const;

    // Get IDs of all nodes to be cached in a run.
    virtual const NodeIdCollection& GetCachedNodeIds() const;

    // Get Node Proto or Set Node Proto
    virtual const NodesMap& GetNodesMap() const;
    virtual NodesMap& GetNodesMap();

    // Add ID of a node to be sunk in a run.
    virtual void AddSunkNodeId(const std::string& node_id);

    // Add ID of a node to be cached in a run.
    virtual void AddCachedNodeId(const std::string& node_id);

    // Get cache path from a cache node ID
    virtual std::string GetCachePathFromId(const std::string& node_id) const;

    // Set cache path for a cache node ID
    // For each ID, if the path exists already, only same path is accepted.
    virtual void SetCachePathForId(const std::string& node_id, const std::string& path);

    // Clear all kept statuses.
    virtual void ResetAll();

    // Copy
    virtual Session* Clone() const;

    virtual bool IsCachedNodeIn(const std::string &node_id);

    // Merge with another Session.
    // For 'other', all its cached node ids must have cache path been set.
    // virtual void Merge(const Session& other);

private:
    NodeIdCollection m_sunk_node_ids;
    NodeIdCollection m_cached_node_ids;
    IdToPath m_id_to_path;
    NodesMap m_nodes_map;
};

} // namespace runtime
} // namespace flume
} // namespace baidu

#endif  // FLUME_RUNTIME_SESSION_H
