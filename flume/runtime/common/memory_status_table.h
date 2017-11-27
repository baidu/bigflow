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
// Interface.

#ifndef FLUME_RUNTIME_COMMON_MEMORY_STATUS_TABLE_H_
#define FLUME_RUNTIME_COMMON_MEMORY_STATUS_TABLE_H_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "boost/foreach.hpp"
#include "boost/optional.hpp"
#include "boost/unordered_map.hpp"

#include "flume/proto/logical_plan.pb.h"
#include "flume/runtime/status_table.h"

namespace baidu {
namespace flume {
namespace runtime {

class MemoryStatusTable : public StatusTable {
public:
    virtual ~MemoryStatusTable() {}

    template<typename ScopeList, typename NodeList>
    void Initialize(const ScopeList& scopes, const NodeList& nodes);

    virtual StatusTable::ScopeVisitor* GetScopeVisitor(const std::string& id,
                                                       const std::vector<toft::StringPiece>& keys);

    virtual StatusTable::NodeVisitor* GetNodeVisitor(const std::string& id,
                                                     const std::vector<toft::StringPiece>& keys);

private:
    typedef std::set<std::string> EntryList;
    typedef std::vector<std::string> Path;

    struct Value {
        enum {
            EMPTY,
            NORMAL,
            INVALID,
        } status;

        std::string value;

        Value() : status(EMPTY) {}
    };

    struct ScopeInfo {
        PbScope message;
        ScopeInfo* father;
        std::map<Path, EntryList> entries;

        ScopeInfo() : father(NULL) {}
    };

    struct NodeInfo {
        ScopeInfo* scope;
        std::map<Path, Value> values;

        NodeInfo() : scope(NULL) {}
    };

    class ScopeVisitorImpl;
    class NodeVisitorImpl;

    Path ToPath(const std::vector<toft::StringPiece>& keys);

    std::map<std::string, ScopeInfo> m_scope_meta;
    std::map<std::string, NodeInfo> m_node_meta;
};

template<typename ScopeList, typename NodeList>
void MemoryStatusTable::Initialize(const ScopeList& scopes, const NodeList& nodes) {
    for (typename ScopeList::const_iterator ptr = scopes.begin(); ptr != scopes.end(); ++ptr) {
        ScopeInfo& meta = m_scope_meta[ptr->id()];
        meta.message = *ptr;
        meta.father = &m_scope_meta[ptr->father()];
    }

    for (typename NodeList::const_iterator ptr = nodes.begin(); ptr != nodes.end(); ++ptr) {
        NodeInfo& meta = m_node_meta[ptr->id()];
        meta.scope = &m_scope_meta[ptr->scope()];
    }
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_MEMORY_STATUS_TABLE_H_
