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

#include <netinet/in.h>

#include <string>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "boost/scoped_ptr.hpp"

#include "flume/runtime/common/memory_status_table.h"
#include "flume/runtime/util/iterator.h"

namespace baidu {
namespace flume {
namespace runtime {

class MemoryStatusTable::ScopeVisitorImpl : public StatusTable::ScopeVisitor {
public:
    explicit ScopeVisitorImpl(const EntryList& entries) : m_entries(entries) {}
    virtual ~ScopeVisitorImpl() {}

    virtual StatusTable::Iterator* ListEntries() {
        m_deleter.push_back(new internal::StlIterator<EntryList>(m_entries));
        return &m_deleter.back();
    }

    virtual void Release() {
        delete this;
    }

private:
    const EntryList& m_entries;
    boost::ptr_vector<StatusTable::Iterator> m_deleter;
};

class MemoryStatusTable::NodeVisitorImpl : public StatusTable::NodeVisitor {
public:
    explicit NodeVisitorImpl(Value* ptr) : m_ptr(ptr) {}

    virtual bool IsValid() {
        return m_ptr->status != Value::INVALID;
    }

    // return true if value is loaded successfully
    virtual bool Read(std::string* value) {
        if (m_ptr->status != Value::NORMAL) {
            return false;
        }

        *value = m_ptr->value;
        return true;
    }

    virtual void Update(const std::string& value) {
        CHECK_NE(m_ptr->status, Value::INVALID);
        m_ptr->status = Value::NORMAL;
        m_ptr->value = value;
    }

    virtual void Invalidate() {
        m_ptr->status = Value::INVALID;
    }

    virtual void Release() {
        delete this;
    }

private:
    Value* m_ptr;
};

StatusTable::ScopeVisitor*
MemoryStatusTable::GetScopeVisitor(const std::string& id,
                                   const std::vector<toft::StringPiece>& keys) {
    ScopeInfo& info = m_scope_meta[id];
    return new ScopeVisitorImpl(info.entries[ToPath(keys)]);
}

StatusTable::StatusTable::NodeVisitor*
MemoryStatusTable::GetNodeVisitor(const std::string& id,
                                  const std::vector<toft::StringPiece>& keys) {
    NodeInfo& node = m_node_meta[id];
    Path path = ToPath(keys);
    std::auto_ptr<NodeVisitorImpl> visitor(new NodeVisitorImpl(&node.values[path]));

    ScopeInfo* scope = node.scope;
    while (scope != NULL && !path.empty()) {
        std::string entry = path.back();
        path.pop_back();

        scope->entries[path].insert(entry);
        scope = scope->father;
    }

    return visitor.release();
}

inline MemoryStatusTable::Path
MemoryStatusTable::ToPath(const std::vector<toft::StringPiece>& keys) {
    Path path;
    for (size_t i = 0; i < keys.size(); ++i) {
        path.push_back(keys[i].as_string());
    }
    return path;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
