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

#ifndef FLUME_RUNTIME_TESTING_MOCK_STATUS_TABLE_H_
#define FLUME_RUNTIME_TESTING_MOCK_STATUS_TABLE_H_

#include <list>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/string/string_piece.h"

#include "flume/runtime/status_table.h"

namespace baidu {
namespace flume {
namespace runtime {

class MockScopeVisitor : public StatusTable::ScopeVisitor {
public:
    static const toft::StringPiece kEmptyArg;

    MOCK_METHOD0(ListEntries, StatusTable::Iterator* ());  // NOLINT
    MOCK_METHOD0(Release, void ());  // NOLINT

    StatusTable::Iterator* MakeIterator(const toft::StringPiece& arg0 = kEmptyArg,
                           const toft::StringPiece& arg1 = kEmptyArg,
                           const toft::StringPiece& arg2 = kEmptyArg,
                           const toft::StringPiece& arg3 = kEmptyArg) {
        toft::StringPiece args[] = {arg0, arg1, arg2, arg3};

        std::vector<toft::StringPiece> entries;
        for (size_t i = 0; i < TOFT_ARRAY_SIZE(args); ++i) {
            if (args[i].empty()) {
                break;
            }
            entries.push_back(args[i]);
        }

        m_iterators.push_back(new IteratorImpl(entries));
        return &m_iterators.back();
    }


private:
    class IteratorImpl : public StatusTable::Iterator {
    public:
        explicit IteratorImpl(const std::vector<toft::StringPiece>& args)
                : m_entries(args), m_ptr(m_entries.begin()) {}

        virtual bool HasNext() const {
            return m_ptr != m_entries.end();
        }

        virtual toft::StringPiece NextValue() {
            return *m_ptr++;
        }

        virtual void Reset() {
            m_ptr = m_entries.begin();
        }

        virtual void Done() {
            m_entries.clear();
            m_ptr = m_entries.end();
        }

    private:
        std::vector<toft::StringPiece> m_entries;
        std::vector<toft::StringPiece>::const_iterator m_ptr;
    };

    boost::ptr_vector<StatusTable::Iterator> m_iterators;
};

const toft::StringPiece MockScopeVisitor::kEmptyArg;

class MockNodeVisitor : public StatusTable::NodeVisitor {
public:
    MOCK_METHOD0(IsValid, bool ());  // NOLINT
    MOCK_METHOD1(Read, bool (std::string*));  // NOLINT
    MOCK_METHOD1(Update, void (const std::string&));  // NOLINT
    MOCK_METHOD0(Invalidate, void ());  // NOLINT
    MOCK_METHOD0(Release, void ());  // NOLINT
};

class MockStatusTable : public StatusTable {
public:
    MockStatusTable() {
        using ::testing::_;
        using ::testing::Return;

        ON_CALL(*this, GetScopeVisitor(_, _)).WillByDefault(Return(&m_scope_visitor));
        ON_CALL(*this, GetNodeVisitor(_, _)).WillByDefault(Return(&m_node_visitor));
    }

    MOCK_METHOD2(GetScopeVisitor,
                 ScopeVisitor* (const std::string&, const std::vector<std::string>&));

    MOCK_METHOD2(GetNodeVisitor,
                 NodeVisitor* (const std::string&, const std::vector<std::string>&));

    MockScopeVisitor& scope_visitor() { return m_scope_visitor; }

    MockNodeVisitor& node_visitor() { return m_node_visitor; }

private:
    std::vector<std::string> ToStringList(const std::vector<toft::StringPiece>& keys) {
        std::vector<std::string> results;
        for (size_t i = 0; i < keys.size(); ++i) {
            results.push_back(keys[i].as_string());
        }
        return results;
    }

    virtual ScopeVisitor* GetScopeVisitor(const std::string& id,
                                          const std::vector<toft::StringPiece>& keys) {
        return GetScopeVisitor(id, ToStringList(keys));
    }


    virtual NodeVisitor* GetNodeVisitor(const std::string& id,
                                        const std::vector<toft::StringPiece>& keys) {
        return GetNodeVisitor(id, ToStringList(keys));
    }

    MockScopeVisitor m_scope_visitor;
    MockNodeVisitor m_node_visitor;
};



}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_MOCK_STATUS_TABLE_H_
