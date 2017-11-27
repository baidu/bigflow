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

#ifndef FLUME_RUNTIME_TESTING_MOCK_DATASET_H_
#define FLUME_RUNTIME_TESTING_MOCK_DATASET_H_

#include <list>
#include <string>

#include "boost/ptr_container/ptr_vector.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/string/string_piece.h"

#include "flume/runtime/dataset.h"
#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace runtime {

class MockDatasetManager : public DatasetManager {
public:
    MOCK_METHOD1(GetDataset, Dataset* (const std::string&));
};

class MockDataset : public Dataset {
public:
    explicit MockDataset(uint32_t scope_level)
            : m_scope_level(scope_level), m_is_ready(false) {
        using ::testing::Invoke;

        ON_CALL(*this, Commit())
            .WillByDefault(Invoke(this, &MockDataset::DelegateCommit));

        ON_CALL(*this, Discard())
            .WillByDefault(Invoke(this, &MockDataset::DelegateDiscard));

        ON_CALL(*this, NewIterator())
            .WillByDefault(Invoke(this, &MockDataset::DelegateNewIterator));
    }

    MockDataset& SetIsReady() {
        m_is_ready = true;
        return *this;
    }

    MockDataset& AddData(const std::string& data) {
        m_datas.push_back(data);
        return *this;
    }

    MOCK_METHOD0(Commit, void ());  // NOLINT
    MOCK_METHOD0(Discard, Iterator* ());  // NOLINT
    MOCK_METHOD0(NewIterator, Iterator* ());  // NOLINT

    MOCK_METHOD1(GetChild, Dataset* (const toft::StringPiece&));  // NOLINT
    MOCK_METHOD1(Emit, void (const toft::StringPiece&));  // NOLINT

    MOCK_METHOD0(Release, void ());  // NOLINT

    virtual uint32_t GetScopeLevel() {
        return m_scope_level;
    }

    // IsReady means this Dataset is readable and immutable.
    virtual bool IsReady() {
        return m_is_ready;
    }

    virtual util::Arena* AcquireArena() {
        return &m_arena;
    }

    virtual void ReleaseArena() {
    }

    virtual void AddReference() {
        LOG(FATAL) << "MockDataset can not increase reference.";
    }

private:
    class IteratorImpl : public Iterator {
    public:
        typedef std::list<std::string>::iterator PtrType;

        IteratorImpl(PtrType begin,  PtrType end)
                : m_begin(begin), m_end(end), m_ptr(begin), m_is_done(false) {}

        ~IteratorImpl() {
            CHECK(m_is_done);
        }

        virtual bool HasNext() const {
            CHECK(!m_is_done);
            return m_ptr != m_end;
        }

        virtual toft::StringPiece NextValue() {
            CHECK(!m_is_done);
            return *m_ptr++;
        }

        virtual void Reset() {
            CHECK(!m_is_done);
            m_ptr = m_begin;
        }

        virtual void Done() {
            m_is_done = true;
        }

    private:
        PtrType m_begin;
        PtrType m_end;
        PtrType m_ptr;

        bool m_is_done;
    };

protected:
    void DelegateCommit() {
        CHECK_EQ(false, m_is_ready);
        m_is_ready = true;
    }

    Iterator* DelegateDiscard() {
        m_iterators.push_back(new IteratorImpl(m_datas.begin(), m_datas.end()));
        return &m_iterators.back();
    }

    Iterator* DelegateNewIterator() {
        m_iterators.push_back(new IteratorImpl(m_datas.begin(), m_datas.end()));
        return &m_iterators.back();
    }


private:
    util::Arena m_arena;
    uint32_t m_scope_level;
    bool m_is_ready;

    std::list<std::string> m_datas;
    boost::ptr_vector<IteratorImpl> m_iterators;
};

std::list<std::string> ToList(Dataset* dataset) {
    std::list<std::string> result;

    Dataset::Iterator* iterator = dataset->NewIterator();
    while (iterator->HasNext()) {
        result.push_back(iterator->NextValue().as_string());
    }
    iterator->Done();

    return result;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_MOCK_DATASET_H_
