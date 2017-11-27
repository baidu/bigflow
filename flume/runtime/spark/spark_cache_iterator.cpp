/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

//
// Created by zhangyuncong on 2017/9/19.
//

#include "spark_cache_iterator.h"

#include <utility>
#include <exception>

#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

struct CacheRecord {
    CacheRecord() {}
    CacheRecord(const std::vector<std::string>& keys, const std::string& value): keys(keys), value(value) {}

    std::vector<std::string> keys;
    std::string value;
};

class CacheIterator::Impl {
public:
    Impl() {
        Reset();
    }

    bool Next() {
        if (m_pos + 1 == m_records.size()) {
            return false;
        }
        ++m_pos;
        m_keys.resize(m_records[m_pos].keys.size());
        for (size_t i = 0; i != m_records[m_pos].keys.size(); ++i) {
            m_keys[i].set(m_records[m_pos].keys[i]);
        }
        return true;
    }

    void Reset() {
        m_pos = -1;
    }

    void Done() {
    }

    const std::vector<toft::StringPiece>& Keys() const {
        return m_keys;
    }

    void* Value() const {
        CHECK(false) << "not supported yet";
    }

    void Put(const std::vector<std::string>& keys, const std::string& value) {
        m_records.emplace_back(keys, value);
    }

    toft::StringPiece ValueStr() const {
        return toft::StringPiece(m_records[m_pos].value);
    }

private:

    std::vector<toft::StringPiece> m_keys;
    toft::StringPiece m_value;

    std::vector<CacheRecord> m_records;
    int32_t m_pos;
};

CacheIterator::CacheIterator() : m_impl(new Impl){
}

bool CacheIterator::Next() {
    return m_impl->Next();
}

void CacheIterator::Reset(){
    m_impl->Reset();
}

void CacheIterator::Done() {
    m_impl->Done();
}

const std::vector<toft::StringPiece>& CacheIterator::Keys() const {
    return m_impl->Keys();
}

void* CacheIterator::Value() const {
    return m_impl->Value();
}

toft::StringPiece CacheIterator::ValueStr() const {
    return m_impl->ValueStr();
}

void CacheIterator::Put(const std::vector<std::string>& keys, const std::string& value) {
    return m_impl->Put(keys, value);
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
