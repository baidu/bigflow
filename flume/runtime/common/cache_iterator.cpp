/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)

#include <map>
#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "flume/runtime/common/cache_iterator.h"
#include "flume/runtime/util/object_holder.h"

namespace baidu {
namespace flume {
namespace runtime {

class CacheIterator::Impl {
public:
    Impl(CacheManager::Reader* cache_reader,
                  const flume::core::Entity<flume::core::Objector>& objector)
                  : m_objector_entity(objector),
                  m_objector(NULL),
                  m_cache_reader(cache_reader),
                  m_iterator(NULL),
                  m_value_holder(NULL) {
        Reset();
    }

    virtual ~Impl() {}

    virtual bool Next() {
        while(m_cur_split != m_splits.size()) {
            if (NULL == m_iterator) {
                m_iterator = m_cache_reader->Read(m_splits[m_cur_split]);
                CHECK(NULL != m_iterator) << "read split " << m_splits[m_cur_split] << " failed";
            }
            if (m_iterator->Next()) {
                m_keys = m_iterator->Keys();
                m_value = m_iterator->Value();
                return true;
            }
            ++m_cur_split;
            m_iterator = NULL;
        }
        return false;
    }

    virtual void Reset() {
        m_splits.clear();
        m_cache_reader->GetSplits(&m_splits);
        m_cur_split = 0;
    }

    virtual void Done() {
        if (m_iterator) {
            m_iterator->Done();
        }
        m_splits.clear();
    }

    virtual const std::vector<toft::StringPiece>& Keys() const {
        return m_keys;
    }

    virtual void* Value() const {
        if (m_value_holder.get() == NULL) {
            m_objector.reset(m_objector_entity.CreateAndSetup());
            m_value_holder.reset(new ObjectHolder(m_objector.get()));
        }
        return m_value_holder->Reset(m_value);
    }

    virtual toft::StringPiece ValueStr() const {
        return m_value;
    }

private:
    flume::core::Entity<flume::core::Objector> m_objector_entity;
    mutable toft::scoped_ptr<flume::core::Objector> m_objector;
    CacheManager::Reader* m_cache_reader;
    std::vector<std::string> m_splits;
    size_t m_cur_split;
    CacheManager::Iterator* m_iterator;
    std::vector<toft::StringPiece> m_keys;
    toft::StringPiece m_value;
    mutable toft::scoped_ptr<ObjectHolder> m_value_holder;
};

CacheIterator::CacheIterator(CacheManager::Reader* cache_reader,
                             const flume::core::Entity<flume::core::Objector>& objector)
                            : m_impl(new CacheIterator::Impl(cache_reader, objector)) {
}

bool CacheIterator::Next() {
    return m_impl->Next();
}

void CacheIterator::Reset() {
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

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
