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

#include "flume/runtime/common/memory_dataset.h"

#include <algorithm>
#include <iterator>
#include <map>

#include "boost/shared_ptr.hpp"

#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace runtime {

using toft::StringPiece;

namespace {

typedef boost::shared_ptr<util::Arena> SharedArena;
const SharedArena kEmptyArena;

SharedArena NewArena() {
    return SharedArena(new util::Arena());
}

struct DatasetCoreImpl;
typedef boost::shared_ptr<DatasetCoreImpl> DatasetCore;
const DatasetCore kEmptyDataset;

struct DatasetCoreImpl {
    bool is_ready;
    SharedArena arena;
    std::vector<StringPiece> datas;
    std::map<std::string, DatasetCore> childs;
};

DatasetCore NewDatasetCore() {
    DatasetCore result(new DatasetCoreImpl());
    result->is_ready = false;
    return result;
}

class IteratorImpl : public Dataset::Iterator {
public:
    explicit IteratorImpl(const DatasetCore& dataset)
            : m_dataset(dataset), m_ptr(dataset->datas.begin()) {}

    virtual bool HasNext() const {
        return m_ptr != m_dataset->datas.end();
    }

    virtual toft::StringPiece NextValue() {
        return *m_ptr++;
    }

    virtual void Reset() {
        m_ptr = m_dataset->datas.begin();
    }

    virtual void Done() {
        m_dataset.reset();
    }

private:
    DatasetCore m_dataset;
    std::vector<StringPiece>::const_iterator m_ptr;
};

class DatasetImpl : public Dataset {
public:
    explicit DatasetImpl(uint32_t scope_level, const std::string& name,
                         const DatasetCore& father, const DatasetCore& self)
            : m_scope_level(scope_level), m_name(name), m_father(father), m_self(self) {}

    virtual ~DatasetImpl() {}

    virtual uint32_t GetScopeLevel() {
        return m_scope_level;
    }

    virtual bool IsReady() {
        return m_self != kEmptyDataset && m_self->is_ready;
    }

    virtual Iterator* NewIterator() {
        CHECK_NE(m_self, kEmptyDataset) << "This dataset is already discarded!";

        m_iterators.push_back(new IteratorImpl(m_self));
        return &m_iterators.back();
    }

    virtual void Commit() {
        CHECK_NE(m_self, kEmptyDataset) << "This dataset is already discarded!";
        CHECK(!m_self->is_ready) << "This dataset is already committed!";
        m_self->is_ready = true;

        if (m_father != kEmptyDataset && !m_father->is_ready) {
            std::copy(m_self->datas.begin(), m_self->datas.end(),
                      std::back_inserter(m_father->datas));
            m_father->childs[m_name] = m_self;
        }
        m_father.reset();
    }

    virtual Iterator* Discard() {
        CHECK_NE(m_self, kEmptyDataset) << "This dataset is already discarded!";

        IteratorImpl* iterator = new IteratorImpl(m_self);
        m_iterators.push_back(iterator);

        m_self->is_ready = true;
        m_self->childs.clear();
        m_self.reset();

        return iterator;
    }

    virtual Dataset* GetChild(const toft::StringPiece& key) {
        DatasetCore dataset;
        if (m_self != kEmptyDataset) {
            if (m_self->arena == kEmptyArena) {
                m_self->arena = NewArena();
            }

            std::map<std::string, DatasetCore>::iterator ptr =
                    m_self->childs.find(key.as_string());
            if (ptr != m_self->childs.end()) {
                dataset = ptr->second;
            } else {
                dataset = NewDatasetCore();
                dataset->arena = m_self->arena;
            }
        } else {
            dataset = NewDatasetCore();
        }

        return new DatasetImpl(m_scope_level + 1, key.as_string(), m_self, dataset);
    }

    virtual void Emit(const StringPiece& value) {
        CHECK_NE(m_self, kEmptyDataset) << "This dataset is already discarded!";
        m_self->datas.push_back(value);
    }

    virtual util::Arena* AcquireArena() {
        CHECK_NE(m_self, kEmptyDataset) << "This dataset is already discarded!";

        if (m_self->arena == kEmptyArena) {
            m_self->arena = NewArena();
        }
        return m_self->arena.get();
    }

    virtual void ReleaseArena() {
    }

    virtual void Release() {
        delete this;
    }

    virtual void AddReference() {
        LOG(FATAL) << "DatasetImpl can not increase reference.";
    }

private:
    uint32_t m_scope_level;
    const std::string m_name;

    DatasetCore m_father;
    DatasetCore m_self;
    boost::ptr_vector<IteratorImpl> m_iterators;
};

}  // namespace

MemoryDatasetManager::MemoryDatasetManager() {
}

MemoryDatasetManager::~MemoryDatasetManager() {
}

Dataset* MemoryDatasetManager::GetDataset(const std::string& identity) {
    return new DatasetImpl(0,                   // scope level
                           identity,            // name
                           kEmptyDataset,       // no father
                           NewDatasetCore());   // re-allocate an dataset
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
