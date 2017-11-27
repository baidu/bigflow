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
// Author: Liu Cheng <liucheng02@baidu.com>
//
// MemoryPieceStore is used to cache small objects, so that we can save time for new/delete
// it is not thread-safe
// Examples can be found in cached_allocator_test.cpp

#ifndef  FLUME_UTIL_CACHED_ALLOCATOR_H_
#define  FLUME_UTIL_CACHED_ALLOCATOR_H_

#include <cstdlib>
#include <memory>

#include "glog/logging.h"
#include "toft/base/singleton.h"

namespace baidu {
namespace flume {

class MemoryPieceStore : public toft::SingletonBase<MemoryPieceStore> {
public:
    friend class toft::SingletonBase<MemoryPieceStore>;
    static const int kMaxCacheByte = 128;  // most objects should be less than 128B
    static const int kMaxCacheCount = 16;  // only cache 16 of them

    void* Allocate(size_t sz) {
        m_allocate_count++;
        if (sz <= static_cast<size_t>(kMaxCacheByte) && m_cached_count[sz] > 0) {
            int count = m_cached_count[sz];
            char* ptr = m_cached_objects[sz][0];
            m_cached_objects[sz][0] = m_cached_objects[sz][count - 1];
            m_cached_count[sz]--;
            return ptr;
        }
        return malloc(sz);
    }

    void Release(void* ptr, size_t sz) {
        if (ptr == NULL) {
            return;
        }
        m_release_count++;
        if (static_cast<char*>(ptr) < m_buffer ||
            static_cast<char*>(ptr) >= (m_buffer + m_length)) {
            free(ptr);
            return;
        }

        DCHECK(sz > 0U && sz <= static_cast<size_t>(kMaxCacheByte)
               && m_cached_count[sz] < kMaxCacheCount);
        int count = m_cached_count[sz];
        m_cached_objects[sz][count] = static_cast<char*>(ptr);
        m_cached_count[sz]++;
    }

    MemoryPieceStore() : m_allocate_count(0), m_release_count(0), m_length(0), m_buffer(NULL) {
        for (int i = 1; i <= kMaxCacheByte; ++i) {
            m_length += i * kMaxCacheCount;
        }
        LOG(INFO) << "total allocate cache size:" << m_length;

        m_buffer = new char[m_length];
        int use_len = 0;
        for (int i = 1; i <= kMaxCacheByte; ++i) {
            for (int j = 0; j < kMaxCacheCount; ++j) {
                m_cached_objects[i][j] = m_buffer + use_len;
                use_len += i;
            }
            m_cached_count[i] = kMaxCacheCount;
        }
        CHECK_EQ(use_len, m_length);
    }

    ~MemoryPieceStore() {
        delete[] m_buffer;
    }

    int64_t allocate_count() const { return m_allocate_count; }
    int64_t release_count() const { return m_release_count; }

private:
    int64_t m_allocate_count;
    int64_t m_release_count;
    int m_cached_count[kMaxCacheByte + 1];
    char* m_cached_objects[kMaxCacheByte + 1][kMaxCacheCount];
    int m_length;
    char* m_buffer;
};

template<class T>
class CachedAllocator {
public:
    void* operator new(size_t sz);
    void operator delete(void* p, size_t sz);
    virtual ~CachedAllocator() {}
};

template<class T>
void* CachedAllocator<T>::operator new(size_t sz) {
    DCHECK_EQ(sizeof(T), sz);
    return MemoryPieceStore::Instance()->Allocate(sz);
}

template<class T>
void CachedAllocator<T>::operator delete(void* p, size_t sz) {
    DCHECK_EQ(sizeof(T), sz);
    if(p == NULL) return;
    return MemoryPieceStore::Instance()->Release(p, sz);
}

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_UTIL_CACHED_ALLOCATOR_H_
