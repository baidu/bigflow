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
// Author: Xue Kang <bigflow-opensource@baidu.com>
//
// Description: A class to manage allocation of memory

#ifndef FLUME_UTIL_ARENA_H
#define FLUME_UTIL_ARENA_H

#include <cstddef>
#include <cstring>

#include "boost/align.hpp"
#include "boost/function.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "glog/logging.h"
#include "toft/base/scoped_array.h"
#include "toft/base/string/string_piece.h"
#include "toft/base/uncopyable.h"

namespace baidu {
namespace flume {
namespace util {

class Arena {
    TOFT_DECLARE_UNCOPYABLE(Arena);

public:
    Arena();

    // To check whether the remained memory is enough to allocate,
    // if not, allocate a new block of memory large enough.
    // Return the address of reserved buffer
    char* ReserveBytes(size_t buffer_size);

    // Return a pointer to a newly allocated memory of "size" bytes from remained buffer.
    // if failed, return NULL
    char* AllocateBytes(size_t size);

    // Copy binary to arena
    toft::StringPiece Allocate(const toft::StringPiece& binary);

    // For KeyReader::ReadKey and Objector::Serialize
    // if the buffer size is not enough, the construct fn return the buffer size needed
    // if construct success, return the filled buffer size
    template<typename ConstructFn>
    inline toft::StringPiece Allocate(ConstructFn fn) {
        size_t data_size = fn(remained_buffer(), remained_buffer_size());
        if (data_size > remained_buffer_size()) {
            ReserveBytes(data_size);
            data_size = fn(remained_buffer(), remained_buffer_size());
            DCHECK_LE(data_size, remained_buffer_size());
        }
        return toft::StringPiece(AllocateBytes(data_size), data_size);
    }

    // In-placement new T from arena
    template<typename T>
    T* Allocate();

    void Reset();

    char* remained_buffer();

    size_t remained_buffer_size();

    // Returns total size of memory reserved, including the memory wasted in every block
    size_t total_reserved_bytes();

    // for test
    char* current_block();

private:
    // Encapsulation of memory blocks
    class Component;

    // Return increased size by a factor of 2
    size_t CalculateAllocatingBytes(size_t buffer_size);

    boost::ptr_vector<Component> m_components;

    size_t m_total_reserved_bytes;
};

class Arena::Component {
public:
    explicit Component(size_t block_size) {
        m_block.reset(new char[block_size]);
        m_size = block_size;
        m_offset = 0;
    }

    // Allocate bytes from self's block
    char* AllocateBytes(size_t size) {
        DCHECK_LE(m_offset + size, m_size);

        char* result = m_block.get() + m_offset;
        m_offset += size;
        return result;
    }

    void Reset() {
        m_offset = 0;
    }

    char* block() {
        return m_block.get();
    }

    size_t size() {
        return m_size;
    }

    size_t offset() {
        return m_offset;
    }

    char* remained_buffer() {
        return m_block.get() + m_offset;
    }

    size_t remained_buffer_size() {
        return m_size - m_offset;
    }

private:
    toft::scoped_array<char> m_block;
    size_t m_size;
    size_t m_offset;
};

inline char* Arena::remained_buffer() {
    if (m_components.empty()) {
        ReserveBytes(0);
    }
    return m_components.back().remained_buffer();
}

inline size_t Arena::remained_buffer_size() {
    if (m_components.empty()) {
        ReserveBytes(0);
    }
    return m_components.back().remained_buffer_size();
}

inline size_t Arena::total_reserved_bytes() {
    return m_total_reserved_bytes;
}

inline char* Arena::current_block() {
    return m_components.empty() ? NULL : m_components.back().block();
}

inline char* Arena::AllocateBytes(size_t size) {
    if (m_components.empty() || size > this->remained_buffer_size()) {
        ReserveBytes(size);
    }
    return m_components.back().AllocateBytes(size);
}

inline toft::StringPiece Arena::Allocate(const toft::StringPiece& binary) {
    char* buffer = AllocateBytes(binary.size());
    std::memcpy(buffer, binary.data(), binary.size());
    return toft::StringPiece(buffer, binary.size());
}

template<typename T>
inline T* Arena::Allocate() {
    using boost::alignment::align;
    using boost::alignment::alignment_of;

    ReserveBytes(sizeof(T) + alignment_of<T>::value - 1);
    size_t space = remained_buffer_size();
    void* ptr = remained_buffer();
    CHECK_NOTNULL(align(alignment_of<T>::value, sizeof(T), ptr, space));

    space -= sizeof(T);
    AllocateBytes(remained_buffer_size() - space);

    return new(ptr) T();
}

} // namespace util
} // namespace flume
} // namespace baidu

#endif  // FLUME_UTIL_ARENA_H
