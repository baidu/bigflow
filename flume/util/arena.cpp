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
// Author: Xue Kang <xuekang@baidu.com>

#include <assert.h>

#include "gflags/gflags.h"

#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace util {

static const int kMinBlockSize = 4096;              // 4k page; should be 2^N
static const int kMaxBlockSize = 64 * 1024 * 1024;  // 64M; should be 2^N

Arena::Arena() : m_total_reserved_bytes(0) {}

char* Arena::ReserveBytes(size_t buffer_size) {
    if (!m_components.empty() && this->remained_buffer_size() >= buffer_size) {
        return this->remained_buffer();
    }

    while (!m_components.empty() && m_components.back().offset() == 0) {
        m_total_reserved_bytes -= m_components.back().size();
        m_components.pop_back();
    }

    size_t allocating_bytes = CalculateAllocatingBytes(buffer_size);
    m_components.push_back(new Component(allocating_bytes));
    m_total_reserved_bytes += allocating_bytes;
    return m_components.back().remained_buffer();
}

inline size_t Arena::CalculateAllocatingBytes(size_t required_size) {
    if (required_size >= kMaxBlockSize) {
        return ((required_size - 1) & ~(kMinBlockSize - 1)) + kMinBlockSize;
    }

    size_t increased_block_size =
            m_components.empty() ? kMinBlockSize : m_components.back().size() * 2;
    if (increased_block_size > kMaxBlockSize) {
        increased_block_size = kMaxBlockSize;
    }

    while (increased_block_size < required_size) {
        increased_block_size *= 2;
    }
    return increased_block_size;
}

void Arena::Reset() {
    while (m_components.size() > 1) {
        m_components.pop_back();
    }

    if (m_components.empty()) {
        m_total_reserved_bytes = 0;
    } else {
        m_components.back().Reset();
        m_total_reserved_bytes = m_components.back().size();
    }
}

} // namespace util
} // namespace flume
} // namespace baidu
