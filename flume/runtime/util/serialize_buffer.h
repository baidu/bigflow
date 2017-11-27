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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//
// A temporary buffer for key/value serialization

#ifndef FLUME_RUNTIME_UTIL_SERIALIZE_BUFFER_H_
#define FLUME_RUNTIME_UTIL_SERIALIZE_BUFFER_H_

#include <stdint.h>

#include "boost/scoped_array.hpp"
#include "glog/logging.h"
#include "toft/base/string/string_piece.h"

namespace baidu {
namespace flume {
namespace runtime {

class SerializeBuffer {
public:
    template<typename Fn>
    struct AllocateContext {
        Fn* fn;
        uint32_t (Fn::* method)(void*, char*, uint32_t);

        void* object;
        char* buffer;
        uint32_t buffer_size;
    };

    struct CopyContext {
        const char* data;
        uint32_t data_size;

        char* buffer;
        uint32_t buffer_size;
    };

    struct ReserveContext {
        uint32_t size;

        char* buffer;
        uint32_t buffer_size;
    };

    class Guard {
    public:
        Guard() : m_data(NULL), m_size(0) {}

        template<typename Fn>
        void operator=(const AllocateContext<Fn>& context) {
            Fn* fn = context.fn;

            m_size = fn->Serialize(context.object, context.buffer, context.buffer_size);
            m_data = context.buffer;

            if (m_size > context.buffer_size) {
                const size_t buffer_size = m_size;
                m_buffer.reset(new char[buffer_size]);

                m_data = m_buffer.get();
                m_size = fn->Serialize(context.object, m_buffer.get(), buffer_size);
                DCHECK_LE(m_size, buffer_size);
            } else {
                m_buffer.reset();
            }
        }

        void operator=(const CopyContext& context) {
            if (context.data_size <= context.buffer_size) {
                m_buffer.reset();
                m_data = context.buffer;
            } else {
                m_buffer.reset(new char[context.data_size]);
                m_data = m_buffer.get();
            }
            memcpy(m_data, context.data, context.data_size);

            m_size = context.data_size;
        }

        void operator=(const ReserveContext& context) {
            if (context.size <= context.buffer_size) {
                m_buffer.reset();
                m_data = context.buffer;
            } else {
                m_buffer.reset(new char[context.size]);
                m_data = m_buffer.get();
            }

            m_size = context.size;
        }

        char* data() { return m_data; }

        uint32_t size() { return m_size; }

        toft::StringPiece ref() const {
            return toft::StringPiece(m_data, m_size);
        }

    private:
        boost::scoped_array<char> m_buffer;
        char* m_data;
        uint32_t m_size;
    };

    explicit SerializeBuffer(size_t buffer_size) : m_buffer_size(buffer_size) {}

    template<typename Fn>
    AllocateContext<Fn> Allocate(Fn* fn,
                                 uint32_t (Fn::* method)(void*, char*, uint32_t),
                                 void* object) {
        if (m_buffer.get() == NULL) {
            m_buffer.reset(new char[m_buffer_size]);
        }

        AllocateContext<Fn> context;
        context.fn = fn;
        context.method = method;
        context.object = object;
        context.buffer = m_buffer.get();
        context.buffer_size = m_buffer_size;
        return context;
    }

    CopyContext Copy(const toft::StringPiece& src) {
        if (m_buffer.get() == NULL) {
            m_buffer.reset(new char[m_buffer_size]);
        }

        CopyContext context;
        context.data = src.data();
        context.data_size = src.size();
        context.buffer = m_buffer.get();
        context.buffer_size = m_buffer_size;
        return context;
    }

    ReserveContext Reserve(uint32_t size) {
        if (m_buffer.get() == NULL) {
            m_buffer.reset(new char[m_buffer_size]);
        }

        ReserveContext context;
        context.size = size;
        context.buffer = m_buffer.get();
        context.buffer_size = m_buffer_size;
        return context;
    }

private:
    boost::scoped_array<char> m_buffer;
    const size_t m_buffer_size;
};


}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_UTIL_SERIALIZE_BUFFER_H_
