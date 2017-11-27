/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Cong <wangcong09@baidu.com>

#ifndef FLUME_RUNTIME_SPARK_SPARK_KV_BUFFER_H
#define FLUME_RUNTIME_SPARK_SPARK_KV_BUFFER_H

#include <stdint.h>

#include "boost/scoped_array.hpp"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

// A Buffer that stores K/V bytes that potentially used for JVM/native data interactions
//
// |KeyLength|KeyBytes|ValueLength|ValueBytes|KeyLength|KeyBytes|...
//
class KVBuffer {

public:
    explicit KVBuffer(int64_t initial_size);
    ~KVBuffer();

    void reset();
    void put(const toft::StringPiece& key, const toft::StringPiece& value);
    const bool has_next();
    void next();
    const void key(toft::StringPiece* key);
    const void value(toft::StringPiece* value);

private:
    // Allocate enough buffer to ensure _size > required_size + current_usage
    void grow(size_t required_size);
    // Auto shrink the buffer size to release memory to OS
    void shrink();
    void put(const void* data, size_t size);

private:
    int64_t _size;
    toft::StringPiece _current_key;
    toft::StringPiece _current_value;
    boost::scoped_array<char> _buffer;
    char* _total;
    char* _end;
    char* _current;
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_SPARK_SPARK_KV_BUFFER_H
