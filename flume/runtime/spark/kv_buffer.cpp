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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "flume/runtime/spark/kv_buffer.h"

#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

static const int64_t MAX_SIZE = 512 * 1024 * 1024;

KVBuffer::KVBuffer(int64_t initial_size) : _size(initial_size) {
    CHECK(_size > 0);
    _buffer.reset(new char[_size]);
    _total = _buffer.get() + _size;
    _end = _buffer.get();
    _current = _buffer.get();
}

KVBuffer::~KVBuffer() {
    LOG(INFO) << "Releasing KVBuffer";
}

void KVBuffer::put(const toft::StringPiece& key, const toft::StringPiece& value) {
    int64_t key_len = key.length();
    int64_t value_len = value.length();
    int64_t total_length = key_len + value_len + 2 * sizeof(int64_t);

    if (_end + total_length > _total) {
        grow(static_cast<size_t>(total_length));
    }

    put(&key_len, sizeof(key_len));
    put(key.data(), key_len);
    put(&value_len, sizeof(value_len));
    put(value.data(), value_len);
}

void KVBuffer::reset() {
    DLOG(INFO) << "Buffer reset!";
    _end = _buffer.get();
    _current = _buffer.get();
}

const bool KVBuffer::has_next() {
    return _current < _end;
}

void KVBuffer::next() {
    CHECK(has_next()) << "Has no next";

    int64_t key_len = *reinterpret_cast<int64_t*>(_current);
    _current_key.set(_current + sizeof(int64_t), key_len);

    _current += (key_len + sizeof(int64_t));

    int64_t value_len = *reinterpret_cast<int64_t*>(_current);
    _current_value.set(_current + sizeof(int64_t), value_len);

    _current += (value_len + sizeof(int64_t));
}

const void KVBuffer::key(toft::StringPiece* key) {
    key->set(_current_key.data(), _current_key.size());
}

const void KVBuffer::value(toft::StringPiece* value) {
    value->set(_current_value.data(), _current_value.size());
}

void KVBuffer::grow(size_t required_size) {
    // when grow is called, it's certain that _buffer cannot hold all the
    // data(current_usage + required_size)
    int64_t current_usage = _end - _current;
    CHECK_GE(current_usage, 0) << "Current buffer usage can not be less than 0";
    CHECK_LE(current_usage + required_size, MAX_SIZE)
        << "Cannot grow KVBuffer: maximum size exceeded";
    int64_t previous_size = _size;
    while (_size < 1.1 * (current_usage + required_size)) {
        // 1.1 is a magic number so that we don't pay another allocation cost if total memory
        // requirement is a bit large the _size
        _size *= 2;
    }
    // make sure we don't allocate buffer greater than MAX_SIZE
    if (_size >= MAX_SIZE) {_size = MAX_SIZE;}
    LOG(INFO) << "Trying to grow KVBuffer size from " << previous_size << " to " << _size;
    char* new_buffer = new(std::nothrow) char[_size];
    CHECK_NOTNULL(new_buffer);
    _end = std::copy(_current, _end, new_buffer);
    _current = new_buffer;
    _total = new_buffer + _size;
    _buffer.reset(new_buffer);
}

void KVBuffer::shrink() {
    // todo: Add shrink operation to release allocated buffer to OS quickly.
}

void KVBuffer::put(const void* data, size_t size) {
    memcpy(_end, data, size);
    _end += size;
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

