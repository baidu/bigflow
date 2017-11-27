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
// Author: Wen Xiang <wenxiang@baidu.com>
//
// Interface, write datas to a storage like file, database and so on. Corresponding to
// SINK_NODE in logical execution plan. See flume/doc/core.rst for details.

#ifndef FLUME_CORE_SINKER_H_
#define FLUME_CORE_SINKER_H_

#include <stdint.h>
#include <string>
#include <vector>

#include "flume/core/status.h"
#include "toft/base/string/string_piece.h"

namespace baidu {
namespace flume {
namespace core {

class Committer {
public:
    virtual ~Committer() {};

    virtual void Commit() = 0;
};

class Sinker : public IStatus {
public:
    virtual ~Sinker() {}

    virtual void Setup(const std::string& config) = 0;

    virtual void Open(const std::vector<toft::StringPiece>& keys) = 0;
    // if splits is empty, use api default concurrency
    virtual void Split(std::vector<std::string>* splits) {}
    virtual void Sink(void* object) = 0;
    virtual void Close() = 0;

    virtual Committer* GetCommitter() {
        return NULL;
    }

    // save loader status to buffer if has enough buffer
    // return serialize bytes size
    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size) { return 0; }

    // return an address to deserialized loader.
    virtual bool Deserialize(const char* buffer, uint32_t buffer_size) { return true; }

    virtual uint32_t ObjectSize() { return sizeof(Sinker); }

};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_SINKER_H_
