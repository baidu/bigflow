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
//
// Interface, load datas from a storage like file, database and so on. Corresponding to
// LOAD_NODE in logical execution plan. See flume/doc/core.rst for details.

#ifndef FLUME_CORE_LOADER_H_
#define FLUME_CORE_LOADER_H_

#include <stdint.h>
#include <string>
#include <vector>

#include "flume/core/status.h"
#include "flume/core/emitter.h"
#include "flume/proto/split.pb.h"  // flume/proto/split.proto

namespace baidu {
namespace flume {
namespace core {

class Loader : public IStatus {
public:
    static PbSplit EncodeSplit(const std::string& raw_split);
    static PbSplit DecodeSplit(const std::string& split);

public:
    virtual ~Loader() {}

    virtual void Setup(const std::string& config) = 0;

    virtual void Split(const std::string& uri, std::vector<std::string>* splits) = 0;

    virtual void Load(const std::string& split, Emitter* emitter) = 0;

    // save loader status to buffer if has enough buffer
    // return serialize bytes size
    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size) { return 0; }

    // return an address to deserialized loader.
    virtual bool Deserialize(const char* buffer, uint32_t buffer_size) { return true; }

    virtual uint32_t ObjectSize() { return sizeof(Loader); }

};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_LOADER_H_
