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
// Interface. Decide an bucket number which the record should be put in, used in Shuffle
// process. See flume/doc/core.rst for details.

#ifndef FLUME_CORE_PARTITIONER_H_
#define FLUME_CORE_PARTITIONER_H_

#include <stdint.h>
#include <string>

#include "glog/logging.h"
#include "toft/base/byte_order.h"
#include "toft/base/string/string_piece.h"
#include "toft/system/memory/unaligned.h"

namespace baidu {
namespace flume {
namespace core {

class Partitioner {
public:
    virtual ~Partitioner() {}

    virtual void Setup(const std::string& config) = 0;

    // returned sequence should be less than partition_number
    virtual uint32_t Partition(void* object, uint32_t partition_number) = 0;
};

inline uint32_t DecodePartition(const toft::StringPiece& buffer) {
    CHECK_EQ(buffer.size(), sizeof(uint32_t));   // NOLINT(runtime/sizeof)
    uint32_t partition = toft::GetUnaligned<uint32_t>(buffer.data());
    return toft::ByteOrder::FromNet<uint32_t>(partition);
}

inline std::string EncodePartition(uint32_t partition) {
    uint32_t encoded_partition = toft::ByteOrder::ToNet<uint32_t>(partition);
    return std::string(reinterpret_cast<char*>(&encoded_partition), sizeof(encoded_partition));
}

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_PARTITIONER_H_
