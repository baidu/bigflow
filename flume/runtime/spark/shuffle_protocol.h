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

#ifndef FLUME_RUNTIME_SPARK_SHUFFLE_PROTOCOL_H_
#define FLUME_RUNTIME_SPARK_SHUFFLE_PROTOCOL_H_

#include <netinet/in.h>

#include <limits>

#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

#pragma pack(push, 1)
struct ShuffleHeader {
    static const ShuffleHeader* cast(const void *ptr) {
        return static_cast<const ShuffleHeader*>(ptr);
    }

    static ShuffleHeader* cast(void *ptr) {
        return static_cast<ShuffleHeader*>(ptr);
    }

    uint32_t task_index() const {
        return _task_index;
    }

    uint32_t partition() const {
        return ntohs(_partition) >> 1;
    }

    bool is_placeholder() const {
        return !(ntohs(_partition) & 1);
    }

    const char* content() const {
        return _content;
    }

    void set(uint32_t task, uint32_t partition, bool is_placeholder) {
        DCHECK_LT(task, 256);
        DCHECK_LT(partition, std::numeric_limits<int16_t>::max());

        _task_index = task;
        // reserve least significant bit for placeholder, so null record will come before
        // any other records in same partition
        _partition = htons((static_cast<uint16_t>(partition) << 1) | (is_placeholder ? 0 : 1));
    }

private:
    uint8_t _task_index;
    uint16_t _partition;
    char _content[0];
};

#pragma pack(pop)

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_SPARK_SHUFFLE_PROTOCOL_H_
