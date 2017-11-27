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
/**
* @created:     2016/06/21
* @filename:    window.h
* @author:      zhangyuncong@baidu.com
* @brief:       window interface
*/

#ifndef FLUME_CORE_WINDOW_H
#define FLUME_CORE_WINDOW_H

#include "boost/shared_ptr.hpp"

#include "flume/core/status.h"

namespace baidu {
namespace flume {
namespace core {

class Trigger;

struct WindowStatus : public core::IStatus{
    WindowStatus() {}
    WindowStatus(uint64_t id): id(id) {}
    uint64_t id;
    struct Interval {
        // [begin, end)
        Interval(uint64_t begin, uint64_t end) : begin(begin), end(end) {}
        uint64_t begin;
        uint64_t end;
    };
    std::vector<Interval> intervals;
    boost::shared_ptr<core::Trigger> trigger;

    // save status to buffer if has enough buffer
    // return serialize bytes size
    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size) {
        return 0;
    }

    // serialize success return true else false
    virtual bool Deserialize(const char* buffer, uint32_t buffer_size) {
        return true;
    }
};

} // namespace core
} // namespace flume
} // namespace baidu

#endif // FLUME_CORE_WINDOW_H

