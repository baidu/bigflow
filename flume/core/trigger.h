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
* @filename:    trigger.h
* @author:      bigflow-opensource@baidu.com
* @brief:       trigger interface
*/

#ifndef FLUME_CORE_TRIGGER_H
#define FLUME_CORE_TRIGGER_H

#include <vector>

#include "flume/core/status.h"
#include "flume/core/timer.h"

namespace baidu {
namespace flume {
namespace core {

class TimerPusher;

class Trigger : public IStatus {
public:
    virtual ~Trigger() {}

    virtual void Setup(const std::string& config) = 0;
    virtual const std::string config() const = 0;
    virtual void reset() = 0;
    virtual bool is_finished() = 0;

    virtual bool on_element(uint64_t ts, void* object, TimerPusher* pusher) = 0;
    virtual bool on_timer(TimerType type, uint64_t ts, TimerPusher* pusher) = 0;

    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size) = 0;
    virtual bool Deserialize(const char* buffer, uint32_t buffer_size) = 0;
};

} // namespace core
} // namespace flume
} // namespace baidu

#endif // FLUME_CORE_TRIGGER_H
