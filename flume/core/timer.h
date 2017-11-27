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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)
// storage interface

#ifndef FLUME_RUNTIME_CORE_TIMER_H
#define FLUME_RUNTIME_CORE_TIMER_H

#include <string>
#include <queue>

#include "boost/shared_ptr.hpp"
#include "toft/base/string/string_piece.h"
#include "glog/logging.h"
//#include "flume/runtime/stream/storage_manager.h"
#include "flume/core/timer_queue.h"

namespace baidu {
namespace flume {
namespace core {

//typedef std::priority_queue<Timer, std::vector<Timer>, std::greater<Timer> > TimerQueue;

enum TimerType {
    EVENT_TIME = 0,
    PROCESSING_TIME = 1,
    //FIXED_PROCESSING_TIME = 2
};

class TimerPusher {
public:
    TimerPusher(TimerQueue* event_timers,
            TimerQueue* processing_timers,
            const std::vector<toft::StringPiece>& keys,
            uint64_t window_id)
        : _event_timers(event_timers),
        _processing_timers(processing_timers),
        _window_key(keys, window_id) {
        }

    TimerPusher(TimerQueue* event_timers,
            TimerQueue* processing_timers,
            const WindowKey& win_key)
        : _event_timers(event_timers),
        _processing_timers(processing_timers),
        _window_key(win_key) {
    }

    void push(TimerType type, uint64_t timestamp) {
        LOG(INFO) << "timestamp = " << timestamp << ", winid = " << _window_key.second;
        TimerQueue* queue = (type == EVENT_TIME ? _event_timers : _processing_timers);
        Timer timer = std::make_pair(timestamp, _window_key);
        queue->push(timer);
    }

private:
    TimerQueue* _event_timers;
    TimerQueue* _processing_timers;
    WindowKey _window_key;
};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_CORE_TIMER_H
