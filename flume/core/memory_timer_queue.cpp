/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

 $Id$
 *
 **************************************************************************/

 /**
 * @file memory_timer_queue.cpp
 * @author gaoran(bigflow-opensource@baidu.com)
 * @date 2017/01/11 10:27:37
 * @version $Revision$
 * @brief
 *
 **/

#include "flume/core/timer_queue.h"

namespace baidu {
namespace flume {
namespace core {

MemoryTimerQueue::MemoryTimerQueue() {}
MemoryTimerQueue::~MemoryTimerQueue() {}

void MemoryTimerQueue::push(Timer &timer) {
    _mem_queue.push(timer);
}

void MemoryTimerQueue::pop() {
    _mem_queue.pop();
}

Timer MemoryTimerQueue::top() {
    return _mem_queue.top();
}

}
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
