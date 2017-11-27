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
 * @file memory_timer_queue.h
 * @author gaoran(bigflow-opensource@baidu.com)
 * @date 2017/01/10 23:58:18
 * @version $Revision$
 * @brief
 *
 **/
#ifndef FLUME_CORE_MEMORY_TIMER_QUEUE_H
#define FLUME_CORE_MEMORY_TIMER_QUEUE_H

#include "flume/core/timer_queue.h"

namespace baidu {
namespace flume {
namespace core {

class MemoryTimerQueue : public TimerQueue<Timer> {
public:
    MemoryTimerQueue();
    virtual ~MemoryTimerQueue();
    virtual void push(Timer &timer);
    virtual void pop();
    virtual Timer top();

private:
    std::priority_queue<core::Timer, std::vector<core::Timer>, std::greater<core::Timer> > _mem_queue;
};

}
}
}
#endif  // MEMORY_TIMER_QUEUE_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
