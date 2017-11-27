/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
// Description:

#ifndef FLUME_PLANNER_MONITOR_MONITOR_TAGS_H_
#define FLUME_PLANNER_MONITOR_MONITOR_TAGS_H_

#include <map>
#include <string>

#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

struct External {
    enum Type {
        INVALID = 0,
        LOG_INPUT,
        MONITOR_READER,
        MONITOR_WRITER,
        MONITOR_DECODER,
        TYPE_COUNT
    };
    External() : type(INVALID) {}
    Type type;

    static std::string TypeString(External::Type type) {
        static std::string type_strs[] = {
            "INVALID",
            "LOG_INPUT",
            "MONITOR_READER",
            "MONITOR_WRITER",
            "MONITOR_DECODER"
        };
        return type_strs[static_cast<int>(type)];
    }
};

struct TaskInformation {
    enum Type {
        WORKER_STREAM = 0,
        WORKER_PREPARED,
        CLIENT_STREAM,
        CLIENT_PREPARED
    };
    Type type;
    static std::string TypeString(TaskInformation::Type type) {
        static std::string type_strs[] = {
            "WORKER_STREAM",
            "WORKER_PREPARED",
            "CLIENT_STREAM",
            "CLIENT_PREPARED"
        };
        return type_strs[static_cast<int>(type)];
    }
};

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_MONITOR_MONITOR_TAGS_H_

