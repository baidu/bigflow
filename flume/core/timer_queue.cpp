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
 * @file timer_queue.cpp
 * @author gaoran(gaoran@baidu.com)
 * @date 2017/01/10 10:55:13
 * @version $Revision$
 * @brief
 *
 **/
#include "timer_queue.h"

namespace baidu {
namespace flume {

namespace core {

bool operator< (const WindowKey& a, const WindowKey& b) {
    if (a.second < b.second) {
        return true;
    } else if (a.second > b.second) {
        return false;
    }
    return a.real_first < b.real_first;
}

}
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
