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

 Author: An Zhuo(bigflow-opensource@baidu.com)
 *
 **************************************************************************/

 /**
   interface for loader, sinker, processor status
 * @file status.h
 * @author zhuoan(bigflow-opensource@baidu.com)
 * @date 2016/03/31 11:22:05
 * @version 1.0.0
 * @brief
 *
 **/
#ifndef FLUME_CORE_STATUS_H_
#define FLUME_CORE_STATUS_H_

#include <stdint.h>
#include <string>
#include <vector>

namespace baidu {
namespace flume {
namespace core {

class IStatus {
public:
    virtual ~IStatus() {}

    // save status to buffer if has enough buffer
    // return serialize bytes size
    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size) = 0;

    // serialize success return true else false
    virtual bool Deserialize(const char* buffer, uint32_t buffer_size) = 0;

    // //this status object self use memory bytes, used by lru cache object
    // virtual uint32_t ObjectSize() = 0;
};

}
}
}
#endif  // FLUME_CORE_STATUS_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
