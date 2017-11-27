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
 * @file time_reader.h
 * @author zhuoan(bigflow-opensource@baidu.com)
 * @date 2016/03/23 13:58:05
 * @version 1.0.0
 * @brief
 *
 **/
#ifndef FLUME_CORE_TIME_READER_H
#define FLUME_CORE_TIME_READER_H

namespace baidu {
namespace flume {
namespace core {

class TimeReader {
public:
    virtual ~TimeReader() {}

    virtual void Setup(const std::string& config) = 0;

    virtual uint64_t get_timestamp(void* object) = 0;
};

}
}
}
#endif  // FLUME_CORE_TIME_READER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
