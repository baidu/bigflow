/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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
// Author: Wen Xiang <wenxiang@baidu.com>
//
// Interface. Attach a key to the record, as the group according in the Shuffle process.
// See flume/doc/core.rst for details.

#ifndef FLUME_CORE_KEY_READER_H_
#define FLUME_CORE_KEY_READER_H_

#include <stdint.h>
#include <string>

namespace baidu {
namespace flume {
namespace core {

class KeyReader {
public:
    virtual ~KeyReader() {}

    virtual void Setup(const std::string& config) = 0;

    // save key to buffer if has enough buffer
    // return key size
    virtual uint32_t ReadKey(void* object, char* buffer, uint32_t buffer_size) = 0;
};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_KEY_READER_H_
