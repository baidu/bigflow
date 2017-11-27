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

#ifndef FLUME_CORE_TESTING_STRING_OBJECTOR_H_
#define FLUME_CORE_TESTING_STRING_OBJECTOR_H_

#include <cstring>
#include <string>

#include "flume/core/objector.h"

namespace baidu {
namespace flume {

class StringObjector : public core::Objector {
public:
    virtual void Setup(const std::string& config) {
    }

    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size) {
        std::string* str = static_cast<std::string*>(object);
        if (str->size() <= buffer_size) {
            std::memcpy(buffer, str->data(), str->size());
        }
        return str->size();
    }

    virtual void* Deserialize(const char* buffer, uint32_t buffer_size) {
        return new std::string(buffer, buffer_size);
    }

    virtual void Release(void *object) {
        delete static_cast<std::string*>(object);
    }
};


}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_STRING_OBJECTOR_H_
