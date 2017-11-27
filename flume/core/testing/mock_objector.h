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

#ifndef FLUME_CORE_TESTING_MOCK_OBJECTOR_H_
#define FLUME_CORE_TESTING_MOCK_OBJECTOR_H_

#include <string>

#include "boost/ptr_container/ptr_map.hpp"
#include "gmock/gmock.h"

#include "flume/core/objector.h"
#include "flume/core/testing/mock_base.h"

namespace baidu {
namespace flume {

class MockObjector : public MockBase< ::baidu::flume::core::Objector, MockObjector > {
public:
    MOCK_METHOD3(Serialize, uint32_t(void*, char*, uint32_t));
    MOCK_METHOD2(Deserialize, void*(const char*, uint32_t));
    MOCK_METHOD1(Release, void(void*)); // NOLINT

    void Delegate(MockObjector* mock) {
        DELEGATE_METHOD3(mock, MockObjector, Serialize);
        DELEGATE_METHOD2(mock, MockObjector, Deserialize);
        DELEGATE_METHOD1(mock, MockObjector, Release);
    }
};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_OBJECTOR_H_
