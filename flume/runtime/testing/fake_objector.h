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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//
// A fake implemention of core::Objector.

#ifndef FLUME_RUNTIME_TESTING_FAKE_OBJECTOR_H_
#define FLUME_RUNTIME_TESTING_FAKE_OBJECTOR_H_

#include <set>
#include <string>

#include "boost/lexical_cast.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/core/objector.h"
#include "flume/runtime/testing/fake_pointer.h"

namespace baidu {
namespace flume {
namespace runtime {

class FakeObjector : public core::Objector {
public:
    virtual ~FakeObjector() {
        using ::testing::ElementsAre;
        EXPECT_THAT(m_objects, ElementsAre());
    }

    virtual void Setup(const std::string& config) {}

    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size) {
        std::string result = boost::lexical_cast<std::string>(reinterpret_cast<uint64_t>(object));
        if (result.size() <= buffer_size) {
            std::memcpy(buffer, result.data(), result.size());
        }
        return result.size();
    }

    virtual void* Deserialize(const char* buffer, uint32_t buffer_size) {
        void* object = ObjectPtr(boost::lexical_cast<uint64_t>(buffer, buffer_size));
        m_objects.insert(object);
        return object;
    }

    virtual void Release(void* object) {
        ASSERT_NE(0u, m_objects.count(object));
        m_objects.erase(object);
    }

private:
    std::multiset<void*> m_objects;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_FAKE_OBJECTOR_H_
