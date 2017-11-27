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

#ifndef FLUME_CORE_TESTING_MOCK_KEY_READER_H_
#define FLUME_CORE_TESTING_MOCK_KEY_READER_H_

#include <cstring>
#include <string>

#include "boost/ptr_container/ptr_map.hpp"
#include "gmock/gmock.h"

#include "flume/core/key_reader.h"
#include "flume/core/testing/mock_base.h"

namespace baidu {
namespace flume {

class MockKeyReader : public MockBase< ::baidu::flume::core::KeyReader, MockKeyReader > {
public:
    class KeySetter {
    public:
        KeySetter(MockKeyReader* base, void* object) : m_base(base), m_object(object) {}

        void operator=(const std::string& key) const {
            using ::testing::Return;
            EXPECT_CALL(*m_base, RealReadKey(m_object)).WillRepeatedly(Return(key));
        }

    private:
        MockKeyReader* m_base;
        void* m_object;
    };

    MOCK_METHOD1(RealReadKey, std::string (void* object));  // NOLINT

    void Delegate(MockKeyReader* mock) {
        using ::testing::_;
        using ::testing::AnyNumber;
        using ::testing::Invoke;

        ON_CALL(*this, RealReadKey(_))
            .WillByDefault(Invoke(mock, &MockKeyReader::RealReadKey));
        EXPECT_CALL(*this, RealReadKey(_)).Times(AnyNumber());
    }

    KeySetter KeyOf(void* object) {
        return KeySetter(this, object);
    }

private:
    virtual uint32_t ReadKey(void* object, char* buffer, uint32_t size) {
        std::string result = RealReadKey(object);
        if (result.size() <= size) {
            std::memcpy(buffer, result.data(), result.size());
        }
        return result.size();
    }
};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_KEY_READER_H_
