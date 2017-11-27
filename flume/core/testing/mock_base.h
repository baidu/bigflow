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

#ifndef FLUME_CORE_TESTING_MOCK_BASE_H_
#define FLUME_CORE_TESTING_MOCK_BASE_H_

#include <string>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/core/loader.h"

namespace baidu {
namespace flume {

template<typename Interface, typename MockType>
class MockBase : public Interface {
public:
    static MockType& Mock() {
        return Mock(toft::CreateCanonicalUUIDString());
    }

    static MockType& Mock(const std::string& config) {
        if (!s_mocks.count(config)) {
            std::string key = config;  // ptr_map::insert only accept non-const key as paramter
            s_mocks.insert(key, new MockType);

            s_mocks[config].m_type = MOCK;
            s_mocks[config].m_config = config;
        }
        return s_mocks[config];
    }

    enum Type {
        DELEGATE,
        MOCK
    };

public:
    MockBase() : m_type(DELEGATE) {}

    const std::string config() const { return m_config; }

    virtual void Setup(const std::string& config) {
        m_config = config;
        if (m_type == DELEGATE) {
            MockType* delegater = static_cast<MockType*>(this);
            delegater->Delegate(&Mock(config));
        } else {
            LOG(FATAL) << "Do not call Setup() directly.";
        }
    }

protected:
    static boost::ptr_map<std::string, MockType> s_mocks;

    Type m_type;
    std::string m_config;
};

template<typename Interface, typename MockType>
boost::ptr_map<std::string, MockType> MockBase<Interface, MockType>::s_mocks;

#define DELEGATE_METHOD0(mock, Type, Method) {                  \
        using ::testing::_;                                     \
        using ::testing::AnyNumber;                             \
        using ::testing::Invoke;                                \
        ON_CALL(*this, Method())                                \
            .WillByDefault(Invoke(mock, &Type::Method));        \
        EXPECT_CALL(*this, Method()).Times(AnyNumber());        \
}

#define DELEGATE_METHOD1(mock, Type, Method) {                  \
        using ::testing::_;                                     \
        using ::testing::AnyNumber;                             \
        using ::testing::Invoke;                                \
        ON_CALL(*this, Method(_))                               \
            .WillByDefault(Invoke(mock, &Type::Method));        \
        EXPECT_CALL(*this, Method(_)).Times(AnyNumber());       \
}

#define DELEGATE_METHOD2(mock, Type, Method) {                  \
        using ::testing::_;                                     \
        using ::testing::AnyNumber;                             \
        using ::testing::Invoke;                                \
        ON_CALL(*this, Method(_, _))                            \
            .WillByDefault(Invoke(mock, &Type::Method));        \
        EXPECT_CALL(*this, Method(_, _)).Times(AnyNumber());    \
}

#define DELEGATE_METHOD3(mock, Type, Method) {                      \
        using ::testing::_;                                         \
        using ::testing::AnyNumber;                                 \
        using ::testing::Invoke;                                    \
        ON_CALL(*this, Method(_, _, _))                             \
            .WillByDefault(Invoke(mock, &Type::Method));            \
        EXPECT_CALL(*this, Method(_, _, _)).Times(AnyNumber());     \
}

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_BASE_H_
