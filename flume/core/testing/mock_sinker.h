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

#ifndef FLUME_CORE_TESTING_MOCK_SINKER_H_
#define FLUME_CORE_TESTING_MOCK_SINKER_H_

#include <string>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "gmock/gmock.h"

#include "flume/core/sinker.h"
#include "flume/core/testing/mock_base.h"

namespace baidu {
namespace flume {

class MockSinker : public MockBase< ::baidu::flume::core::Sinker, MockSinker > {
public:
    MOCK_METHOD1(Open, void (const std::vector<std::string>&));        // NOLINT
    MOCK_METHOD1(Sink, void (void*));                                  // NOLINT
    MOCK_METHOD0(Close, void ());                                      // NOLINT

    void Delegate(MockSinker* mock) {
        // DELEGATE_METHOD1(mock, MockSinker, Open);
        {
            using ::testing::_;
            using ::testing::An;
            using ::testing::AnyNumber;
            using ::testing::Invoke;
            typedef void (MockSinker::* FuncPtr)(const std::vector<std::string>&);

            ON_CALL(*this, Open(_))
                .WillByDefault(Invoke(mock, static_cast<FuncPtr>(&MockSinker::Open)));
            EXPECT_CALL(*this, Open(_)).Times(AnyNumber());
        }

        DELEGATE_METHOD1(mock, MockSinker, Sink);
        DELEGATE_METHOD0(mock, MockSinker, Close);
    }

private:
    virtual void Open(const std::vector<toft::StringPiece>& keys) {
        std::vector<std::string> string_keys;
        for (size_t i = 0; i < keys.size(); ++i) {
            string_keys.push_back(keys[i].as_string());
        }
        Open(string_keys);
    }
};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_SINKER_H_
