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
 **************************************************************************/
// Author: Zhang Yuncong <zhangyuncong@baidu.com>, Wen Xiang <wenxiang@baidu.com>

#ifndef FLUME_CORE_TESTING_MOCK_TIME_READER_H
#define FLUME_CORE_TESTING_MOCK_TIME_READER_H

#include <cstring>
#include <string>

#include "boost/ptr_container/ptr_map.hpp"
#include "gmock/gmock.h"

#include "flume/core/time_reader.h"
#include "flume/core/testing/mock_base.h"

namespace baidu {
namespace flume {

class MockTimeReader : public MockBase< ::baidu::flume::core::TimeReader, MockTimeReader > {
public:
    class TimeSetter {
    public:
        TimeSetter(MockTimeReader* base, void* object) : m_base(base), m_object(object) {}

        void operator=(const uint64_t& timestamp) const {
            using ::testing::Return;
            EXPECT_CALL(*m_base, get_timestamp(m_object)).WillRepeatedly(Return(timestamp));
        }

    private:
        MockTimeReader* m_base;
        void* m_object;
    };

    MOCK_METHOD1(get_timestamp, uint64_t (void* object));  // NOLINT

    void Delegate(MockTimeReader* mock) {
        using ::testing::_;
        using ::testing::AnyNumber;
        using ::testing::Invoke;
        DELEGATE_METHOD1(mock, MockTimeReader, get_timestamp);
    }

    TimeSetter TimestampOf(void* object) {
        return TimeSetter(this, object);
    }

};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_TIME_READER_H
