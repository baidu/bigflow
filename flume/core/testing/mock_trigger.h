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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>, Wen Xiang <bigflow-opensource@baidu.com>

#ifndef FLUME_CORE_TESTING_MOCK_TRIGGER_H
#define FLUME_CORE_TESTING_MOCK_TRIGGER_H

#include <cstring>
#include <string>

#include "boost/function.hpp"
#include "boost/bind.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "gmock/gmock.h"

#include "flume/core/trigger.h"
#include "flume/core/timer.h"
#include "flume/core/testing/mock_base.h"


namespace baidu {
namespace flume {

bool push_timer(uint64_t delay_time, uint64_t ts, void* object, core::TimerPusher* pusher) {
    pusher->push(core::EVENT_TIME, ts + delay_time);
    return false;
}

class MockTrigger : public MockBase< ::baidu::flume::core::Trigger, MockTrigger> {
public:
    MockTrigger() {
        using ::testing::_;
        using ::testing::Return;
        using ::testing::AnyNumber;
        EXPECT_CALL(*this, on_element(_, _ , _))
            .WillRepeatedly(Return(false));
        EXPECT_CALL(*this, on_timer(_, _ , _))
            .WillRepeatedly(Return(true));
    }

    void Delegate(MockTrigger* mock) {

        DELEGATE_METHOD3(mock, MockTrigger, on_element);
        DELEGATE_METHOD3(mock, MockTrigger, on_timer);
    }

    MOCK_METHOD3(on_element, bool (uint64_t ts, void* object, core::TimerPusher* pusher));
    MOCK_METHOD3(on_timer, bool (core::TimerType, uint64_t ts, core::TimerPusher* pusher));

    void trigger_on_element(void* object) {
        using ::testing::_;
        using ::testing::Return;
        EXPECT_CALL(*this, on_element(_, object, _)).WillRepeatedly(Return(true));
    }

    void trigger_after_past_element(void* object, uint64_t delay_time) {
        using ::testing::Invoke;
        using ::testing::_;

        boost::function<bool(uint64_t, void*, core::TimerPusher*)> fn
            = boost::bind(push_timer, delay_time, _1, _2, _3);
        EXPECT_CALL(*this, on_element(_, object, _)).WillRepeatedly(Invoke(fn));
    }

    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size) {}

    virtual bool Deserialize(const char* buffer, uint32_t buffer_size) {}

    virtual ~MockTrigger() {
        DLOG(INFO) << "~MockTrigger";
    }
};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_TRIGGER_H
