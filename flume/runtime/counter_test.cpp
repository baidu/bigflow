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

#include "flume/runtime/counter.h"

#include <string>

#include "gtest/gtest.h"

namespace baidu {
namespace flume {
namespace runtime {

TEST(CounterTest, BasicCounterOperations) {
    CounterSession counters;

    Counter* counter = counters.GetCounter("|test");
    EXPECT_EQ(0u, counter->GetValue());

    counter->Update(1);
    EXPECT_EQ(1u, counter->GetValue());
    EXPECT_EQ(1u, counters.GetCounter("|test")->GetValue());

    counter->Update(99);
    EXPECT_EQ(100u, counter->GetValue());
    EXPECT_EQ(100u, counters.GetCounter("|test")->GetValue());

    counter->Reset();
    EXPECT_EQ(0u, counter->GetValue());
    EXPECT_EQ(0u, counters.GetCounter("|test")->GetValue());
}

TEST(CounterTest, BasicCounterSessionOperations) {
    CounterSession session;
    session.GetCounter("|0");
    session.GetCounter("|1")->Update(1);

    std::map<std::string, const Counter*> counters = session.GetAllCounters();
    EXPECT_EQ(2u, counters.size());
    EXPECT_EQ(0u, counters["|0"]->GetValue());
    EXPECT_EQ(1u, counters["|1"]->GetValue());

    session.ResetAllCounters();
    counters = session.GetAllCounters();
    EXPECT_EQ(2u, counters.size());
    EXPECT_EQ(0u, counters["|0"]->GetValue());
    EXPECT_EQ(0u, counters["|1"]->GetValue());

    CounterSession session_;
    session_.GetCounter("|1")->Update(1);
    session_.GetCounter("|2")->Update(2);

    session.Merge(session_);
    counters = session.GetAllCounters();
    EXPECT_EQ(3u, counters.size());
    EXPECT_EQ(0u, counters["|0"]->GetValue());
    EXPECT_EQ(1u, counters["|1"]->GetValue());
    EXPECT_EQ(2u, counters["|2"]->GetValue());
}

TEST(CounterTest, CounterSessionNormalTest) {
    std::string name = "abc";
    std::string default_prefix = "default";
    std::string prefix_get = "";
    std::string name_get = "";

    EXPECT_EQ(default_prefix + "|" +  name,
              CounterSession::GenerateCounterKey(default_prefix, name));

    CounterSession::GetPrefixFromCounterKey(
                        CounterSession::GenerateCounterKey(default_prefix, name),
                        &prefix_get, &name_get);
    EXPECT_EQ(default_prefix, prefix_get);
    EXPECT_EQ(name, name_get);

    CounterSession::GetPrefixFromCounterKey(name, &prefix_get, &name_get);

    EXPECT_EQ(CounterSession::COUNTER_DEFAULT_PREFIX, prefix_get);
    EXPECT_EQ(name, name_get);
}

DEFINE_COUNTER(test);

TEST(CounterTest, GlobalCounter) {
    EXPECT_EQ(0u, static_cast<uint64_t>(COUNTER_test));

    EXPECT_EQ(0u, static_cast<uint64_t>(COUNTER_test++));
    EXPECT_EQ(1, static_cast<uint64_t>(COUNTER_test));

    ++ ++COUNTER_test;
    EXPECT_EQ(static_cast<uint64_t>(COUNTER_test), 3);

    COUNTER_test += 2;
    EXPECT_EQ(static_cast<uint64_t>(COUNTER_test), 5);

    std::map<std::string, const Counter*> counters =
            CounterSession::GlobalCounterSession()->GetAllCounters();
    std::string test_counter_name = "Flume|baidu::flume::runtime::test";
    EXPECT_EQ(1u, counters.count(test_counter_name));
    EXPECT_EQ(5u, CounterSession::GlobalCounterSession()
                ->GetCounter(test_counter_name)->GetValue());

    CounterSession::GlobalCounterSession()->ResetAllCounters();
    EXPECT_EQ(static_cast<uint64_t>(COUNTER_test), 0);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
