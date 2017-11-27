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

#include "flume/runtime/common/sub_executor_manager.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "flume/planner/graph_helper.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::Assign;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Expectation;

class FakeSource : public Source {
public:
    FakeSource(const std::string& id, unsigned scope_level)
        : m_id(id), m_scope_level(scope_level) {}

    virtual Handle* RequireIterator(IteratorCallback* iterator_callback,
                                    DoneCallback* done) {
        return NULL;
    }

    virtual Handle* RequireStream(uint32_t flag,
                                  StreamCallback* callback, DoneCallback* done) {
        return NULL;
    }

    const std::string& id() { return m_id; }
    unsigned scope_level() { return m_scope_level; }

private:
    std::string m_id;
    unsigned m_scope_level;
};

class MockExecutor : public Executor {
public:
    MockExecutor(unsigned scope_level,
                 const std::vector<std::string>& inputs,
                 const std::vector<std::string>& outputs)
            : m_scope_level(scope_level),
              m_inputs(inputs.begin(), inputs.end()),
              m_outputs(outputs.begin(), outputs.end()),
              m_is_setup(false) {
        ON_CALL(*this, Setup(_))
            .WillByDefault(DoAll(Invoke(this, &MockExecutor::VerifyInputs),
                                 Assign(&m_is_setup, true)));
    }

    virtual ~MockExecutor() {}

    MOCK_METHOD1(Setup, void (const std::map<std::string, Source*>&));  // NOLINT
    MOCK_METHOD1(BeginGroup, void (const toft::StringPiece& key));      // NOLINT
    MOCK_METHOD0(FinishGroup, void ());                                 // NOLINT

    virtual Source* GetSource(const std::string& id, unsigned scope_level) {
        EXPECT_EQ(true, m_is_setup);

        FakeSource* source = new FakeSource(id, scope_level);
        m_sources.push_back(source);
        return source;
    }

private:
    void VerifyInputs(const std::map<std::string, Source*>& inputs) {
        typedef std::set<std::string>::iterator Iterator;
        for (Iterator ptr = m_inputs.begin(); ptr != m_inputs.end(); ++ptr) {
            ASSERT_EQ(1u, inputs.count(*ptr));
            FakeSource* source = dynamic_cast<FakeSource*>(inputs.find(*ptr)->second);
            CHECK_NOTNULL(source);
            ASSERT_EQ(*ptr, source->id());
            ASSERT_EQ(m_scope_level, source->scope_level());
        }
    }

private:
    unsigned m_scope_level;
    std::set<std::string> m_inputs;
    std::set<std::string> m_outputs;

    bool m_is_setup;
    boost::ptr_vector<FakeSource> m_sources;
};

class SubExecutorManagerTest : public ::testing::Test {
protected:
    static const int kScopeLevel = 2;  // random choosed value

protected:
    virtual void SetUp() {
        m_message.set_scope_level(kScopeLevel);
    }

    MockExecutor& CreateExecutor(const std::vector<std::string>& inputs,
                                 const std::vector<std::string>& outputs) {
        MockExecutor* executor = new MockExecutor(kScopeLevel, inputs, outputs);
        m_executors.push_back(executor);

        PbExecutor* child = m_message.add_child();
        child->set_type(PbExecutor::EXTERNAL);
        child->set_scope_level(kScopeLevel);
        std::copy(inputs.begin(), inputs.end(),
                  google::protobuf::RepeatedFieldBackInserter(child->mutable_input()));
        std::copy(outputs.begin(), outputs.end(),
                  google::protobuf::RepeatedFieldBackInserter(child->mutable_output()));

        return *executor;
    }

    void AddInput(const std::string& id) {
        FakeSource* source = new FakeSource(id, kScopeLevel);
        m_sources.push_back(source);

        m_message.add_input(id);
    }

    void AddOutput(const std::string& id) {
        m_message.add_output(id);
    }

    std::map<std::string, Source*> GetInputSources() {
        std::map<std::string, Source*> results;
        for (size_t i = 0; i < m_sources.size(); ++i) {
            results[m_sources[i].id()] = &m_sources[i];
        }
        return results;
    }

    std::vector<Executor*> GetChildExecutors() {
        return std::vector<Executor*>(m_executors.c_array(),
                                      m_executors.c_array() + m_executors.size());
    }

    std::vector<std::string> Array(const char* s1 = NULL, const char* s2 = NULL,
                                   const char* s3 = NULL, const char* s4 = NULL) {
        std::vector<std::string> results;
        const char* array[] = {s1, s2, s3, s4};
        for (size_t i = 0; i < TOFT_ARRAY_SIZE(array); ++i) {
            if (array[i] != NULL) {
                results.push_back(array[i]);
            }
        }
        return results;
    }

protected:
    boost::ptr_vector<MockExecutor> m_executors;
    boost::ptr_vector<FakeSource> m_sources;
    PbExecutor m_message;
};

TEST_F(SubExecutorManagerTest, Setup) {
    AddInput("input-1");
    AddOutput("3-1");

    // define in random order
    MockExecutor& executor_4 = CreateExecutor(Array("2-1", "3-1"), Array("4-1", "4-2"));
    MockExecutor& executor_3 = CreateExecutor(Array("1-1", "2-2"), Array("3-1"));
    MockExecutor& executor_1 = CreateExecutor(Array(), Array("1-1"));
    MockExecutor& executor_5 = CreateExecutor(Array("4-1", "4-2"), Array());
    MockExecutor& executor_2 = CreateExecutor(Array("input-1"), Array("2-1", "2-2"));

    Expectation setup_1 = EXPECT_CALL(executor_1, Setup(_));
    Expectation setup_2 = EXPECT_CALL(executor_2, Setup(_));
    Expectation setup_3 = EXPECT_CALL(executor_3, Setup(_)).After(setup_1, setup_2);
    Expectation setup_4 = EXPECT_CALL(executor_4, Setup(_)).After(setup_2, setup_3);
    Expectation setup_5 = EXPECT_CALL(executor_5, Setup(_)).After(setup_4);

    SubExecutorManager manager;
    manager.Initialize(m_message, GetChildExecutors());
    manager.Setup(GetInputSources());

    FakeSource* source = dynamic_cast<FakeSource*>(manager.GetSource("3-1", 0));
    EXPECT_EQ(source->id(), "3-1");
    EXPECT_EQ(source->scope_level(), 0u);
}

TEST_F(SubExecutorManagerTest, BeginGroupAndFinishGroup) {
    const std::string kKey = "test_key";
    const toft::StringPiece kKeyPiece = kKey;

    MockExecutor& executor_2 = CreateExecutor(Array("1"), Array("2"));
    MockExecutor& executor_1 = CreateExecutor(Array(), Array("1"));
    MockExecutor& executor_3 = CreateExecutor(Array("2"), Array());

    {
        InSequence in_sequence;

        EXPECT_CALL(executor_1, Setup(_));
        EXPECT_CALL(executor_2, Setup(_));
        EXPECT_CALL(executor_3, Setup(_));

        EXPECT_CALL(executor_1, BeginGroup(kKeyPiece));
        EXPECT_CALL(executor_2, BeginGroup(kKeyPiece));
        EXPECT_CALL(executor_3, BeginGroup(kKeyPiece));

        EXPECT_CALL(executor_3, FinishGroup());
        EXPECT_CALL(executor_2, FinishGroup());
        EXPECT_CALL(executor_1, FinishGroup());
    }

    SubExecutorManager manager;
    manager.Initialize(m_message, GetChildExecutors());
    manager.Setup(GetInputSources());

    manager.BeginGroup(kKeyPiece);
    manager.FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
