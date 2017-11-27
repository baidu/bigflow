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

#include "flume/runtime/common/executor_base.h"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "boost/bind.hpp"
#include "boost/foreach.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/fake_pointer.h"
#include "flume/runtime/testing/mock_listener.h"
#include "flume/runtime/testing/mock_source.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

class MockExecutorCore : public ExecutorCore {
public:
    MOCK_METHOD1(Begin, void(const std::vector<std::string>&));
    MOCK_METHOD0(End, void());

    MockListener& RequireObject(const std::string& id) {
        return m_object_listeners[id];
    }

    MockListener& RequireBinary(const std::string& id) {
        return m_binary_listeners[id];
    }

    MockListener& RequireObjectAndBinary(const std::string& id) {
        return m_object_and_binary_listeners[id];
    }

    MockListener& RequireIterator(const std::string& id) {
        return m_iterator_listeners[id];
    }

    void Emit(const std::string& id, void* object) {
        m_base->GetOutput(id)->EmitObject(object);
    }

    void CloseOutput(const std::string& id) {
        m_base->GetOutput(id)->Done();
    }

    void CloseInput(const std::string& id) {
        m_handles[id]->Done();
    }

    void BeginSubGroup(const toft::StringPiece& key) {
        m_base->BeginSubGroup(key);
    }

    void EndSubGroup() {
        m_base->EndSubGroup();
    }

protected:
    typedef boost::ptr_map<std::string, MockListener> ListenerMap;

    virtual void Setup(const PbExecutor& message, ExecutorBase* base) {
        m_message = message;
        m_base = base;

        std::for_each(m_object_listeners.begin(), m_object_listeners.end(),
                      boost::bind(&MockExecutorCore::DoRequireObject, this, _1));
        std::for_each(m_binary_listeners.begin(), m_binary_listeners.end(),
                      boost::bind(&MockExecutorCore::DoRequireBinary, this, _1));
        std::for_each(m_object_and_binary_listeners.begin(), m_object_and_binary_listeners.end(),
                      boost::bind(&MockExecutorCore::DoRequireObjectAndBinary, this, _1));
        std::for_each(m_iterator_listeners.begin(), m_iterator_listeners.end(),
                      boost::bind(&MockExecutorCore::DoRequireIterator, this, _1));
    }

    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys) {
        std::vector<std::string> args;
        for (size_t i = 0; i < keys.size(); ++i) {
            args.push_back(keys[i].as_string());
        }
        Begin(args);
    }

    virtual void EndGroup() {
        End();
    }

private:
    void DoRequireObject(ListenerMap::reference record) {
        m_handles[record.first] = record.second->RequireObject(m_base->GetInput(record.first));
    }

    void DoRequireBinary(ListenerMap::reference record) {
        m_handles[record.first] = record.second->RequireBinary(m_base->GetInput(record.first));
    }

    void DoRequireObjectAndBinary(ListenerMap::reference record) {
        m_handles[record.first] =
                record.second->RequireObjectAndBinary(m_base->GetInput(record.first));
    }

    void DoRequireIterator(ListenerMap::reference record) {
        m_handles[record.first] = record.second->RequireIterator(m_base->GetInput(record.first));
    }

private:
    ListenerMap m_object_listeners;
    ListenerMap m_binary_listeners;
    ListenerMap m_object_and_binary_listeners;
    ListenerMap m_iterator_listeners;

    PbExecutor m_message;
    ExecutorBase* m_base;
    std::map<std::string, Source::Handle*> m_handles;
};

ACTION_P3(Emit, core, identity, object) {
    core->Emit(identity, object);
}

ACTION_P2(CloseInput, core, identity) {
    core->CloseInput(identity);
}

ACTION_P2(CloseOutput, core, identity) {
    core->CloseOutput(identity);
}

ACTION_P2(BeginSubGroup, core, key) {
    core->BeginSubGroup(key);
}

ACTION_P(EndSubGroup, core) {
    core->EndSubGroup();
}

class MockExecutor : public Executor {
public:
    MockExecutor& ListenTo(const std::string& identity) {
        m_inputs.push_back(identity);
        return *this;
    }

    MockExecutor& Provide(const std::string& identity) {
        m_outputs.push_back(identity);
        return *this;
    }

    MOCK_METHOD2(GetSource, Source* (const std::string&, unsigned));    // NOLINT
    MOCK_METHOD1(Begin, void (const std::string& key));                 // NOLINT
    MOCK_METHOD0(Finish, void ());                                      // NOLINT
    MOCK_METHOD2(Got, void (int , const std::vector<int>&));            // NOLINT

    std::vector<std::string>& inputs() { return m_inputs; }

    std::vector<std::string>& outputs() { return m_outputs; }

protected:
    virtual void Setup(const std::map<std::string, Source*>& sources) {
        for (size_t i = 0; i < m_inputs.size(); ++i) {
            std::map<std::string, Source*>::const_iterator ptr = sources.find(m_inputs[i]);
            ASSERT_TRUE(ptr != sources.end());
            ptr->second->RequireStream(Source::REQUIRE_OBJECT,
                    toft::NewPermanentClosure(this, &MockExecutor::OnInputCome, i),
                    toft::NewPermanentClosure(this, &MockExecutor::OnInputDone, i)
            );
        }
    }

    void BeginGroup(const toft::StringPiece& key) {
        Begin(key.as_string());
    }

    void FinishGroup() {
        Finish();
    }

private:
    void OnInputCome(int index, const std::vector<toft::StringPiece>& keys,
                     void* object, const toft::StringPiece& binary) {
        m_records[index].push_back(reinterpret_cast<uint64_t>(object));
    }

    void OnInputDone(int index) {
        Got(index, m_records[index]);
        m_records.erase(index);
    }

private:
    std::vector<std::string> m_inputs;
    std::vector<std::string> m_outputs;

    std::map<int, std::vector<int> > m_records;
};

class ExecutorBaseTest : public ::testing::Test {
public:
    MockSource& AddInput(const std::string& identity) {
        m_message.add_input(identity);
        m_inputs[identity].SetObjector(new FakeObjector());
        return m_inputs[identity];
    }

    MockListener& AddOutput(const std::string& identity) {
        m_message.add_output(identity);
        return m_outputs[identity];
    }

    PbExecutor::Dispatcher* AddDispatcher(const std::string& identity) {
        PbExecutor::Dispatcher* dispatcher = m_message.add_dispatcher();
        dispatcher->set_identity(identity);
        return dispatcher;
    }

    MockExecutor& AddChild() {
        m_childs.push_back(new MockExecutor());
        return m_childs.back();
    }

    ExecutorBase& StartTest(uint32_t input_scope_level, uint32_t output_scope_level,
                            ExecutorCore* core) {
        m_message.set_type(PbExecutor::EXTERNAL);
        m_message.set_scope_level(output_scope_level);

        std::vector<Executor*> childs;
        BOOST_FOREACH(MockExecutor& executor, m_childs) {
            childs.push_back(&executor);

            PbExecutor* message = m_message.add_child();
            message->set_type(PbExecutor::EXTERNAL);
            message->set_scope_level(output_scope_level);
            std::copy(executor.inputs().begin(), executor.inputs().end(),
                      google::protobuf::RepeatedFieldBackInserter(message->mutable_input()));
            std::copy(executor.outputs().begin(), executor.outputs().end(),
                      google::protobuf::RepeatedFieldBackInserter(message->mutable_output()));
        }

        m_base.reset(new ExecutorBase(core));
        m_base->Initialize(m_message, childs, input_scope_level, NULL);
        Executor* executor = static_cast<Executor*>(m_base.get());

        std::map<std::string, Source*> sources;
        typedef boost::ptr_map<std::string, MockSource>::reference InputType;
        BOOST_FOREACH(InputType input, m_inputs) {
            sources[input.first] = input.second;
        }
        executor->Setup(sources);

        typedef boost::ptr_map<std::string, MockListener>::reference OutputType;
        BOOST_FOREACH(OutputType output, m_outputs) {
            output.second->RequireObject(executor->GetSource(output.first, input_scope_level));
        }

        return *m_base;
    }

private:
    PbExecutor m_message;
    toft::scoped_ptr<ExecutorBase> m_base;

    boost::ptr_map<std::string, MockSource> m_inputs;
    boost::ptr_map<std::string, MockListener> m_outputs;
    boost::ptr_vector<MockExecutor> m_childs;
};

TEST_F(ExecutorBaseTest, DelegateObject) {
    MockSource& source = AddInput("input");
    EXPECT_CALL(source, RequireObject(_, _));

    MockExecutorCore& core = *new MockExecutorCore();
    MockListener& inside_listener = core.RequireObject("input");
    {
        InSequence in_sequence;

        EXPECT_CALL(core, Begin(ElementsAre("", "g-1")));
        EXPECT_CALL(inside_listener, GotObject(ObjectPtr(1)))
            .WillOnce(Emit(&core, "output", ObjectPtr(11)));
        EXPECT_CALL(inside_listener, GotObject(ObjectPtr(2)))
            .WillOnce(CloseInput(&core, "input"));
        EXPECT_CALL(source, Done());
        EXPECT_CALL(inside_listener, GotDone());
        EXPECT_CALL(core, End());

        EXPECT_CALL(core, Begin(ElementsAre("", "g-2")));
        EXPECT_CALL(inside_listener, GotObject(ObjectPtr(3)))
            .WillOnce(CloseOutput(&core, "output"));
        EXPECT_CALL(inside_listener, GotDone());
        EXPECT_CALL(core, End());
    }

    MockListener& outside_listener = AddOutput("output");
    {
        InSequence in_sequence;

        EXPECT_CALL(outside_listener, GotObject(ObjectPtr(11)));
        EXPECT_CALL(outside_listener, GotDone());

        EXPECT_CALL(outside_listener, GotDone());
    }

    AddDispatcher("output");

    Executor& executor = StartTest(1, 1, &core);
    executor.BeginGroup("");
    {
        executor.BeginGroup("g-1");
        executor.FinishGroup();
        source.DispatchObject(ObjectPtr(1));
        source.DispatchObject(ObjectPtr(2));
        source.DispatchDone();
    }
    {
        executor.BeginGroup("g-2");
        executor.FinishGroup();
        source.DispatchObject(ObjectPtr(3));
        source.DispatchDone();
    }
    executor.FinishGroup();
}

TEST_F(ExecutorBaseTest, DelegateBinary) {
    MockSource& source = AddInput("input");
    EXPECT_CALL(source, RequireBinary(_, _));

    MockExecutorCore& core = *new MockExecutorCore();
    MockListener& inside_listener = core.RequireBinary("input");
    {
        InSequence in_sequence;

        EXPECT_CALL(core, Begin(ElementsAre("", "g-1")));
        EXPECT_CALL(inside_listener, GotBinary("1"))
            .WillOnce(Emit(&core, "output", ObjectPtr(11)));
        EXPECT_CALL(inside_listener, GotBinary("2"))
            .WillOnce(CloseInput(&core, "input"));
        EXPECT_CALL(source, Done());
        EXPECT_CALL(inside_listener, GotDone());
        EXPECT_CALL(core, End());

        EXPECT_CALL(core, Begin(ElementsAre("", "g-2")));
        EXPECT_CALL(inside_listener, GotBinary("3"))
            .WillOnce(CloseOutput(&core, "output"));
        EXPECT_CALL(inside_listener, GotDone());
        EXPECT_CALL(core, End());
    }

    MockListener& outside_listener = AddOutput("output");
    {
        InSequence in_sequence;

        EXPECT_CALL(outside_listener, GotObject(ObjectPtr(11)));
        EXPECT_CALL(outside_listener, GotDone());

        EXPECT_CALL(outside_listener, GotDone());
    }

    AddDispatcher("output");

    Executor& executor = StartTest(1, 1, &core);
    executor.BeginGroup("");
    {
        executor.BeginGroup("g-1");
        executor.FinishGroup();
        source.DispatchBinary("1");
        source.DispatchBinary("2");
        source.DispatchDone();
    }
    {
        executor.BeginGroup("g-2");
        executor.FinishGroup();
        source.DispatchBinary("3");
        source.DispatchDone();
    }
    executor.FinishGroup();
}

TEST_F(ExecutorBaseTest, DelegateObjectAndBinary) {
    MockSource& source = AddInput("input");
    EXPECT_CALL(source, RequireObjectAndBinary(_, _));

    MockExecutorCore& core = *new MockExecutorCore();
    MockListener& inside_listener = core.RequireObjectAndBinary("input");
    {
        InSequence in_sequence;

        EXPECT_CALL(core, Begin(ElementsAre("", "g-1")));
        EXPECT_CALL(inside_listener, GotObjectAndBinary(ObjectPtr(1), "1"))
            .WillOnce(Emit(&core, "output", ObjectPtr(11)));
        EXPECT_CALL(inside_listener, GotObjectAndBinary(ObjectPtr(2), "2"))
            .WillOnce(CloseInput(&core, "input"));
        EXPECT_CALL(source, Done());
        EXPECT_CALL(inside_listener, GotDone());
        EXPECT_CALL(core, End());

        EXPECT_CALL(core, Begin(ElementsAre("", "g-2")));
        EXPECT_CALL(inside_listener, GotObjectAndBinary(ObjectPtr(3), "3"))
            .WillOnce(CloseOutput(&core, "output"));
        EXPECT_CALL(inside_listener, GotDone());
        EXPECT_CALL(core, End());
    }

    MockListener& outside_listener = AddOutput("output");
    {
        InSequence in_sequence;

        EXPECT_CALL(outside_listener, GotObject(ObjectPtr(11)));
        EXPECT_CALL(outside_listener, GotDone());

        EXPECT_CALL(outside_listener, GotDone());
    }

    AddDispatcher("output");

    Executor& executor = StartTest(1, 1, &core);
    executor.BeginGroup("");
    {
        executor.BeginGroup("g-1");
        executor.FinishGroup();
        source.DispatchObjectAndBinary(ObjectPtr(1), "1");
        source.DispatchObjectAndBinary(ObjectPtr(2), "2");
        source.DispatchDone();
    }
    {
        executor.BeginGroup("g-2");
        executor.FinishGroup();
        source.DispatchObjectAndBinary(ObjectPtr(3), "3");
        source.DispatchDone();
    }
    executor.FinishGroup();
}

TEST_F(ExecutorBaseTest, DelegateIterator) {
    MockSource& source = AddInput("input");
    EXPECT_CALL(source, RequireIterator(_, _));

    MockExecutorCore& core = *new MockExecutorCore();
    MockListener& inside_listener = core.RequireIterator("input");
    {
        InSequence in_sequence;

        EXPECT_CALL(core, Begin(ElementsAre("", "g-1")));
        EXPECT_CALL(inside_listener, GotIterator(IterateOver(1, 2)))
            .WillOnce(Emit(&core, "output", ObjectPtr(11)));
        EXPECT_CALL(inside_listener, GotDone());
        EXPECT_CALL(core, End());
    }

    MockListener& outside_listener = AddOutput("output");
    {
        InSequence in_sequence;

        EXPECT_CALL(outside_listener, GotObject(ObjectPtr(11)));
        EXPECT_CALL(outside_listener, GotDone());
    }

    PbExecutor::Dispatcher* output = AddDispatcher("output");
    output->set_need_dataset(true);

    Executor& executor = StartTest(1, 1, &core);
    executor.BeginGroup("");
    {
        executor.BeginGroup("g-1");
        executor.FinishGroup();
        source.DispatchIterator(1, 2);
        source.DispatchDone();
    }
    executor.FinishGroup();
}

TEST_F(ExecutorBaseTest, DelegateChildOutput) {
    MockExecutor& child_0 = AddChild();
    child_0.Provide("inside-source");

    MockSource inside_source_0;
    EXPECT_CALL(child_0, GetSource("inside-source", 0))
        .WillOnce(Return(&inside_source_0));
    EXPECT_CALL(inside_source_0, RequireObject(_, _));

    MockSource inside_source_1;
    EXPECT_CALL(child_0, GetSource("inside-source", 1))
        .WillOnce(Return(&inside_source_1));
    EXPECT_CALL(inside_source_1, RequireObject(_, _));

    MockListener& listener = AddOutput("inside-source");
    EXPECT_CALL(listener, GotObject(ObjectPtr(1)));
    EXPECT_CALL(listener, GotDone());

    MockExecutor& child_1 = AddChild();
    child_1.ListenTo("inside-source");
    EXPECT_CALL(child_1, Got(0, ElementsAre(11)));

    StartTest(0, 1, new MockExecutorCore());
    {
        inside_source_0.DispatchObject(ObjectPtr(1));
        inside_source_0.DispatchDone();

        inside_source_1.DispatchObject(ObjectPtr(11));
        inside_source_1.DispatchDone();
    }
}

TEST_F(ExecutorBaseTest, OneChildWithOutsideInput) {
    MockSource& input = AddInput("input");
    EXPECT_CALL(input, RequireObject(_, _));

    AddDispatcher("output");

    MockExecutorCore& core = *new MockExecutorCore();
    MockListener& listener = core.RequireObject("input");
    {
        InSequence in_sequence;

        EXPECT_CALL(core, Begin(ElementsAre("g", "g-1")));
        EXPECT_CALL(listener, GotObject(ObjectPtr(1)))
            .WillOnce(DoAll(BeginSubGroup(&core, "g-1-1"),
                            Emit(&core, "output", ObjectPtr(11)),
                            CloseOutput(&core, "output"),
                            EndSubGroup(&core)));
        EXPECT_CALL(listener, GotObject(ObjectPtr(2)))
            .WillOnce(DoAll(BeginSubGroup(&core, "g-1-2"),
                            Emit(&core, "output", ObjectPtr(12)),
                            EndSubGroup(&core)));
        EXPECT_CALL(listener, GotDone());
        EXPECT_CALL(core, End());

        EXPECT_CALL(core, Begin(ElementsAre("g", "g-2")))
            .WillOnce(BeginSubGroup(&core, "g-2-1"));
        EXPECT_CALL(listener, GotDone())
            .WillOnce(Emit(&core, "output", ObjectPtr(21)));
        EXPECT_CALL(core, End())
            .WillOnce(EndSubGroup(&core));
    }

    MockExecutor& child = AddChild();
    child.ListenTo("output");
    {
        InSequence in_sequence;

        EXPECT_CALL(child, Begin("g"));

        EXPECT_CALL(child, Begin("g-1"));
        {
            EXPECT_CALL(child, Begin("g-1-1"));
            EXPECT_CALL(child, Finish());
            EXPECT_CALL(child, Got(0, ElementsAre(11)));

            EXPECT_CALL(child, Begin("g-1-2"));
            EXPECT_CALL(child, Finish());
            EXPECT_CALL(child, Got(0, ElementsAre(12)));
        }
        EXPECT_CALL(child, Finish());

        EXPECT_CALL(child, Begin("g-2"));
        {
            EXPECT_CALL(child, Begin("g-2-1"));
            EXPECT_CALL(child, Finish());
            EXPECT_CALL(child, Got(0, ElementsAre(21)));
        }
        EXPECT_CALL(child, Finish());

        EXPECT_CALL(child, Finish());
    }

    Executor& executor = StartTest(1, 2, &core);
    executor.BeginGroup("g");
    {
        executor.BeginGroup("g-1");
        executor.FinishGroup();
        input.DispatchObject(ObjectPtr(1));
        input.DispatchObject(ObjectPtr(2));
        input.DispatchDone();
    }
    {
        executor.BeginGroup("g-2");
        executor.FinishGroup();
        input.DispatchDone();
    }
    executor.FinishGroup();
}

TEST_F(ExecutorBaseTest, OneChildWithNoInput) {
    AddDispatcher("output");

    MockExecutorCore& core = *new MockExecutorCore();
    {
        InSequence in_sequence;

        EXPECT_CALL(core, Begin(ElementsAre("g", "g-1")))
            .WillOnce(DoAll(BeginSubGroup(&core, "g-1-1"),
                            Emit(&core, "output", ObjectPtr(11)),
                            EndSubGroup(&core)));
        EXPECT_CALL(core, End())
            .WillOnce(DoAll(BeginSubGroup(&core, "g-1-2"),
                            Emit(&core, "output", ObjectPtr(12)),
                            EndSubGroup(&core)));

        EXPECT_CALL(core, Begin(ElementsAre("g", "g-2")))
            .WillOnce(BeginSubGroup(&core, "g-2-1"));
        EXPECT_CALL(core, End())
            .WillOnce(EndSubGroup(&core));
    }

    MockExecutor& child = AddChild();
    child.ListenTo("output");
    {
        InSequence in_sequence;

        EXPECT_CALL(child, Begin("g"));

        EXPECT_CALL(child, Begin("g-1"));
        {
            EXPECT_CALL(child, Begin("g-1-1"));
            EXPECT_CALL(child, Finish());
            EXPECT_CALL(child, Got(0, ElementsAre(11)));

            EXPECT_CALL(child, Begin("g-1-2"));
            EXPECT_CALL(child, Finish());
            EXPECT_CALL(child, Got(0, ElementsAre(12)));
        }
        EXPECT_CALL(child, Finish());

        EXPECT_CALL(child, Begin("g-2"));
        {
            EXPECT_CALL(child, Begin("g-2-1"));
            EXPECT_CALL(child, Finish());
            EXPECT_CALL(child, Got(0, ElementsAre()));
        }
        EXPECT_CALL(child, Finish());

        EXPECT_CALL(child, Finish());
    }

    Executor& executor = StartTest(1, 2, &core);
    executor.BeginGroup("g");
    {
        executor.BeginGroup("g-1");
        executor.FinishGroup();

        executor.BeginGroup("g-2");
        executor.FinishGroup();
    }
    executor.FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
