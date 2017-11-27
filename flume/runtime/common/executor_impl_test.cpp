/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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

#include "flume/runtime/common/executor_impl.h"

#include "boost/lexical_cast.hpp"
#include "boost/scoped_ptr.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/runtime/common/general_dispatcher.h"
#include "flume/runtime/common/single_dispatcher.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/fake_pointer.h"
#include "flume/runtime/testing/mock_dispatcher.h"
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
using ::testing::ReturnNull;

class MockInputManager : public internal::ExecutorInputManager {
public:
    MockInputManager(const PbExecutor& message, uint32_t input_scope_level) :
            ExecutorInputManager(message, input_scope_level) {}

    MOCK_METHOD1(input_ready, void(const std::vector<std::string>&));
    MOCK_METHOD0(input_done, void());

private:
    virtual void on_input_ready() {
        std::vector<std::string> keys;
        for (size_t i = 0; i < _keys.size(); ++i) {
            keys.push_back(_keys[i].as_string());
        }
        input_ready(keys);
    }

    virtual void on_input_done() {
        input_done();
    }
};

class ExecutorInputManagerTest : public ::testing::Test {
public:
    MockSource& add_input(const std::string& identity) {
        _message.add_input(identity);
        _sources.push_back(new MockSource());
        return _sources.back();
    }

    MockInputManager& start_test(uint32_t input_scope_level) {
        _manager.reset(new MockInputManager(_message, input_scope_level));

        std::map<std::string, Source*> sources;
        for (int i = 0; i < _message.input_size(); ++i) {
            sources[_message.input(i)] = &_sources[i];
        }
        _manager->setup(sources);

        return *_manager;
    }

private:
    PbExecutor _message;
    boost::ptr_vector<MockSource> _sources;
    boost::scoped_ptr<MockInputManager> _manager;
};

TEST_F(ExecutorInputManagerTest, NoInputNoListener) {
    MockInputManager& manager = start_test(1);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, input_ready(ElementsAre("0", "a")));
        EXPECT_CALL(manager, input_done());

        EXPECT_CALL(manager, input_ready(ElementsAre("0", "b")));
        EXPECT_CALL(manager, input_done());

        EXPECT_CALL(manager, input_ready(ElementsAre("2", "c")));
        EXPECT_CALL(manager, input_done());
    }


    manager.begin_group("0");
    {
        manager.begin_group("a");
        manager.finish_group();
    }
    {
        manager.begin_group("b");
        manager.finish_group();
    }
    manager.finish_group();

    manager.begin_group("1");
    manager.finish_group();

    manager.begin_group("2");
    {
        manager.begin_group("c");
        manager.finish_group();
    }
    manager.finish_group();
}

TEST_F(ExecutorInputManagerTest, MultiInputMultiListener) {
    MockListener listener_0;
    MockListener listener_1;
    MockSource& source_0 = add_input("input-0");
    MockSource& source_1 = add_input("input-1");
    MockInputManager& manager = start_test(1);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, input_ready(ElementsAre("0", "a")));
        EXPECT_CALL(listener_0, GotDone());
        EXPECT_CALL(listener_1, GotObject(ObjectPtr(0)));
        EXPECT_CALL(listener_1, GotDone());
        EXPECT_CALL(manager, input_done());

        EXPECT_CALL(manager, input_ready(ElementsAre("0", "b")));
        EXPECT_CALL(listener_1, GotDone());
        EXPECT_CALL(listener_0, GotObject(ObjectPtr(1)));
        EXPECT_CALL(listener_0, GotDone());
        EXPECT_CALL(manager, input_done());
    }

    EXPECT_CALL(source_0, RequireObject(_, _));
    EXPECT_CALL(source_1, RequireObject(_, _));
    listener_0.RequireObject(manager.get("input-0"));
    listener_1.RequireObject(manager.get("input-1"));

    manager.begin_group("0");
    {
        manager.begin_group("a");
        manager.finish_group();
        source_0.DispatchDone();
        source_1.DispatchObject(ObjectPtr(0));
        source_1.DispatchDone();
    }
    {
        manager.begin_group("b");
        manager.finish_group();
        source_1.DispatchDone();
        source_0.DispatchObject(ObjectPtr(1));
        source_0.DispatchDone();
    }
    manager.finish_group();
}

TEST_F(ExecutorInputManagerTest, RequireStreamWithDoneCallback) {
    MockListener listener;
    MockSource& source = add_input("input");
    MockInputManager& manager = start_test(0);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, input_ready(ElementsAre("0")));
        EXPECT_CALL(listener, GotObject(ObjectPtr(0)));
        EXPECT_CALL(listener, GotDone());
        EXPECT_CALL(manager, input_done());

        EXPECT_CALL(manager, input_ready(ElementsAre("1")));
        EXPECT_CALL(listener, GotObject(ObjectPtr(1)));
        EXPECT_CALL(listener, GotDone());
        EXPECT_CALL(manager, input_done());
    }

    EXPECT_CALL(source, RequireObject(_, _));
    listener.RequireObject(manager.get("input"));

    manager.begin_group("0");
    manager.finish_group();
    source.DispatchObject(ObjectPtr(0));
    source.DispatchDone();

    manager.begin_group("1");
    manager.finish_group();
    source.DispatchObject(ObjectPtr(1));
    source.DispatchDone();
}

TEST_F(ExecutorInputManagerTest, RequireStreamWithoutDoneCallback) {
    MockListener listener;
    MockSource& source = add_input("input");
    MockInputManager& manager = start_test(0);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, input_ready(ElementsAre("0")));
        EXPECT_CALL(listener, GotBinary("a"));
        EXPECT_CALL(manager, input_done());

        EXPECT_CALL(manager, input_ready(ElementsAre("1")));
        EXPECT_CALL(listener, GotBinary("b"));
        EXPECT_CALL(manager, input_done());
    }

    EXPECT_CALL(source, RequireBinary(_, _));
    EXPECT_CALL(listener, NewDoneCallback()).WillRepeatedly(ReturnNull());
    listener.RequireBinary(manager.get("input"));

    manager.begin_group("0");
    manager.finish_group();
    source.DispatchBinary("a");
    source.DispatchDone();

    manager.begin_group("1");
    manager.finish_group();
    source.DispatchBinary("b");
    source.DispatchDone();
}

TEST_F(ExecutorInputManagerTest, RequireIteratorWithDoneCallback) {
    MockListener listener;
    MockSource& source = add_input("input");
    MockInputManager& manager = start_test(0);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, input_ready(ElementsAre("0")));
        EXPECT_CALL(listener, GotIterator(IterateOver(0, 9)));
        EXPECT_CALL(manager, input_done());

        EXPECT_CALL(manager, input_ready(ElementsAre("1")));
        EXPECT_CALL(listener, GotIterator(IterateOver(10, 19)));
        EXPECT_CALL(manager, input_done());
    }

    EXPECT_CALL(source, RequireIterator(_, _));
    EXPECT_CALL(listener, NewDoneCallback()).WillRepeatedly(ReturnNull());
    listener.RequireIterator(manager.get("input"));

    manager.begin_group("0");
    manager.finish_group();
    source.DispatchIterator(0, 9);
    source.DispatchDone();

    manager.begin_group("1");
    manager.finish_group();
    source.DispatchIterator(10, 19);
    source.DispatchDone();
}

TEST_F(ExecutorInputManagerTest, RequireIteratorWithoutDoneCallback) {
    MockListener listener;
    MockSource& source = add_input("input");
    MockInputManager& manager = start_test(0);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, input_ready(ElementsAre("0")));
        EXPECT_CALL(listener, GotIterator(IterateOver(0, 9)));
        EXPECT_CALL(listener, GotDone());
        EXPECT_CALL(manager, input_done());

        EXPECT_CALL(manager, input_ready(ElementsAre("1")));
        EXPECT_CALL(listener, GotIterator(IterateOver(10, 19)));
        EXPECT_CALL(listener, GotDone());
        EXPECT_CALL(manager, input_done());
    }

    EXPECT_CALL(source, RequireIterator(_, _));
    listener.RequireIterator(manager.get("input"));

    manager.begin_group("0");
    manager.finish_group();
    source.DispatchIterator(0, 9);
    source.DispatchDone();

    manager.begin_group("1");
    manager.finish_group();
    source.DispatchIterator(10, 19);
    source.DispatchDone();
}

class DispatcherManagerTest : public ::testing::Test {
public:
    typedef internal::DispatcherManager DispatcherManager;

    template<typename DispatcherType>
    bool is_type_of(Dispatcher* dispatcher) {
        return dynamic_cast<DispatcherType*>(dispatcher) != NULL;
    }
};

TEST_F(DispatcherManagerTest, Empty) {
    DispatcherManager manager(PbExecutor(), NULL);
    manager.begin_group("", 0);
    manager.finish_group();
    manager.done();
}

TEST_F(DispatcherManagerTest, PriorityOutput) {
    PbExecutor message;
    message.set_scope_level(0);

    for (int i = 0; i < 5; ++i) {
        PbExecutor::Dispatcher* config = message.add_dispatcher();
        config->set_identity(boost::lexical_cast<std::string>(i));
        config->set_usage_count(1);
        config->set_need_dataset(false);
    }
    message.mutable_dispatcher(4)->set_priority(0);
    message.mutable_dispatcher(3)->set_priority(1);
    message.mutable_dispatcher(1)->set_priority(1);
    message.mutable_dispatcher(0)->set_priority(2);
    DispatcherManager manager(message, NULL);

    std::vector<MockDispatcher*> dispatchers;
    for (int i = 0; i < message.dispatcher_size(); ++i) {
        MockDispatcher& dispatcher = *(new MockDispatcher);
        std::auto_ptr<Dispatcher> deleter(
                manager.replace(boost::lexical_cast<std::string>(i), &dispatcher)
        );

        InSequence in_sequence;
        EXPECT_CALL(dispatcher, GetScopeLevel()).WillOnce(Return(-1));
        EXPECT_CALL(dispatcher, BeginGroup(toft::StringPiece("")));
        EXPECT_CALL(dispatcher, GetScopeLevel()).WillRepeatedly(Return(0));
        EXPECT_CALL(dispatcher, FinishGroup());

        dispatchers.push_back(&dispatcher);
    }

    {
        InSequence in_sequence;

        EXPECT_CALL(*dispatchers[4], Done());
        EXPECT_CALL(*dispatchers[1], Done());
        EXPECT_CALL(*dispatchers[3], Done());
        EXPECT_CALL(*dispatchers[0], Done());
        EXPECT_CALL(*dispatchers[2], Done());
    }

    manager.begin_group("", 0);
    manager.finish_group();
    manager.close_prior_dispatchers(0);
    manager.close_prior_dispatchers(1);
    manager.close_prior_dispatchers(1);
    manager.close_prior_dispatchers(2);
    manager.done();
}

TEST_F(DispatcherManagerTest, SingleDispatcher) {
    PbExecutor message;
    message.set_scope_level(0);

    PbExecutor::Dispatcher* dispatcher = message.add_dispatcher();
    dispatcher->set_identity("output");
    dispatcher->set_usage_count(1);
    dispatcher->set_need_dataset(false);

    DispatcherManager manager(message, NULL);
    ASSERT_TRUE(is_type_of<SingleStreamDispatcher>(manager.get("output")));

    MockListener listener;
    listener.RequireObject(manager.get("output")->GetSource(0));
    EXPECT_CALL(listener, GotDone());

    manager.begin_group("", 0);
    manager.finish_group();
    manager.done();
}

TEST_F(DispatcherManagerTest, GeneralDispatcher) {
    PbExecutor message;
    message.set_scope_level(0);

    PbExecutor::Dispatcher* dispatcher = message.add_dispatcher();
    dispatcher->set_identity("output");
    dispatcher->set_usage_count(2);
    dispatcher->set_need_dataset(false);

    DispatcherManager manager(message, NULL);
    ASSERT_TRUE(is_type_of<GeneralDispatcher>(manager.get("output")));
    manager.begin_group("", 0);
    manager.finish_group();
    manager.done();
}

class MockRunner {
public:
    MockRunner() : _context(NULL) {}
    virtual ~MockRunner() {}

    MOCK_METHOD1(begin, void(const std::vector<std::string>&));
    MOCK_METHOD0(end, void());
    MOCK_METHOD2(got, void(const std::string&, int));
    MOCK_METHOD1(done, void(const std::string&));

    void setup(const PbExecutor& message, ExecutorContext* context) {
        _context = context;
        for (int i = 0; i < message.input_size(); ++i) {
            register_listener(message.input(i), context->input(message.input(i)));
        }
    }

    void begin_group(const std::vector<toft::StringPiece>& keys) {
        std::vector<std::string> args;
        for (size_t i = 0; i < keys.size(); ++i) {
            args.push_back(keys[i].as_string());
        }
        begin(args);
    }

    void end_group() {
        end();
    }

    void begin_sub_group(const toft::StringPiece& key) {
        _context->begin_sub_group(key);
    }

    void end_sub_group() {
        _context->end_sub_group();
    }

    void emit(const std::string& identity, void* object) {
        _context->output(identity)->EmitObject(object);
    }

    void emit_done(const std::string& identity) {
        _context->output(identity)->Done();
    }

protected:
    void register_listener(const std::string& identity, Source* source) {
        CHECK_NOTNULL(source);
        source->RequireStream(
                Source::REQUIRE_OBJECT,
                toft::NewPermanentClosure(this, &MockRunner::on_input_come, identity),
                toft::NewPermanentClosure(this, &MockRunner::on_input_done, identity)
        );
    }

    void on_input_come(const std::string& identity, const std::vector<toft::StringPiece>& keys,
                       void* object, const toft::StringPiece& binary) {
        got(identity, reinterpret_cast<uint64_t>(object));
    }

    void on_input_done(const std::string& identity) {
        done(identity);
    }

    ExecutorContext* _context;
    std::map<std::string, std::vector<int> > _records;
};

class MockChild : public MockRunner, public Executor {
public:
    explicit MockChild(PbExecutor* message) : _message(message) {}

    MOCK_METHOD1(begin_group, void(const std::string&));
    MOCK_METHOD0(finish_group, void());

    void add_input(const std::string& identity) {
        _message->add_input(identity);
    }

    MockSource& add_output(const std::string& identity) {
        _message->add_output(identity);
        return _sources[identity];
    }

    void emit(const std::string& identity, void* object) {
        _sources[identity].DispatchObject(object);
    }

    void emit_done(const std::string& identity) {
        _sources[identity].DispatchDone();
    }

protected:
    virtual void Setup(const std::map<std::string, Source*>& sources) {
        for (int i = 0; i < _message->input_size(); ++i) {
            std::map<std::string, Source*>::const_iterator ptr = sources.find(_message->input(i));
            ASSERT_TRUE(ptr != sources.end());
            register_listener(_message->input(i), ptr->second);
        }
    }

    virtual Source* GetSource(const std::string& identity, unsigned scope_level) {
        CHECK_EQ(_sources.count(identity), 1);
        return &_sources[identity];
    }

    void BeginGroup(const toft::StringPiece& key) {
        begin_group(key.as_string());
    }

    void FinishGroup() {
        finish_group();
    }

private:
    PbExecutor* _message;
    boost::ptr_map<std::string, MockSource> _sources;
};

ACTION_P3(Emit, runner, identity, object) {
    runner->emit(identity, object);
}

ACTION_P2(EmitDone, runner, identity) {
    runner->emit_done(identity);
}

ACTION_P2(BeginSubGroup, runner, key) {
    runner->begin_sub_group(key);
}

ACTION_P(EndSubGroup, runner) {
    runner->end_sub_group();
}

class ExecutorImplTest : public ::testing::Test {
public:
    typedef ExecutorRunnerWrapper<MockRunner> MockRunnerExecutor;

    MockSource& add_input(const std::string& identity) {
        _message.add_input(identity);
        _inputs[identity].SetObjector(new FakeObjector());
        return _inputs[identity];
    }

    MockListener& add_output(const std::string& identity) {
        _message.add_output(identity);
        return _outputs[identity];
    }

    void add_dispatcher(const std::string& identity) {
        PbExecutor::Dispatcher* dispatcher = _message.add_dispatcher();
        dispatcher->set_identity(identity);
        dispatcher->set_usage_count(0);  // use GeneralDispatcher
        dispatcher->set_need_dataset(false);
    }

    MockChild& add_child() {
        _childs.push_back(new MockChild(_message.add_child()));
        return _childs.back();
    }

    MockRunnerExecutor& start_test(uint32_t input_scope_level, uint32_t output_scope_level) {
        std::vector<Executor*> childs;
        for (int i = 0; i < _message.child_size(); ++i) {
            childs.push_back(&_childs[i]);
        }

        _message.set_scope_level(output_scope_level);
        _executor.reset(
                new_executor_by_runner<MockRunner>(input_scope_level, _message, childs, NULL)
        );

        typedef boost::ptr_map<std::string, MockSource>::iterator InputIterator;
        std::map<std::string, Source*> inputs;
        for (InputIterator ptr = _inputs.begin(); ptr != _inputs.end(); ++ptr) {
            inputs[ptr->first] = ptr->second;
        }
        _executor->Setup(inputs);

        for (int i = 0; i < _message.output_size(); ++i) {
            Source* source = _executor->GetSource(_message.output(i), input_scope_level);
            CHECK_NOTNULL(source);
            _outputs[_message.output(i)].RequireObject(source);
        }

        return *_executor;
    }

    MockRunner& runner() {
        return *_executor->runner();
    }

private:
    PbExecutor _message;
    boost::scoped_ptr<MockRunnerExecutor> _executor;
    boost::ptr_map<std::string, MockSource> _inputs;
    boost::ptr_map<std::string, MockListener> _outputs;
    boost::ptr_vector<MockChild> _childs;
};

TEST_F(ExecutorImplTest, NoInputNoDispatcherNoChild) {
    // input_scope_level = output_scope_level = 0
    MockRunnerExecutor& executor = start_test(0, 0);
    MockRunner& runner = *executor.runner();
    {
        InSequence in_sequence;

        EXPECT_CALL(runner, begin(ElementsAre("a")));
        EXPECT_CALL(runner, end());

        EXPECT_CALL(runner, begin(ElementsAre("b")));
        EXPECT_CALL(runner, end());
    }

    executor.BeginGroup("a");
    executor.FinishGroup();

    executor.BeginGroup("b");
    executor.FinishGroup();
}

TEST_F(ExecutorImplTest, HasInputNoDispatcherNoChild) {
    MockSource& input = add_input("input");
    EXPECT_CALL(input, RequireObject(_, _));

    // input_scope_level = output_scope_level = 1
    MockRunnerExecutor& executor = start_test(1, 1);
    MockRunner& runner = *executor.runner();
    {
        InSequence in_sequence;

        EXPECT_CALL(runner, begin(ElementsAre("a", "0")));
        EXPECT_CALL(runner, got("input", 0));
        EXPECT_CALL(runner, done("input"));
        EXPECT_CALL(runner, end());

        EXPECT_CALL(runner, begin(ElementsAre("a", "1")));
        EXPECT_CALL(runner, got("input", 1));
        EXPECT_CALL(runner, done("input"));
        EXPECT_CALL(runner, end());

        EXPECT_CALL(runner, begin(ElementsAre("b", "3")));
        EXPECT_CALL(runner, done("input"));
        EXPECT_CALL(runner, end());
    }

    executor.BeginGroup("a");
    {
        executor.BeginGroup("0");
        executor.FinishGroup();
        input.DispatchObject(ObjectPtr(0));
        input.DispatchDone();

        executor.BeginGroup("1");
        executor.FinishGroup();
        input.DispatchObject(ObjectPtr(1));
        input.DispatchDone();
    }
    executor.FinishGroup();

    executor.BeginGroup("b");
    {
        executor.BeginGroup("3");
        executor.FinishGroup();
        input.DispatchDone();
    }
    executor.FinishGroup();
}

TEST_F(ExecutorImplTest, NoInputHasDispatcherNoChild) {
    add_dispatcher("output");
    MockListener& listener = add_output("output");

    // input_scope_level = 0,  output_scope_level = 1
    MockRunnerExecutor& executor = start_test(0, 1);
    MockRunner& runner = *executor.runner();
    {
        InSequence in_sequence;

        {
            EXPECT_CALL(runner, begin(ElementsAre("a"))).WillOnce(DoAll(
                    BeginSubGroup(&runner, "0"),
                    Emit(&runner, "output", ObjectPtr(0x00))
            ));
            EXPECT_CALL(listener, GotObject(ObjectPtr(0x00)));

            EXPECT_CALL(runner, end()).WillOnce(DoAll(
                    Emit(&runner, "output", ObjectPtr(0x01)),
                    EndSubGroup(&runner)
            ));
            EXPECT_CALL(listener, GotObject(ObjectPtr(0x01)));
            EXPECT_CALL(listener, GotDone());
        }

        {
            EXPECT_CALL(runner, begin(ElementsAre("b"))).WillOnce(DoAll(
                    BeginSubGroup(&runner, "1"),
                    Emit(&runner, "output", ObjectPtr(0x10)),
                    EmitDone(&runner, "output"),
                    EndSubGroup(&runner)
            ));
            EXPECT_CALL(listener, GotObject(ObjectPtr(0x10)));

            EXPECT_CALL(runner, end()).WillOnce(DoAll(
                    BeginSubGroup(&runner, "2"),
                    Emit(&runner, "output", ObjectPtr(0x11)),
                    EndSubGroup(&runner)
            ));
            EXPECT_CALL(listener, GotObject(ObjectPtr(0x11)));
            EXPECT_CALL(listener, GotDone());
        }

        {
            EXPECT_CALL(runner, begin(ElementsAre("c")));
            EXPECT_CALL(runner, end());
            EXPECT_CALL(listener, GotDone());
        }
    }

    executor.BeginGroup("a");
    executor.FinishGroup();

    executor.BeginGroup("b");
    executor.FinishGroup();

    executor.BeginGroup("c");
    executor.FinishGroup();
}

TEST_F(ExecutorImplTest, NoInputNoDispatcherHasChild) {
    MockChild& child = add_child();

    // input_scope_level = 0,  output_scope_level = 0
    MockRunnerExecutor& executor = start_test(0, 0);
    MockRunner& runner = *executor.runner();

    {
        InSequence in_sequence;

        {
            EXPECT_CALL(child, begin_group("a"));
            EXPECT_CALL(child, finish_group());
            EXPECT_CALL(runner, begin(ElementsAre("a")));
            EXPECT_CALL(runner, end());
        }

        {
            EXPECT_CALL(child, begin_group("b"));
            EXPECT_CALL(child, finish_group());
            EXPECT_CALL(runner, begin(ElementsAre("b")));
            EXPECT_CALL(runner, end());
        }
    }

    executor.BeginGroup("a");
    executor.FinishGroup();

    executor.BeginGroup("b");
    executor.FinishGroup();
}

TEST_F(ExecutorImplTest, HasInputHasDispatcherHasChild) {
    add_dispatcher("runner-output");

    MockSource& input = add_input("input");
    EXPECT_CALL(input, RequireObject(_, _));

    MockChild& child = add_child();
    child.add_input("runner-output");
    MockSource& output = child.add_output("child-output");
    EXPECT_CALL(output, RequireObject(_, _));

    MockListener& listener = add_output("child-output");

    // input_scope_level = 0,  output_scope_level = 1
    MockRunnerExecutor& executor = start_test(0, 1);
    MockRunner& runner = *executor.runner();

    {
        InSequence in_sequence;

        {
            EXPECT_CALL(child, begin_group("a"));
            EXPECT_CALL(runner, begin(ElementsAre("a")));
            {
                EXPECT_CALL(runner, got("input", 0)).WillOnce(DoAll(
                        BeginSubGroup(&runner, "0"),
                        Emit(&runner, "runner-output", ObjectPtr(0x10)),
                        EndSubGroup(&runner)
                ));
                EXPECT_CALL(child, begin_group("0"));
                EXPECT_CALL(child, finish_group());
                EXPECT_CALL(child, got("runner-output", 0x10));
                EXPECT_CALL(child, done("runner-output"));
            }
            {
                EXPECT_CALL(runner, got("input", 1));
            }
            EXPECT_CALL(runner, done("input"));
            EXPECT_CALL(runner, end());
            EXPECT_CALL(child, finish_group()).WillOnce(
                EmitDone(&child, "child-output")
            );
            EXPECT_CALL(listener, GotDone());
        }

        {
            EXPECT_CALL(child, begin_group("b"));
            EXPECT_CALL(runner, begin(ElementsAre("b")));
            {
                EXPECT_CALL(runner, got("input", 2)).WillOnce(
                        BeginSubGroup(&runner, "0")
                );
                EXPECT_CALL(child, begin_group("0"));
                EXPECT_CALL(child, finish_group());
            }
            {
                EXPECT_CALL(runner, got("input", 3)).WillOnce(
                        Emit(&runner, "runner-output", ObjectPtr(0x11))
                );
                EXPECT_CALL(child, got("runner-output", 0x11)).WillOnce(
                        Emit(&child, "child-output", ObjectPtr(0x20))
                );
                EXPECT_CALL(listener, GotObject(ObjectPtr(0x20)));
            }
            {
                EXPECT_CALL(runner, got("input", 4)).WillOnce(
                        EndSubGroup(&runner)
                );
                EXPECT_CALL(child, done("runner-output"));
            }
            EXPECT_CALL(runner, done("input"));
            EXPECT_CALL(runner, end());
            EXPECT_CALL(child, finish_group()).WillOnce(
                    EmitDone(&child, "child-output")
            );
            EXPECT_CALL(listener, GotDone());
        }
    }

    executor.BeginGroup("a");
    executor.FinishGroup();
    input.DispatchObject(ObjectPtr(0));
    input.DispatchObject(ObjectPtr(1));
    input.DispatchDone();

    executor.BeginGroup("b");
    executor.FinishGroup();
    input.DispatchObject(ObjectPtr(2));
    input.DispatchObject(ObjectPtr(3));
    input.DispatchObject(ObjectPtr(4));
    input.DispatchDone();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
