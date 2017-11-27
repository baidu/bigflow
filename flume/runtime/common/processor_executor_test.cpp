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

#include "flume/runtime/common/processor_executor.h"

#include <string>
#include <vector>

#include "boost/assign.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/core/emitter.h"
#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/processor.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/fake_pointer.h"
#include "flume/runtime/testing/mock_dispatcher.h"
#include "flume/runtime/testing/mock_executor_context.h"
#include "flume/runtime/testing/mock_listener.h"
#include "flume/runtime/testing/mock_source.h"
#include "flume/runtime/testing/mock_status_table.h"
#include "flume/runtime/testing/test_marker.h"

namespace baidu {
namespace flume {
namespace runtime {

using namespace boost::assign;

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::Each;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::IsNull;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;
using ::testing::SetArgPointee;

using core::Emitter;
using core::Entity;
using core::Processor;
using core::Objector;

static const char* kOutput = "fake_output_id";
static const unsigned kScopeLevel = 1;
static const std::vector<Executor*> kEmptyChilds;

class ProcessorExecutorTest : public ::testing::Test {
protected:
    static const bool PREPARED = true;
    static const bool NON_PREPARED = false;
    typedef internal::ProcessorRunner ProcessorRunner;

    ProcessorExecutorTest() : _processor(MockProcessor::Mock()) {
        _message.set_type(PbExecutor::PROCESSOR);
        _message.set_scope_level(kScopeLevel);
        _message.add_output(kOutput);

        PbExecutor::Dispatcher* dispatcher = _message.add_dispatcher();
        dispatcher->set_identity(kOutput);
        *dispatcher->mutable_objector() = Entity<Objector>::Of<FakeObjector>("").ToProtoMessage();
        EXPECT_CALL(_context, output(kOutput)).WillRepeatedly(Return(&_output));

        PbProcessorExecutor* sub_message = _message.mutable_processor_executor();
        sub_message->set_identity(kOutput);
        *sub_message->mutable_processor() =
                Entity<Processor>::Of<MockProcessor>(_processor.config()).ToProtoMessage();
    }

    MockSource& add_stream_input(const std::string& identity) {
        _message.add_input(identity);

        PbProcessorExecutor::Source* config = _message.mutable_processor_executor()->add_source();
        config->set_identity(identity);
        config->set_type(PbProcessorExecutor::REQUIRE_STREAM);

        MockSource& source = *new MockSource();
        _inputs.push_back(&source);
        EXPECT_CALL(source, RequireObject(_, IsNull()));
        EXPECT_CALL(_context, input(identity)).WillRepeatedly(Return(&source));

        return source;
    }

    MockSource& add_iterator_input(const std::string& identity, bool is_prepared) {
        _message.add_input(identity);

        PbProcessorExecutor::Source* config = _message.mutable_processor_executor()->add_source();
        config->set_identity(identity);
        config->set_type(PbProcessorExecutor::REQUIRE_ITERATOR);
        config->set_is_prepared(is_prepared);

        MockSource& source = *new MockSource();
        _inputs.push_back(&source);
        EXPECT_CALL(source, RequireIterator(_, _));
        EXPECT_CALL(_context, input(identity)).WillRepeatedly(Return(&source));

        return source;
    }

    void add_dummy_input(const std::string& identity, bool is_prepared) {
        PbProcessorExecutor::Source* config = _message.mutable_processor_executor()->add_source();
        config->set_identity(identity);
        config->set_type(PbProcessorExecutor::DUMMY);
        config->set_is_prepared(is_prepared);
    }

    void set_partial_key_number(uint32_t number) {
        _message.mutable_processor_executor()->set_partial_key_number(number);
    }

    ProcessorRunner& start_test() {
        _runner.reset(new NormalProcessorRunner());
        _runner->setup(_message, &_context);
        return *_runner;
    }

protected:
    MockProcessor& _processor;
    MockExecutorContext _context;
    MockDispatcher _output;

private:
    PbExecutor _message;
    boost::ptr_vector<MockSource> _inputs;
    boost::scoped_ptr<ProcessorRunner> _runner;
};

TEST_F(ProcessorExecutorTest, EmptyGroup) {
    set_partial_key_number(2);
    add_dummy_input("input", NON_PREPARED);
    {
        InSequence in_sequence;

        EXPECT_CALL(_processor, BeginGroup(ElementsAre("global", "a"), ElementsAre(IsNull())));
        EXPECT_CALL(_processor, EndGroup());

        EXPECT_CALL(_processor, BeginGroup(ElementsAre("global", "b"), ElementsAre(IsNull())));
        EXPECT_CALL(_processor, EndGroup());
    }
    ProcessorRunner& runner = start_test();

    runner.begin_group(list_of<toft::StringPiece>("global")("a")("0"));
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("global")("b")("1"));
    runner.end_group();
}

TEST_F(ProcessorExecutorTest, SingleStreamInput) {
    MockSource& source = add_stream_input("input");
    {
        InSequence in_sequence;

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("a"), ElementsAre(IsNull())))
                .WillOnce(EmitAndExpect(&_processor, ObjectPtr(11), true));
            EXPECT_CALL(_output, EmitObject(ObjectPtr(11)));

            EXPECT_CALL(_processor, Process(0, ObjectPtr(0)))
                .WillOnce(EmitAndExpect(&_processor, ObjectPtr(12), true));
            EXPECT_CALL(_output, EmitObject(ObjectPtr(12)));

            EXPECT_CALL(_processor, EndGroup())
                .WillOnce(EmitAndExpect(&_processor, ObjectPtr(13), true));
            EXPECT_CALL(_output, EmitObject(ObjectPtr(13)));
        }

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("b"), ElementsAre(IsNull())));
            EXPECT_CALL(_processor, EndGroup())
                .WillOnce(DoAll(EmitDone(&_processor),
                                EmitAndExpect(&_processor, ObjectPtr(21), false)));
        }

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("c"), ElementsAre(IsNull())));

            EXPECT_CALL(_processor, Process(0, ObjectPtr(1))).WillOnce(EmitDone(&_processor));
            EXPECT_CALL(source, Done());
            EXPECT_CALL(_output, Done());

            EXPECT_CALL(_processor, EndGroup())
                .WillOnce(EmitAndExpect(&_processor, ObjectPtr(31), false));
        }

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("d"), ElementsAre(IsNull())))
                .WillOnce(EmitDone(&_processor));
            EXPECT_CALL(source, Done());
            EXPECT_CALL(_output, Done());

            EXPECT_CALL(_processor, EndGroup());
        }
    }
    ProcessorRunner& runner = start_test();

    runner.begin_group(list_of<toft::StringPiece>("a"));
    source.DispatchObject(ObjectPtr(0));
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("b"));
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("c"));
    source.DispatchObject(ObjectPtr(1));
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("d"));
    runner.end_group();
}

TEST_F(ProcessorExecutorTest, SinglePreparedInput) {
    MockSource& source = add_iterator_input("input", PREPARED);
    {
        InSequence in_sequence;

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("a"),
                                               ElementsAre(IterateOver(0, 9))))
                .WillOnce(EmitAndExpect(&_processor, ObjectPtr(11), true));
            EXPECT_CALL(_output, EmitObject(ObjectPtr(11)));

            EXPECT_CALL(_processor, EndGroup())
                .WillOnce(EmitAndExpect(&_processor, ObjectPtr(12), true));
            EXPECT_CALL(_output, EmitObject(ObjectPtr(12)));
            EXPECT_CALL(source, IteratorDone());
        }

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("b"),
                                               ElementsAre(IterateOver(10, 19))))
                .WillOnce(EmitDone(&_processor));
            EXPECT_CALL(source, Done());
            EXPECT_CALL(_output, Done());

            EXPECT_CALL(_processor, EndGroup())
                .WillOnce(EmitAndExpect(&_processor, ObjectPtr(21), false));
            EXPECT_CALL(source, IteratorDone());
        }

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("c"),
                                               ElementsAre(IterateOver(20, 29))));

            EXPECT_CALL(_processor, EndGroup()).WillOnce(EmitDone(&_processor));
            EXPECT_CALL(source, IteratorDone());
        }
    }
    ProcessorRunner& runner = start_test();

    runner.begin_group(list_of<toft::StringPiece>("a"));
    source.DispatchIterator(0, 9);
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("b"));
    source.DispatchIterator(10, 19);
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("c"));
    source.DispatchIterator(20, 29);
    runner.end_group();
}

TEST_F(ProcessorExecutorTest, MultiIteratorInput) {
    MockSource& source_0 = add_iterator_input("input-0", PREPARED);
    MockSource& source_1 = add_iterator_input("input-1", NON_PREPARED);
    add_dummy_input("input-2", PREPARED);
    {
        InSequence in_sequence;

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("a"),
                                               ElementsAre(IterateOver(0, 4),
                                                           IsNull(),
                                                           IsEmptyIterator())));

            EXPECT_CALL(_processor, Process(1, ObjectPtr(5)));
            EXPECT_CALL(_processor, Process(1, ObjectPtr(6)));
            EXPECT_CALL(source_1, IteratorDone());

            EXPECT_CALL(_processor, EndGroup());
            EXPECT_CALL(source_0, IteratorDone());
        }

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("b"),
                                               ElementsAre(IterateOver(10, 14),
                                                           IsNull(),
                                                           IsEmptyIterator())));

            EXPECT_CALL(_processor, Process(1, ObjectPtr(15))).WillOnce(EmitDone(&_processor));
            EXPECT_CALL(source_0, Done());
            EXPECT_CALL(source_1, Done());
            EXPECT_CALL(_output, Done());
            EXPECT_CALL(source_1, IteratorDone());

            EXPECT_CALL(_processor, EndGroup());
            EXPECT_CALL(source_0, IteratorDone());
        }

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("c"),
                                               ElementsAre(IterateOver(20, 24),
                                                           IsNull(),
                                                           IsEmptyIterator())))
                .WillOnce(EmitDone(&_processor));
            EXPECT_CALL(source_0, Done());
            EXPECT_CALL(source_1, Done());
            EXPECT_CALL(_output, Done());
            EXPECT_CALL(source_1, IteratorDone());

            EXPECT_CALL(_processor, EndGroup());
            EXPECT_CALL(source_0, IteratorDone());
        }
    }
    ProcessorRunner& runner = start_test();

    runner.begin_group(list_of<toft::StringPiece>("a"));
    source_0.DispatchIterator(0, 4);
    source_1.DispatchIterator(5, 6);
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("b"));
    source_0.DispatchIterator(10, 14);
    source_1.DispatchIterator(15, 16);
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("c"));
    source_1.DispatchIterator(25, 26);
    source_0.DispatchIterator(20, 24);
    runner.end_group();
}

TEST_F(ProcessorExecutorTest, PreparedAndNonPreparedInput) {
    MockSource& source_0 = add_stream_input("input-0");
    MockSource& source_1 = add_iterator_input("input-1", PREPARED);
    MockSource& source_2 = add_iterator_input("input-2", NON_PREPARED);
    {
        InSequence in_sequence;

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("a"),
                                               ElementsAre(IsNull(),
                                                           IterateOver(0, 4),
                                                           IsNull())));

            EXPECT_CALL(_processor, Process(2, ObjectPtr(5)));
            EXPECT_CALL(_processor, Process(2, ObjectPtr(6)));
            EXPECT_CALL(source_2, IteratorDone());

            EXPECT_CALL(_processor, Process(0, ObjectPtr(7)));

            EXPECT_CALL(_processor, EndGroup());
            EXPECT_CALL(source_1, IteratorDone());
        }

        {
            EXPECT_CALL(_processor, BeginGroup(ElementsAre("b"),
                                               ElementsAre(IsNull(),
                                                           IterateOver(10, 14),
                                                           IsNull())));

            EXPECT_CALL(_processor, Process(0, ObjectPtr(15)));

            EXPECT_CALL(_processor, Process(2, ObjectPtr(16)));
            EXPECT_CALL(_processor, Process(2, ObjectPtr(17)));
            EXPECT_CALL(source_2, IteratorDone());

            EXPECT_CALL(_processor, EndGroup());
            EXPECT_CALL(source_1, IteratorDone());
        }
    }
    ProcessorRunner& runner = start_test();

    runner.begin_group(list_of<toft::StringPiece>("a"));
    source_1.DispatchIterator(0, 4);
    source_2.DispatchIterator(5, 6);
    source_0.DispatchObject(ObjectPtr(7));
    runner.end_group();

    runner.begin_group(list_of<toft::StringPiece>("b"));
    source_1.DispatchIterator(10, 14);
    source_0.DispatchObject(ObjectPtr(15));
    source_2.DispatchIterator(16, 17);
    runner.end_group();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
