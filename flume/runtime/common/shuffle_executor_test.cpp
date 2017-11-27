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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/runtime/common/shuffle_executor.h"

#include <cstring>
#include <list>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_partitioner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/mock_dispatcher.h"
#include "flume/runtime/testing/mock_executor_context.h"
#include "flume/runtime/testing/mock_source.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::ElementsAre;
using ::testing::Matcher;
using ::testing::InSequence;
using ::testing::IsEmpty;
using ::testing::ResultOf;
using ::testing::Return;

using core::Entity;
using core::KeyReader;
using core::Objector;
using core::Partitioner;
using core::EncodePartition;

namespace shuffle {

class ShuffleExecutorTest : public ::testing::Test {
protected:
    typedef PbShuffleExecutor::MergeSource MergeSource;

    virtual void SetUp() {
        _message.set_type(PbExecutor::SHUFFLE);

        PbScope* scope = _message.mutable_shuffle_executor()->mutable_scope();
        scope->set_id("fake_scope");
        scope->set_father("fake_father_scope");
        scope->set_is_sorted(false);

        _dataset = _dataset_manager.GetDataset("");
        _dataset->Discard()->Done();
    }

    virtual void TearDown() {
        _dataset->Release();
    }

    PbShuffleExecutor* message() {
        return _message.mutable_shuffle_executor();
    }

    void set_scope_level(uint32_t scope_level) {
        _message.set_scope_level(scope_level);
    }

    MockExecutorContext& start_test(ExecutorRunner* runner) {
        runner->setup(_message, &_context);
        return _context;
    }

    PbShuffleNode* add_shuffle_input(const MockKeyReader& key_reader,
                                     MockSource* input, MockDispatcher* output) {
        PbShuffleNode message;
        message.set_type(PbShuffleNode::KEY);
        *message.mutable_key_reader() =
                Entity<KeyReader>::Of<MockKeyReader>(key_reader.config()).ToProtoMessage();

        return add_shuffle_node(message, input, output);
    }

    PbShuffleNode* add_shuffle_input(const MockPartitioner& partitioner,
                                     MockSource* input, MockDispatcher* output) {
        PbShuffleNode message;
        message.set_type(PbShuffleNode::SEQUENCE);
        *message.mutable_partitioner() =
                Entity<Partitioner>::Of<MockPartitioner>(partitioner.config()).ToProtoMessage();

        return add_shuffle_node(message, input, output);
    }

    PbShuffleNode* add_broadcast_input(MockSource* input, MockDispatcher* output) {
        PbShuffleNode message;
        message.set_type(PbShuffleNode::BROADCAST);

        return add_shuffle_node(message, input, output);
    }

    PbShuffleNode* add_shuffle_node(const PbShuffleNode& message,
                                    MockSource* input, MockDispatcher* output) {
        PbLogicalPlanNode* node = _message.mutable_shuffle_executor()->add_node();
        node->set_id(toft::CreateCanonicalUUIDString());
        node->set_type(PbLogicalPlanNode::SHUFFLE_NODE);
        node->set_scope("fake_scope");
        *node->mutable_objector() = Entity<Objector>::Of<FakeObjector>("").ToProtoMessage();

        PbShuffleNode* shuffle_node = node->mutable_shuffle_node();
        shuffle_node->CopyFrom(message);
        shuffle_node->set_from(toft::CreateCanonicalUUIDString());

        _message.add_input(shuffle_node->from());
        if (message.type() == PbShuffleNode::BROADCAST) {
            EXPECT_CALL(*input, RequireBinary(_, _)).Times(AnyNumber());
        } else {
            EXPECT_CALL(*input, RequireObjectAndBinary(_, _)).Times(AnyNumber());
        }
        EXPECT_CALL(_context, input(shuffle_node->from())).WillRepeatedly(Return(input));

        PbExecutor::Dispatcher* dispatcher = _message.add_dispatcher();
        dispatcher->set_identity(node->id());
        EXPECT_CALL(_context, output(node->id())).WillRepeatedly(Return(output));

        uint32_t input_scope_level = _message.scope_level() - 1;
        EXPECT_CALL(*output, GetScopeLevel()).WillRepeatedly(Return(input_scope_level));
        EXPECT_CALL(*output, GetDataset(input_scope_level)).WillRepeatedly(Return(_dataset));

        return shuffle_node;
    }

    PbShuffleExecutor::MergeSource* add_merge_input(MockSource* input, MockDispatcher* output) {
        PbShuffleExecutor::MergeSource* merge_source =
                _message.mutable_shuffle_executor()->add_source();
        merge_source->set_input(toft::CreateCanonicalUUIDString());
        merge_source->set_output(toft::CreateCanonicalUUIDString());

        EXPECT_CALL(_context, input(merge_source->input())).WillOnce(Return(input));
        EXPECT_CALL(_context, output(merge_source->output())).WillOnce(Return(output));

        EXPECT_CALL(*input, RequireStream(Source::REQUIRE_KEY | Source::REQUIRE_BINARY, _, _));

        return merge_source;
    }

    const toft::StringPiece uint32(uint32_t partition) {
        std::string* buffer = new std::string(core::EncodePartition(partition));
        _strings.push_back(buffer);

        return *buffer;
    }

protected:
    PbExecutor _message;
    MockExecutorContext _context;

    MemoryDatasetManager _dataset_manager;
    Dataset* _dataset;
    boost::ptr_vector<std::string> _strings;
};

TEST_F(ShuffleExecutorTest, SortedLocalRunner) {
    message()->mutable_scope()->set_is_sorted(true);

    MockKeyReader& key_reader = MockKeyReader::Mock();
    {
        key_reader.KeyOf(ObjectPtr(0)) = "a";
        key_reader.KeyOf(ObjectPtr(1)) = "b";
        key_reader.KeyOf(ObjectPtr(2)) = "c";
        key_reader.KeyOf(ObjectPtr(3)) = "d";
    }

    MockSource input;
    MockDispatcher output;
    add_shuffle_input(key_reader, &input, &output);

    LocalRunner<std::string, true> runner;
    MockExecutorContext& context = start_test(&runner);
    {
        InSequence in_sequence;

        EXPECT_CALL(context, begin_sub_group("a"));
        EXPECT_CALL(output, EmitBinary("0"));
        EXPECT_CALL(context, end_sub_group());

        EXPECT_CALL(context, begin_sub_group("b"));
        EXPECT_CALL(output, EmitBinary("1"));
        EXPECT_CALL(context, end_sub_group());

        EXPECT_CALL(context, begin_sub_group("c"));
        EXPECT_CALL(output, EmitBinary("2"));
        EXPECT_CALL(context, end_sub_group());

        EXPECT_CALL(context, begin_sub_group("d"));
        EXPECT_CALL(output, EmitBinary("3"));
        EXPECT_CALL(context, end_sub_group());
    }

    runner.begin_group(as_keys("key"));
    input.DispatchObjectAndBinary(ObjectPtr(0), "0");
    input.DispatchObjectAndBinary(ObjectPtr(2), "2");
    input.DispatchObjectAndBinary(ObjectPtr(1), "1");
    input.DispatchObjectAndBinary(ObjectPtr(3), "3");
    input.DispatchDone();
    runner.end_group();
}

TEST_F(ShuffleExecutorTest, CoGroup) {
    MockKeyReader& key_reader = MockKeyReader::Mock();
    {
        key_reader.KeyOf(ObjectPtr(10)) = "a";
        key_reader.KeyOf(ObjectPtr(11)) = "a";
        key_reader.KeyOf(ObjectPtr(20)) = "b";
        key_reader.KeyOf(ObjectPtr(31)) = "c";
    }

    MockSource input_0, input_1;
    MockDispatcher output_0, output_1;
    add_shuffle_input(key_reader, &input_0, &output_0);
    add_shuffle_input(key_reader, &input_1, &output_1);

    LocalRunner<std::string, false> runner;
    MockExecutorContext& context = start_test(&runner);
    {
        InSequence in_sequence;

        EXPECT_CALL(context, begin_sub_group("a"));
        EXPECT_CALL(output_0, EmitBinary("10"));
        EXPECT_CALL(output_1, EmitBinary("11"));
        EXPECT_CALL(context, end_sub_group());

        EXPECT_CALL(context, begin_sub_group("b"));
        EXPECT_CALL(output_0, EmitBinary("20"));
        EXPECT_CALL(context, end_sub_group());

        EXPECT_CALL(context, begin_sub_group("c"));
        EXPECT_CALL(output_1, EmitBinary("31"));
        EXPECT_CALL(context, end_sub_group());
    }

    runner.begin_group(as_keys("key_a"));
    input_0.DispatchObjectAndBinary(ObjectPtr(10), "10");
    input_1.DispatchObjectAndBinary(ObjectPtr(11), "11");
    input_0.DispatchDone();
    input_1.DispatchDone();
    runner.end_group();

    runner.begin_group(as_keys("key_b"));
    input_0.DispatchObjectAndBinary(ObjectPtr(20), "20");
    input_0.DispatchDone();
    input_1.DispatchDone();
    runner.end_group();

    runner.begin_group(as_keys("key_c"));
    input_1.DispatchObjectAndBinary(ObjectPtr(31), "31");
    input_0.DispatchDone();
    input_1.DispatchDone();
    runner.end_group();
}

TEST_F(ShuffleExecutorTest, Bucket) {
    message()->mutable_scope()->mutable_bucket_scope()->set_bucket_size(2);

    MockPartitioner& partitioner = MockPartitioner::Mock();
    partitioner.PartitionOf(ObjectPtr(1), 2) = 1;

    MockSource input_0, input_1;
    MockDispatcher output_0, output_1;
    add_broadcast_input(&input_0, &output_0);
    add_shuffle_input(partitioner, &input_1, &output_1);

    static const std::string PARTITIONS[] = { EncodePartition(0), EncodePartition(1) };
    static const toft::StringPiece KEYS[] = { PARTITIONS[0], PARTITIONS[1] };

    LocalRunner<uint32_t, false> runner;
    MockExecutorContext& context = start_test(&runner);
    {
        InSequence in_sequence;

        EXPECT_CALL(output_0,
                    BeginGroup(KEYS[0], Matcher<Dataset*>(ResultOf(ToList, ElementsAre("0")))));
        EXPECT_CALL(output_1,
                    BeginGroup(KEYS[0], Matcher<Dataset*>(ResultOf(ToList, IsEmpty()))));
        EXPECT_CALL(context, begin_sub_group(PARTITIONS[0]));
        EXPECT_CALL(context, end_sub_group());

        EXPECT_CALL(output_0,
                    BeginGroup(KEYS[1], Matcher<Dataset*>(ResultOf(ToList, ElementsAre("0")))));
        EXPECT_CALL(output_1,
                    BeginGroup(KEYS[1], Matcher<Dataset*>(ResultOf(ToList, ElementsAre("1")))));
        EXPECT_CALL(context, begin_sub_group(PARTITIONS[1]));
        EXPECT_CALL(context, end_sub_group());
    }

    runner.begin_group(as_keys("key"));
    input_0.DispatchBinary("0");
    input_1.DispatchObjectAndBinary(ObjectPtr(1), "1");
    input_0.DispatchDone();
    input_1.DispatchDone();
    runner.end_group();
}

TEST_F(ShuffleExecutorTest, CombineSequence) {
    set_scope_level(1);
    message()->mutable_scope()->mutable_bucket_scope()->set_bucket_size(2);

    MockSource input;
    MockDispatcher output;
    add_merge_input(&input, &output)->set_priority(0);

    CombineRunner<uint32_t> runner;
    MockExecutorContext& context = start_test(&runner);

    {
        InSequence in_sequence;

        {
            EXPECT_CALL(context, begin_sub_group(EncodePartition(0)));
            EXPECT_CALL(context, end_sub_group());

            EXPECT_CALL(context, begin_sub_group(EncodePartition(1)));
            EXPECT_CALL(context, end_sub_group());
        }

        {
            EXPECT_CALL(context, begin_sub_group(EncodePartition(0)));
            EXPECT_CALL(context, close_prior_outputs(0));
            EXPECT_CALL(output, EmitBinary(as_keys(uint32(0), uint32(0), uint32(1)), "a"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, end_sub_group());

            EXPECT_CALL(context, begin_sub_group(EncodePartition(1)));
            EXPECT_CALL(context, close_prior_outputs(0));
            EXPECT_CALL(output, EmitBinary(as_keys(uint32(0), uint32(1), uint32(0)), "b"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, end_sub_group());
        }

        {
            EXPECT_CALL(context, begin_sub_group(EncodePartition(0)));
            EXPECT_CALL(context, end_sub_group());

            EXPECT_CALL(context, begin_sub_group(EncodePartition(1)));
            EXPECT_CALL(context, close_prior_outputs(0));
            EXPECT_CALL(output, EmitBinary(as_keys(uint32(1), uint32(1)), "a"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, close_prior_outputs(0));
            EXPECT_CALL(output, EmitBinary(as_keys(uint32(0), uint32(1)), "b"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, end_sub_group());
        }
    }

    runner.begin_group(as_keys());
    runner.end_group();

    runner.begin_group(as_keys());
    input.DispatchStream(as_keys(uint32(0), uint32(0), uint32(1)), NULL, "a");
    input.DispatchStream(as_keys(uint32(0), uint32(1), uint32(0)), NULL, "b");
    runner.end_group();

    runner.begin_group(as_keys());
    input.DispatchStream(as_keys(uint32(1), uint32(1)), NULL, "a");
    input.DispatchStream(as_keys(uint32(0), uint32(1)), NULL, "b");
    runner.end_group();
}

TEST_F(ShuffleExecutorTest, CombineString) {
    set_scope_level(1);

    MockKeyReader& key_reader = MockKeyReader::Mock();
    {
        key_reader.KeyOf(ObjectPtr(1)) = "a";
        key_reader.KeyOf(ObjectPtr(3)) = "c";
    }

    MockSource shuffle_input, merge_input;
    MockDispatcher shuffle_output, merge_output;
    add_shuffle_input(key_reader, &shuffle_input, &shuffle_output);
    add_merge_input(&merge_input, &merge_output)->set_priority(1);

    CombineRunner<std::string> runner;
    MockExecutorContext& context = start_test(&runner);
    {
        InSequence in_sequence;

        EXPECT_CALL(shuffle_output,
                    BeginGroup(toft::StringPiece("a"),
                               Matcher<Dataset*>(ResultOf(ToList, ElementsAre("1")))));
        EXPECT_CALL(context, begin_sub_group("a"));
        EXPECT_CALL(context, close_prior_outputs(1));
        EXPECT_CALL(merge_output, EmitBinary(as_keys("", "a", "1"), "11")).WillOnce(Return(true));
        EXPECT_CALL(context, end_sub_group());

        EXPECT_CALL(context, begin_sub_group("b"));
        EXPECT_CALL(context, close_prior_outputs(1));
        EXPECT_CALL(merge_output, EmitBinary(as_keys("", "b", "2"), "12")).WillOnce(Return(true));
        EXPECT_CALL(context, end_sub_group());

        EXPECT_CALL(shuffle_output,
                    BeginGroup(toft::StringPiece("c"),
                               Matcher<Dataset*>(ResultOf(ToList, ElementsAre("3")))));
        EXPECT_CALL(context, begin_sub_group("c"));
        EXPECT_CALL(context, end_sub_group());
    }

    runner.begin_group(as_keys(""));
    shuffle_input.DispatchObjectAndBinary(ObjectPtr(1), "1");
    shuffle_input.DispatchObjectAndBinary(ObjectPtr(3), "3");
    shuffle_input.DispatchDone();
    merge_input.DispatchStream(as_keys("", "a", "1"), NULL, "11");
    merge_input.DispatchStream(as_keys("", "b", "2"), NULL, "12");
    runner.end_group();
}

TEST_F(ShuffleExecutorTest, PriorityMerge) {
    set_scope_level(1);

    MockSource prepared_input, ready_input, passby_input;
    MockDispatcher prepared_output, ready_output, passby_output;

    add_merge_input(&prepared_input, &prepared_output)->set_priority(0);
    add_merge_input(&ready_input, &ready_output)->set_priority(1);
    add_merge_input(&passby_input, &passby_output)->set_priority(2);

    MergeRunner runner;
    MockExecutorContext& context = start_test(&runner);
    {
        InSequence in_sequence;

        {
            EXPECT_CALL(context, begin_sub_group("a"));
            EXPECT_CALL(context, close_prior_outputs(0));
            EXPECT_CALL(prepared_output, EmitBinary(as_keys("global", "a"), "1"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, close_prior_outputs(1));
            EXPECT_CALL(ready_output, EmitBinary(as_keys("global", "a"), "2"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, close_prior_outputs(2));
            EXPECT_CALL(passby_output, EmitBinary(as_keys("global", "a", "0"), "3"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, end_sub_group());
        }
        {
            EXPECT_CALL(context, begin_sub_group("b"));
            EXPECT_CALL(context, close_prior_outputs(1));
            EXPECT_CALL(ready_output, EmitBinary(as_keys("global", "b"), "22"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, end_sub_group());
        }
        {
            EXPECT_CALL(context, begin_sub_group("c"));
            EXPECT_CALL(context, close_prior_outputs(2));
            EXPECT_CALL(passby_output, EmitBinary(as_keys("global", "c", "1"), "33"))
                .WillOnce(Return(true));
            EXPECT_CALL(context, end_sub_group());
        }
    }

    runner.begin_group(as_keys("global"));
    {
        prepared_input.DispatchStream(as_keys("global", "a"), NULL, "1");
        ready_input.DispatchStream(as_keys("global", "a"), NULL, "2");
        passby_input.DispatchStream(as_keys("global", "a", "0"), NULL, "3");
    }
    {
        ready_input.DispatchStream(as_keys("global", "b"), NULL, "22");
    }
    runner.end_group();

    runner.begin_group(as_keys("global"));
    runner.end_group();

    runner.begin_group(as_keys("global"));
    {
        passby_input.DispatchStream(as_keys("global", "c", "1"), NULL, "33");
    }
    runner.end_group();
}

TEST_F(ShuffleExecutorTest, LocalDistribute) {
    MockSource shuffle_input, merge_input;
    MockDispatcher shuffle_output, merge_output;

    MockPartitioner& partitioner = MockPartitioner::Mock();
    add_shuffle_input(partitioner, &shuffle_input, &shuffle_output);
    EXPECT_CALL(shuffle_input, RequireObject(_, _));

    add_merge_input(&merge_input, &merge_output);

    LocalDistributeRunner runner;
    runner.set_partition(1);
    MockExecutorContext& context = start_test(&runner);
    {
        InSequence in_sequence;

        {
            EXPECT_CALL(context, begin_sub_group(core::EncodePartition(1)));
            EXPECT_CALL(context, end_sub_group());
        }

        {
            EXPECT_CALL(context, begin_sub_group(core::EncodePartition(1)));
            EXPECT_CALL(merge_output, EmitBinary(as_keys("", uint32(1)), "1"))
                .WillOnce(Return(true));
            EXPECT_CALL(shuffle_output, EmitObject(ObjectPtr(11)))
                .WillOnce(Return(true));
            EXPECT_CALL(context, end_sub_group());
        }
    }

    runner.begin_group(as_keys(""));
    runner.end_group();

    runner.begin_group(as_keys(""));
    merge_input.DispatchStream(as_keys("", uint32(1)), NULL, "1");
    shuffle_input.DispatchObject(ObjectPtr(11));
    runner.end_group();
}

}  // namespace shuffle

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
