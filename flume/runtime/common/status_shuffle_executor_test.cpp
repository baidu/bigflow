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

#include "flume/runtime/common/status_shuffle_executor.h"

#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_ptr.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/core/entity.h"
#include "flume/core/partitioner.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_partitioner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/mock_dispatcher.h"
#include "flume/runtime/testing/mock_executor_base.h"
#include "flume/runtime/testing/mock_source.h"
#include "flume/runtime/testing/mock_status_table.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::InSequence;
using ::testing::IsEmpty;
using ::testing::Matcher;
using ::testing::ResultOf;
using ::testing::Return;
using ::testing::Sequence;

using core::Entity;
using core::KeyReader;
using core::Objector;
using core::Partitioner;
using core::EncodePartition;

class StatusShuffleExecutorTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_message.set_type(PbExecutor::SHUFFLE);
        m_message.set_scope_level(1);
        PbShuffleExecutor* local_shuffle = m_message.mutable_shuffle_executor();
        local_shuffle->set_type(PbShuffleExecutor::LOCAL);
        PbScope* scope = local_shuffle->mutable_scope();
        scope->set_id("fake-scope");
        scope->set_father("");
        scope->set_is_sorted(false);
    }

    virtual void TearDown() {
        for (size_t i = 0; i < m_datasets.size(); ++i) {
            m_datasets[i]->Release();
        }
    }

    void SetBucketSize(uint32_t size) {
        PbScope* scope_message = m_message.mutable_shuffle_executor()->mutable_scope();
        scope_message->set_concurrency(size);
        scope_message->mutable_bucket_scope()->set_bucket_size(size);
    }

    void AddShuffleInput(const MockKeyReader& key_reader,
                         MockSource* input, MockDispatcher* output) {
        PbShuffleNode message;
        message.set_type(PbShuffleNode::KEY);
        *message.mutable_key_reader() =
                Entity<KeyReader>::Of<MockKeyReader>(key_reader.config()).ToProtoMessage();

        AddShuffleNode(message, input, output);
        m_message.mutable_shuffle_executor()->mutable_scope()->set_type(PbScope::GROUP);
    }

    void AddShuffleInput(const MockPartitioner& partitioner,
                         MockSource* input, MockDispatcher* output) {
        PbShuffleNode message;
        message.set_type(PbShuffleNode::SEQUENCE);
        *message.mutable_partitioner() =
                Entity<Partitioner>::Of<MockPartitioner>(partitioner.config()).ToProtoMessage();

        AddShuffleNode(message, input, output);
        m_message.mutable_shuffle_executor()->mutable_scope()->set_type(PbScope::BUCKET);
    }

    void AddShuffleNode(const PbShuffleNode& message,
                        MockSource* input, MockDispatcher* output) {
        PbLogicalPlanNode* node = m_message.mutable_shuffle_executor()->add_node();
        node->set_id(toft::CreateCanonicalUUIDString());
        node->set_type(PbLogicalPlanNode::SHUFFLE_NODE);
        node->set_scope("fake-scope");
        *node->mutable_objector() = Entity<Objector>::Of<FakeObjector>("").ToProtoMessage();

        PbShuffleNode* shuffle_node = node->mutable_shuffle_node();
        shuffle_node->CopyFrom(message);
        shuffle_node->set_from(toft::CreateCanonicalUUIDString());

        m_message.add_input(shuffle_node->from());
        EXPECT_CALL(*input, RequireObjectAndBinary(_, _));
        EXPECT_CALL(m_base, GetInput(shuffle_node->from())).WillRepeatedly(Return(input));

        m_message.add_output(node->id());
        EXPECT_CALL(m_base, GetOutput(node->id())).WillRepeatedly(Return(output));

        Dataset* dataset = m_dataset_manager.GetDataset(node->id());
        dataset->Discard();
        m_datasets.push_back(dataset);

        input->SetObjector(new FakeObjector());

        uint32_t input_scope_level = m_message.scope_level() - 1;
        EXPECT_CALL(*output, GetScopeLevel()).WillRepeatedly(Return(input_scope_level));
        EXPECT_CALL(*output, GetDataset(input_scope_level)).WillRepeatedly(Return(dataset));
    }

    MockExecutorBase& StartTest(StatusShuffleCore* core) {
        core->SetStatusTable(&m_status_table);
        core->Setup(m_message, &m_base);
        return m_base;
    }

    const std::string& UINT32(uint32_t value) {
        std::string* buffer = new std::string(reinterpret_cast<char*>(&value), sizeof(value));
        m_string_deleter.push_back(buffer);

        return *buffer;
    }

protected:
    boost::ptr_vector<std::string> m_string_deleter;

    MemoryDatasetManager m_dataset_manager;
    std::vector<Dataset*> m_datasets;

    PbExecutor m_message;
    MockExecutorBase m_base;
    MockStatusTable m_status_table;
};

TEST_F(StatusShuffleExecutorTest, Bucket) {
    SetBucketSize(2);
    MockPartitioner& partitioner = MockPartitioner::Mock("Bucket");

    MockSource input;
    MockDispatcher output;
    AddShuffleInput(partitioner, &input, &output);

    MockScopeVisitor& visitor = m_status_table.scope_visitor();
    EXPECT_CALL(m_status_table, GetScopeVisitor("fake-scope", ElementsAre("global-key")));
    EXPECT_CALL(visitor, ListEntries())
            .WillOnce(Return(visitor.MakeIterator(EncodePartition(0))));
    EXPECT_CALL(visitor, Release());

    static const std::string PARTITIONS[] = { EncodePartition(0), EncodePartition(1) };
    static const toft::StringPiece KEYS[] = { PARTITIONS[0], PARTITIONS[1] };

    boost::scoped_ptr<StatusShuffleCore> core(CreateStatusShuffleCore(m_message));
    MockExecutorBase& base = StartTest(core.get());
    {
        InSequence in_sequence;

        EXPECT_CALL(output,
                    BeginGroup(KEYS[0],
                               Matcher<Dataset*>(ResultOf(ToList, IsEmpty()))));
        EXPECT_CALL(base, BeginSubGroup(PARTITIONS[0]));
        EXPECT_CALL(base, EndSubGroup());

        EXPECT_CALL(output,
                    BeginGroup(KEYS[1],
                               Matcher<Dataset*>(ResultOf(ToList, IsEmpty()))));
        EXPECT_CALL(base, BeginSubGroup(PARTITIONS[1]));
        EXPECT_CALL(base, EndSubGroup());
    }

    core->BeginGroup(MakeKeys("global-key"));
    input.DispatchDone();
    core->EndGroup();
}

TEST_F(StatusShuffleExecutorTest, ZeroInput) {
    MockKeyReader& key_reader = MockKeyReader::Mock("ZeroInput");
    MockSource input;
    MockDispatcher output;
    AddShuffleInput(key_reader, &input, &output);

    MockScopeVisitor& visitor = m_status_table.scope_visitor();
    EXPECT_CALL(m_status_table, GetScopeVisitor("fake-scope", ElementsAre("global-key")));
    EXPECT_CALL(visitor, ListEntries()).WillOnce(Return(visitor.MakeIterator("a", "b")));
    EXPECT_CALL(visitor, Release());

    boost::scoped_ptr<StatusShuffleCore> core(CreateStatusShuffleCore(m_message));
    MockExecutorBase& base = StartTest(core.get());
    {
        InSequence in_sequence;
        EXPECT_CALL(base, BeginSubGroup("a"));
        EXPECT_CALL(base, EndSubGroup());
        EXPECT_CALL(base, BeginSubGroup("b"));
        EXPECT_CALL(base, EndSubGroup());
    }

    core->BeginGroup(MakeKeys("global-key"));
    input.DispatchDone();
    core->EndGroup();
}

TEST_F(StatusShuffleExecutorTest, MixedInput) {
    MockKeyReader& key_reader = MockKeyReader::Mock("MixedInput");
    {
        key_reader.KeyOf(ObjectPtr(1)) = "a";
        key_reader.KeyOf(ObjectPtr(2)) = "c";
    }

    MockSource input;
    MockDispatcher output;
    MockScopeVisitor& visitor = m_status_table.scope_visitor();
    {
        EXPECT_CALL(m_status_table, GetScopeVisitor("fake-scope", ElementsAre("global-key")));
        EXPECT_CALL(visitor, ListEntries()).WillOnce(Return(visitor.MakeIterator("a", "b")));
        EXPECT_CALL(visitor, Release());
    }

    AddShuffleInput(key_reader, &input, &output);
    boost::scoped_ptr<StatusShuffleCore> core(CreateStatusShuffleCore(m_message));
    MockExecutorBase& base = StartTest(core.get());
    {
        InSequence in_sequence;

        EXPECT_CALL(output,
                    BeginGroup(toft::StringPiece("a"),
                               Matcher<Dataset*>(ResultOf(ToList, ElementsAre("1")))));
        EXPECT_CALL(base, BeginSubGroup("a"));
        EXPECT_CALL(base, EndSubGroup());

        EXPECT_CALL(base, BeginSubGroup("b"));
        EXPECT_CALL(base, EndSubGroup());

        EXPECT_CALL(output,
                    BeginGroup(toft::StringPiece("c"),
                               Matcher<Dataset*>(ResultOf(ToList, ElementsAre("2")))));
        EXPECT_CALL(base, BeginSubGroup("c"));
        EXPECT_CALL(base, EndSubGroup());
    }

    core->BeginGroup(MakeKeys("global-key"));
    input.DispatchObjectAndBinary(ObjectPtr(2), "2");
    input.DispatchObjectAndBinary(ObjectPtr(1), "1");
    input.DispatchDone();
    core->EndGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
