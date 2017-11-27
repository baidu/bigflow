/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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

#include "flume/runtime/common/shuffle_runner.h"

#include <algorithm>
#include <cstring>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_partitioner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/mock_dataset.h"
#include "flume/runtime/testing/mock_source.h"
#include "flume/runtime/testing/test_marker.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace internal {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::Return;
using ::testing::Sequence;
using ::testing::UnorderedElementsAre;

using core::Entity;
using core::Objector;
using core::KeyReader;
using core::Partitioner;

class ShuffleRunnerTest : public ::testing::Test {
public:
    typedef toft::Closure<void (const PbLogicalPlanNode&, Source*)> AddShuffleInputMethod;

    template<typename RunnerType>
    MockSource& AddShuffleInput(const PbShuffleNode& shuffle_node, RunnerType* runner) {
        PbLogicalPlanNode node;
        node.set_type(PbLogicalPlanNode::SHUFFLE_NODE);
        *node.mutable_shuffle_node() = shuffle_node;

        MockSource* source = new MockSource();
        m_sources.push_back(source);
        switch (shuffle_node.type()) {
            case PbShuffleNode::BROADCAST:
                EXPECT_CALL(*source, RequireBinary(_, _));
                break;
            default:
                EXPECT_CALL(*source, RequireObjectAndBinary(_, _));
        }

        runner->AddShuffleInput(node, source);
        return *source;
    }

    template<typename RunnerType>
    MockSource& AddShuffleInput(PbShuffleNode::Type type, RunnerType* runner) {
        PbShuffleNode shuffle_node;
        shuffle_node.set_type(type);
        return AddShuffleInput(shuffle_node, runner);
    }

    template<typename RunnerType>
    MockSource& AddShuffleInput(const MockKeyReader& key_reader, RunnerType* runner) {
        PbShuffleNode shuffle_node;
        shuffle_node.set_type(PbShuffleNode::KEY);
        *shuffle_node.mutable_key_reader() =
                Entity<KeyReader>::Of<MockKeyReader>(key_reader.config()).ToProtoMessage();
        return AddShuffleInput(shuffle_node, runner);
    }

    template<typename RunnerType>
    MockSource& AddShuffleInput(const MockPartitioner& partitioner, RunnerType* runner) {
        PbShuffleNode shuffle_node;
        shuffle_node.set_type(PbShuffleNode::SEQUENCE);
        *shuffle_node.mutable_partitioner() =
                Entity<Partitioner>::Of<MockPartitioner>(partitioner.config()).ToProtoMessage();
        return AddShuffleInput(shuffle_node, runner);
    }

    std::vector<Dataset*> GenerateDatasets() {
        std::vector<Dataset*> results;
        for (size_t i = 0; i < m_sources.size(); ++i) {
            results.push_back(m_dataset_manager.GetDataset(boost::lexical_cast<std::string>(i)));
        }
        return results;
    }

    std::vector<Dataset*> CommitDatasets(const std::vector<Dataset*>& datasets) {
        for (size_t i = 0; i < datasets.size(); ++i) {
            datasets[i]->Commit();
        }
        return datasets;
    }

    std::vector< std::list<std::string> > ExtractDatasets(const std::vector<Dataset*>& datasets) {
        std::vector< std::list<std::string> > results;
        for (size_t i = 0; i < datasets.size(); ++i) {
            Dataset* dataset = datasets[i];
            CHECK(dataset->IsReady());
            results.push_back(ToList(dataset));
            dataset->Release();
        }
        return results;
    }

    template<typename Key, bool kIsSorted>
    std::map< Key, std::vector< std::list<std::string> > >
    ExtractAll(CoGroupRunner<Key, kIsSorted>* runner) {
        std::map< Key, std::vector< std::list<std::string> > > result;
        typename CoGroupRunner<Key, kIsSorted>::Container::iterator ptr = runner->begin();
        while (ptr != runner->end()) {
            result[runner->key(ptr)] = ExtractDatasets(runner->datasets(ptr));
            ++ptr;
        }
        return result;
    }

private:
    boost::ptr_vector<MockSource> m_sources;
    MemoryDatasetManager m_dataset_manager;
};

TEST_F(ShuffleRunnerTest, Broadcast) {
    BroadcastRunner runner;
    runner.Initialize(PbScope());
    MockSource& source1 = AddShuffleInput(PbShuffleNode::BROADCAST, &runner);
    MockSource& source2 = AddShuffleInput(PbShuffleNode::BROADCAST, &runner);

    {
        std::vector<Dataset*> outputs = GenerateDatasets();
        runner.StartShuffle(outputs);

        source1.DispatchBinary("apple");
        source1.DispatchDone();
        EXPECT_EQ(false, runner.IsInputDone());

        source2.DispatchBinary("orange");
        source2.DispatchBinary("peach");
        source2.DispatchDone();
        EXPECT_EQ(true, runner.IsInputDone());

        EXPECT_THAT(ExtractDatasets(runner.datasets("0")),
                    ElementsAre(ElementsAre("apple"), ElementsAre("orange", "peach")));
        EXPECT_THAT(ExtractDatasets(runner.datasets("1")),
                    ElementsAre(ElementsAre("apple"), ElementsAre("orange", "peach")));

        runner.FinishShuffle();
        EXPECT_THAT(ExtractDatasets(CommitDatasets(outputs)),
                    ElementsAre(ElementsAre("apple", "apple"),
                                ElementsAre("orange", "peach", "orange", "peach")));
    }

    {
        std::vector<Dataset*> outputs = GenerateDatasets();
        runner.StartShuffle(outputs);

        source1.DispatchDone();
        source2.DispatchDone();
        EXPECT_EQ(true, runner.IsInputDone());

        EXPECT_THAT(ExtractDatasets(runner.datasets("0")),
                    ElementsAre(IsEmpty(), IsEmpty()));

        runner.FinishShuffle();
        EXPECT_THAT(ExtractDatasets(CommitDatasets(outputs)),
                    ElementsAre(IsEmpty(), IsEmpty()));
    }
}

TEST_F(ShuffleRunnerTest, CoGroup) {
    typedef CoGroupRunner<std::string, false> RunnerType;

    RunnerType runner;
    RunnerType::Container::iterator ptr;

    PbScope scope;
    scope.set_is_sorted(false);
    runner.Initialize(scope);

    MockKeyReader& key_reader = MockKeyReader::Mock("group_key_reader");
    {
        key_reader.KeyOf(ObjectPtr(1)) = "a";
        key_reader.KeyOf(ObjectPtr(2)) = "a";
        key_reader.KeyOf(ObjectPtr(3)) = "b";
        key_reader.KeyOf(ObjectPtr(4)) = "c";
    }

    MockSource& source1 = AddShuffleInput(key_reader, &runner);
    MockSource& source2 = AddShuffleInput(key_reader, &runner);

    {
        std::vector<Dataset*> outputs = GenerateDatasets();
        runner.StartShuffle(outputs);

        source1.DispatchObjectAndBinary(ObjectPtr(1), "1");
        source2.DispatchObjectAndBinary(ObjectPtr(2), "2");
        source2.DispatchDone();
        EXPECT_EQ(false, runner.IsInputDone());

        source1.DispatchObjectAndBinary(ObjectPtr(3), "3");
        source1.DispatchDone();
        EXPECT_EQ(true, runner.IsInputDone());

        EXPECT_EQ(2u, runner.size());
        EXPECT_THAT(ExtractAll(&runner),
                    ElementsAre(Pair("a", ElementsAre(ElementsAre("1"), ElementsAre("2"))),
                                Pair("b", ElementsAre(ElementsAre("3"), IsEmpty()))));

        runner.FinishShuffle();
        EXPECT_THAT(ExtractDatasets(CommitDatasets(outputs)),
                    ElementsAre(UnorderedElementsAre("1", "3"), UnorderedElementsAre("2")));
    }

    {
        std::vector<Dataset*> outputs = GenerateDatasets();
        runner.StartShuffle(outputs);

        source2.DispatchObjectAndBinary(ObjectPtr(4), "4");
        source2.DispatchDone();
        source1.DispatchDone();
        EXPECT_EQ(true, runner.IsInputDone());

        EXPECT_EQ(1u, runner.size());
        EXPECT_THAT(ExtractAll(&runner),
                    ElementsAre(Pair("c", ElementsAre(IsEmpty(), ElementsAre("4")))));

        runner.FinishShuffle();
        EXPECT_THAT(ExtractDatasets(CommitDatasets(outputs)),
                    ElementsAre(IsEmpty(), UnorderedElementsAre("4")));
    }
}

TEST_F(ShuffleRunnerTest, SortByKey) {
    typedef CoGroupRunner<std::string, true> RunnerType;

    RunnerType runner;
    RunnerType::Container::iterator ptr;

    PbScope scope;
    scope.set_is_sorted(true);
    runner.Initialize(scope);

    std::string keys[] = {
        std::string(""), std::string("a"), std::string(16 * 1024 * 1024, 'a'), std::string("b")
    };
    MockKeyReader& key_reader = MockKeyReader::Mock("sort_key_reader");
    for (size_t i = 0; i < 4; ++i) {
        key_reader.KeyOf(ObjectPtr(i + 1)) = keys[i];
    }

    MockSource& source = AddShuffleInput(key_reader, &runner);

    {
        std::vector<Dataset*> outputs = GenerateDatasets();
        runner.StartShuffle(outputs);

        EXPECT_EQ(false, runner.IsInputDone());
        source.DispatchObjectAndBinary(ObjectPtr(4), "4");
        source.DispatchObjectAndBinary(ObjectPtr(2), "2");
        source.DispatchObjectAndBinary(ObjectPtr(3), "3");
        source.DispatchObjectAndBinary(ObjectPtr(1), "1");
        source.DispatchDone();
        EXPECT_EQ(true, runner.IsInputDone());

        EXPECT_EQ(4u, runner.size());
        EXPECT_THAT(ExtractAll(&runner),
                    ElementsAre(Pair(keys[0], ElementsAre(ElementsAre("1"))),
                                Pair(keys[1], ElementsAre(ElementsAre("2"))),
                                Pair(keys[2], ElementsAre(ElementsAre("3"))),
                                Pair(keys[3], ElementsAre(ElementsAre("4")))));

        runner.FinishShuffle();
        EXPECT_THAT(ExtractDatasets(CommitDatasets(outputs)),
                    ElementsAre(ElementsAre("1", "2", "3", "4")));
    }
}

TEST_F(ShuffleRunnerTest, DefaultPartitioner) {
    typedef CoGroupRunner<uint32_t> RunnerType;
    const uint32_t kBucketNum = 5;

    CoGroupRunner<uint32_t> runner;

    PbScope scope;
    scope.mutable_bucket_scope()->set_bucket_size(kBucketNum);
    runner.Initialize(scope);

    MockSource& source = AddShuffleInput(PbShuffleNode::SEQUENCE, &runner);

    {
        std::vector<Dataset*> outputs = GenerateDatasets();
        runner.StartShuffle(outputs);

        const uint32_t kTotalRecords = 7;
        for (uint32_t i = 0; i < kTotalRecords; ++i) {
            source.DispatchObjectAndBinary(ObjectPtr(i), boost::lexical_cast<std::string>(i));
        }
        source.DispatchDone();

        EXPECT_EQ(kBucketNum, runner.size());
        EXPECT_THAT(ExtractAll(&runner),
                    ElementsAre(Pair(0, ElementsAre(ElementsAre("0", "5"))),
                                Pair(1, ElementsAre(ElementsAre("1", "6"))),
                                Pair(2, ElementsAre(ElementsAre("2"))),
                                Pair(3, ElementsAre(ElementsAre("3"))),
                                Pair(4, ElementsAre(ElementsAre("4")))));

        runner.FinishShuffle();
        EXPECT_THAT(ExtractDatasets(CommitDatasets(outputs)),
                    ElementsAre(ElementsAre("0", "5", "1", "6", "2", "3", "4")));
    }
}

TEST_F(ShuffleRunnerTest, PartitionToOneBucket) {
    typedef CoGroupRunner<uint32_t> RunnerType;
    CoGroupRunner<uint32_t> runner;

    PbScope scope;
    scope.mutable_bucket_scope()->set_bucket_size(2);
    runner.Initialize(scope);

    MockPartitioner& partitioner = MockPartitioner::Mock("partitioner");
    EXPECT_CALL(partitioner, Partition(ObjectPtr(0), 2)).WillOnce(Return(0));
    EXPECT_CALL(partitioner, Partition(ObjectPtr(1), 2)).WillOnce(Return(0));

    MockSource& source = AddShuffleInput(partitioner, &runner);

    {
        std::vector<Dataset*> outputs = GenerateDatasets();
        runner.StartShuffle(outputs);

        source.DispatchObjectAndBinary(ObjectPtr(0), "0");
        source.DispatchObjectAndBinary(ObjectPtr(1), "1");
        source.DispatchDone();

        EXPECT_EQ(2, runner.size());
        EXPECT_THAT(ExtractAll(&runner),
                    ElementsAre(Pair(0, ElementsAre(ElementsAre("0", "1"))),
                                Pair(1, ElementsAre(IsEmpty()))));

        runner.FinishShuffle();
        EXPECT_THAT(ExtractDatasets(CommitDatasets(outputs)),
                    ElementsAre(ElementsAre("0", "1")));
    }
}

}  // namespace internal
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
