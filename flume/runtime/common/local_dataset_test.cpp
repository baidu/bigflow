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
// Author: Wen Xiang <wenxiang@baidu.com>

#include <list>
#include <string>
#include <vector>

#include "flume/runtime/common/local_dataset.h"

#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "leveldb/db.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::ElementsAre;

const std::vector<uint32_t> kEmptySequences;
static const int KB = 1024;
static const int MB = 1024 * KB;

leveldb::DB* NewLevelDb() {
    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::DB* db = NULL;
    CHECK(leveldb::DB::Open(options, "testdb", &db).ok());
    return db;
}

void EmitTo(const std::string& value, Dataset* dataset) {
    util::Arena* arena = dataset->AcquireArena();

    char* buffer = arena->AllocateBytes(value.size());
    std::memcpy(buffer, value.data(), value.size());
    dataset->Emit(toft::StringPiece(buffer, value.size()));

    dataset->ReleaseArena();
}

std::list<std::string> ToList(Dataset::Iterator* iterator) {
    std::list<std::string> result;

    iterator->Reset();
    while (iterator->HasNext()) {
        result.push_back(iterator->NextValue().as_string());
    }

    return result;
}

TEST(LocalDatasetTest, Construct) {
    LocalDatasetManager manager;
    manager.Initialize("Construct", 64 * KB);

    Dataset* dataset = manager.GetDataset("");
    dataset->Discard();
    dataset->Release();
}

TEST(LocalDatasetTest, EmitToMemoryShard) {
    LocalDatasetManager manager;
    manager.Initialize("EmitToMemoryShard", 64 * KB);

    Dataset* dataset = manager.GetDataset("test");
    EXPECT_EQ(false, dataset->IsReady());
    EmitTo("apple", dataset);
    EmitTo("boy", dataset);

    dataset->Commit();
    EXPECT_EQ(true, dataset->IsReady());
    EXPECT_THAT(ToList(dataset->NewIterator()), ElementsAre("apple", "boy"));

    dataset->Release();
}

TEST(LocalDatasetTest, DiscardMemoryShard) {
    LocalDatasetManager manager;
    manager.Initialize("DiscardMemoryShard", 64 * KB);

    Dataset* dataset = manager.GetDataset("test");
    EXPECT_EQ(false, dataset->IsReady());
    EmitTo("apple", dataset);
    EmitTo("boy", dataset);

    EXPECT_THAT(ToList(dataset->Discard()), ElementsAre("apple", "boy"));
    EXPECT_EQ(false, dataset->IsReady());

    dataset->Release();
}

TEST(LocalDatasetTest, EmitToDisk) {
    LocalDatasetManager manager;
    manager.Initialize("EmitToDisk", 64 * KB);

    LocalDatasetManager::DatasetImpl* dataset = manager.GetDataset("test");
    dataset->Dump();
    EmitTo("candy", dataset);
    EmitTo("dog", dataset);

    EXPECT_EQ(false, dataset->IsReady());
    dataset->Commit();
    EXPECT_THAT(ToList(dataset->NewIterator()), ElementsAre("candy", "dog"));
    EXPECT_EQ(true, dataset->IsReady());

    dataset->Release();
}

TEST(LocalDatasetTest, DiscardDiskChunk) {
    LocalDatasetManager manager;
    manager.Initialize("DiscardDiskChunk", 64 * KB);

    LocalDatasetManager::DatasetImpl* dataset = manager.GetDataset("test");
    dataset->Dump();
    EmitTo("candy", dataset);
    EmitTo("dog", dataset);

    EXPECT_EQ(false, dataset->IsReady());
    EXPECT_THAT(ToList(dataset->Discard()), ElementsAre("candy", "dog"));
    EXPECT_EQ(false, dataset->IsReady());

    dataset->Release();
}

TEST(LocalDatasetTest, Dump) {
    LocalDatasetManager manager;
    manager.Initialize("Dump", 64 * KB);

    LocalDatasetManager::DatasetImpl* dataset = manager.GetDataset("test");
    EmitTo("earth", dataset);
    dataset->Dump();
    EmitTo("flower", dataset);

    dataset->Commit();
    EXPECT_THAT(ToList(dataset->NewIterator()), ElementsAre("earth", "flower"));

    dataset->Release();
}

TEST(LocalDatasetTest, AddChildToMemoryChunk) {
    LocalDatasetManager manager;
    manager.Initialize("AddChildToMemoryShard", 64 * KB);
    Dataset* root = manager.GetDataset("root");

    Dataset* child_0 = root->GetChild("child_0");
    EmitTo("one", child_0);
    child_0->Commit();
    child_0->Release();

    Dataset* child_1 = root->GetChild("child_1");
    CHECK_EQ(child_0, child_1);  // reuse
    EmitTo("two", child_1);
    child_1->Discard();

    Dataset* child_2 = root->GetChild("child_2");
    CHECK_NE(child_2, child_1);  // can not reuse
    EmitTo("three", child_2);
    child_2->Commit();
    child_2->Release();

    root->Commit();
    EXPECT_THAT(ToList(root->NewIterator()), ElementsAre("one", "three"));
    root->Release();

    // release last
    child_1->Release();
}

TEST(LocalDatasetTest, AddChildToDiskChunk) {
    typedef LocalDatasetManager::DatasetImpl DatasetImpl;

    LocalDatasetManager manager;
    manager.Initialize("AddChildToDiskShard", 64 * KB);
    DatasetImpl* root = manager.GetDataset("root");
    root->Dump();

    DatasetImpl* father_0 = root->GetChild("0");
    DatasetImpl* father_1 = root->GetChild("1");

    DatasetImpl* childs[] = {
        father_0->GetChild("0"), father_0->GetChild("1"), father_0->GetChild("2"),
        father_1->GetChild("0"), father_1->GetChild("1"), father_1->GetChild("2"),
    };

    father_0->Dump();
    for (size_t i = 0; i < 6; ++i) {
        if (i == 1) {
            childs[i]->Dump();
        }
        EmitTo(boost::lexical_cast<std::string>(i), childs[i]);
    }

    childs[2]->Commit();
    childs[1]->Commit();
    childs[0]->Commit();
    father_0->Commit();

    childs[3]->Commit();
    childs[4]->Commit();
    childs[5]->Commit();
    father_1->Commit();

    root->Commit();

    EXPECT_THAT(ToList(father_1->NewIterator()), ElementsAre("3", "4", "5"));
    EXPECT_THAT(ToList(father_0->NewIterator()), ElementsAre("2", "1", "0"));
    EXPECT_THAT(ToList(root->NewIterator()), ElementsAre("2", "1", "0", "3", "4", "5"));
}

TEST(LocalDatasetTest, AddToDiscardedChunk) {
    LocalDatasetManager manager;
    manager.Initialize("NestedScopeDiscard", 64 * MB);

    Dataset* root = manager.GetDataset("hehe");

    Dataset* child_0 = root->GetChild("0");
    EmitTo("first", child_0);
    child_0->Commit();
    child_0->Release();

    Dataset* child_1 = root->GetChild("1");
    EmitTo("second", child_1);

    EXPECT_THAT(ToList(root->Discard()), ElementsAre("first"));

    child_1->Commit();
    EXPECT_THAT(ToList(child_1->NewIterator()), ElementsAre("second"));
    child_1->Release();

    Dataset* child_0_ = root->GetChild("0");
    child_0_->Release();

    Dataset* child_1_ = root->GetChild("1");
    EmitTo("second_", child_1);
    child_1_->Commit();
    EXPECT_THAT(ToList(child_1_->NewIterator()), ElementsAre("second_"));
    child_1_->Release();

    root->Release();
}

TEST(LocalDatasetTest, MemoryIterator) {
    LocalDatasetManager manager;
    manager.Initialize("MemoryIterator", 64 * MB);
    Dataset* dataset = manager.GetDataset("hehe");

    util::Arena* arena = dataset->AcquireArena();
    char* buffer = arena->AllocateBytes(5);
    memcpy(buffer, "hello", 5);

    dataset->Emit(toft::StringPiece(buffer, 5));
    dataset->Emit("world");

    dataset->Commit();

    Dataset::Iterator* iterator = dataset->NewIterator();
    EXPECT_THAT(ToList(iterator), ElementsAre("hello", "world"));

    iterator->Reset();
    EXPECT_THAT(ToList(iterator), ElementsAre("hello", "world"));
    iterator->Done();

    EXPECT_THAT(ToList(dataset->NewIterator()), ElementsAre("hello", "world"));

    dataset->Release();
}

TEST(LocalDatasetTest, DiskIterator) {
    LocalDatasetManager manager;
    manager.Initialize("DiskIterator", 64 * MB);
    LocalDatasetManager::DatasetImpl* dataset = manager.GetDataset("hehe");
    dataset->Dump();

    util::Arena* arena = dataset->AcquireArena();
    char* buffer = arena->AllocateBytes(5);
    memcpy(buffer, "hello", 5);

    dataset->Emit(toft::StringPiece(buffer, 5));
    dataset->Emit("world");

    dataset->Commit();

    Dataset::Iterator* iterator = dataset->NewIterator();
    EXPECT_THAT(ToList(iterator), ElementsAre("hello", "world"));

    iterator->Reset();
    EXPECT_THAT(ToList(iterator), ElementsAre("hello", "world"));
    iterator->Done();

    EXPECT_THAT(ToList(dataset->NewIterator()), ElementsAre("hello", "world"));

    dataset->Release();
}

TEST(LocalDatasetTest, MassiveDatasets) {
    LocalDatasetManager manager;
    manager.Initialize("MassiveDatasets", 64 * KB);
    Dataset* root = manager.GetDataset("hehe");
    root->Discard();

    for (int dataset_size = 1000; dataset_size <= 10000; dataset_size += 1000) {
        std::list<std::string> expected_results;
        std::vector<Dataset*> datasets;
        datasets.reserve(dataset_size);
        LOG(INFO) << "create " << dataset_size << " datasets ...";

        Dataset* father = root->GetChild("");
        for (int i = 0; i < dataset_size; ++i) {
            std::string value = boost::lexical_cast<std::string>(i);
            expected_results.push_back(value);

            Dataset* child = father->GetChild("");
            datasets.push_back(child);

            EmitTo(value, child);
        }

        for (size_t i = 0; i < datasets.size(); ++i) {
            datasets[i]->Commit();
            datasets[i]->Release();
        }

        father->Commit();
        EXPECT_EQ(ToList(father->NewIterator()), expected_results);
        father->Release();
    }

    root->Release();
}

TEST(LocalDatasetTest, BigDataset) {
    LocalDatasetManager manager;
    manager.Initialize("BigDataset", 64 * KB);

    Dataset* root = manager.GetDataset("hehe");

    std::list<std::string> expected_results;
    for (size_t i = 0; i < 80; ++i) {
        std::string value(1 * KB, 'a');
        expected_results.push_back(value);
        EmitTo(value, root);
    }

    root->Commit();
    EXPECT_EQ(ToList(root->NewIterator()), expected_results);
    root->Release();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
