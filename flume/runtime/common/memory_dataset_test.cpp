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

#include "flume/runtime/common/memory_dataset.h"

#include <list>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "toft/base/string/string_piece.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::ElementsAre;

std::list<std::string> ToList(Dataset::Iterator* iterator) {
    std::list<std::string> results;
    while (iterator->HasNext()) {
        results.push_back(iterator->NextValue().as_string());
    }
    return results;
}

TEST(DatasetTest, EmptyManager) {
    MemoryDatasetManager manager;
}

TEST(DatasetTest, EmptyDataset) {
    MemoryDatasetManager manager;
    manager.GetDataset("hehe")->Release();
    manager.GetDataset("haha")->Release();
}

TEST(DatasetTest, OneScopeCommit) {
    MemoryDatasetManager manager;

    Dataset* dataset = manager.GetDataset("hehe");
    EXPECT_EQ(false, dataset->IsReady());

    util::Arena* arena = dataset->AcquireArena();
    char* buffer = arena->AllocateBytes(5);
    memcpy(buffer, "hello", 5);

    dataset->Emit(toft::StringPiece(buffer, 5));
    dataset->Emit("world");

    dataset->Commit();
    EXPECT_EQ(true, dataset->IsReady());

    Dataset::Iterator* iterator = dataset->NewIterator();
    EXPECT_THAT(ToList(iterator), ElementsAre("hello", "world"));

    iterator->Reset();
    EXPECT_THAT(ToList(iterator), ElementsAre("hello", "world"));
    iterator->Done();

    EXPECT_THAT(ToList(dataset->NewIterator()), ElementsAre("hello", "world"));

    dataset->Release();
}

TEST(DatasetTest, OneScopeDiscard) {
    MemoryDatasetManager manager;

    Dataset* dataset = manager.GetDataset("hehe");
    EXPECT_EQ(false, dataset->IsReady());

    dataset->Emit("hello");
    dataset->Emit("world");

    Dataset::Iterator* iterator = dataset->Discard();
    EXPECT_EQ(false, dataset->IsReady());
    CHECK_NOTNULL(iterator);
    EXPECT_THAT(ToList(iterator), ElementsAre("hello", "world"));
    iterator->Done();

    dataset->Release();
}

TEST(DatasetTest, NestedScopeCommit) {
    MemoryDatasetManager manager;

    Dataset* root = manager.GetDataset("hehe");
    EXPECT_EQ(false, root->IsReady());

    Dataset* child_0 = root->GetChild("0");
    EXPECT_EQ(false, child_0->IsReady());
    {
        Dataset* child_0_0 = child_0->GetChild("0");
        EXPECT_EQ(false, child_0_0->IsReady());
        child_0_0->Emit("apple");

        Dataset* child_0_1 = child_0->GetChild("1");
        EXPECT_EQ(false, child_0_1->IsReady());
        child_0_1->Emit("baidu");

        Dataset* child_0_2 = child_0->GetChild("1");
        EXPECT_EQ(false, child_0_2->IsReady());
        child_0_2->Emit("cisco");

        child_0_1->Commit();
        EXPECT_EQ(true, child_0_1->IsReady());
        EXPECT_THAT(ToList(child_0_1->NewIterator()), ElementsAre("baidu"));
        child_0_1->Release();

        child_0->Commit();
        EXPECT_EQ(true, child_0->IsReady());
        EXPECT_THAT(ToList(child_0->NewIterator()), ElementsAre("baidu"));
        child_0->Release();

        child_0_0->Commit(); // after committing father
        EXPECT_EQ(true, child_0_0->IsReady());
        EXPECT_THAT(ToList(child_0_0->NewIterator()), ElementsAre("apple"));
        child_0_0->Release();

        // child_0_2->Commit();
        EXPECT_EQ(false, child_0_2->IsReady());
        child_0_2->Release();
    }

    Dataset* child_1 = root->GetChild("1");
    EXPECT_EQ(false, child_1->IsReady());
    {
        Dataset* child_1_0 = child_1->GetChild("0");
        EXPECT_EQ(false, child_1_0->IsReady());
        child_1_0->Emit("emc");
        EXPECT_THAT(ToList(child_1_0->Discard()), ElementsAre("emc"));
        child_1_0->Release();

        Dataset* child_1_1 = child_1->GetChild("1");
        EXPECT_EQ(false, child_1_1->IsReady());
        child_1_1->Emit("facebook");

        Dataset* child_1_2 = child_1->GetChild("2");
        EXPECT_EQ(false, child_1_2->IsReady());
        child_1_2->Emit("google");

        child_1_2->Commit();
        EXPECT_EQ(true, child_1_2->IsReady());
        EXPECT_THAT(ToList(child_1_2->NewIterator()), ElementsAre("google"));
        child_1_2->Release();

        child_1_1->Commit();
        EXPECT_EQ(true, child_1_1->IsReady());
        EXPECT_THAT(ToList(child_1_1->NewIterator()), ElementsAre("facebook"));
        child_1_1->Release();

        child_1->Commit();
        EXPECT_EQ(true, child_1->IsReady());
        EXPECT_THAT(ToList(child_1->NewIterator()), ElementsAre("google", "facebook"));
        child_1->Release();
    }

    EXPECT_EQ(false, root->IsReady());
    root->Commit();
    EXPECT_EQ(true, root->IsReady());
    EXPECT_THAT(ToList(root->NewIterator()), ElementsAre("baidu", "google", "facebook"));

    Dataset* child_1_a = root->GetChild("1");
    EXPECT_EQ(true, child_1_a->IsReady());
    EXPECT_THAT(ToList(child_1_a->NewIterator()), ElementsAre("google", "facebook"));

    Dataset* child_1_b = root->GetChild("1");
    EXPECT_EQ(true, child_1_b->IsReady());
    EXPECT_THAT(ToList(child_1_b->NewIterator()), ElementsAre("google", "facebook"));
    child_1_b->Release();

    root->Release();

    Dataset* child_1_2 = child_1_a->GetChild("2");
    child_1_a->Release();
    EXPECT_EQ(true, child_1_2->IsReady());
    EXPECT_THAT(ToList(child_1_2->NewIterator()), ElementsAre("google"));
    child_1_2->Release();
}

TEST(DatasetTest, NestedScopeDiscard) {
    MemoryDatasetManager manager;
    Dataset* root = manager.GetDataset("hehe");

    Dataset* child_0 = root->GetChild("0");
    child_0->Emit("first");
    child_0->Commit();
    child_0->Release();

    Dataset* child_1 = root->GetChild("1");
    child_1->Emit("second");

    EXPECT_THAT(ToList(root->Discard()), ElementsAre("first"));
    EXPECT_EQ(false, root->IsReady());

    child_1->Commit();
    EXPECT_THAT(ToList(child_1->NewIterator()), ElementsAre("second"));
    child_1->Release();

    Dataset* child_0_ = root->GetChild("0");
    EXPECT_EQ(false, child_0_->IsReady());
    child_0_->Release();

    Dataset* child_1_ = root->GetChild("1");
    EXPECT_EQ(false, child_1_->IsReady());
    child_1_->Emit("second_");
    child_1_->Commit();
    EXPECT_THAT(ToList(child_1_->NewIterator()), ElementsAre("second_"));
    child_1_->Release();

    root->Release();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
