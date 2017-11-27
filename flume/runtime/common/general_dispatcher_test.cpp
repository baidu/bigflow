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

#include "flume/runtime/common/general_dispatcher.h"

#include <cstring>
#include <list>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/testing/string_objector.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/testing/mock_dataset.h"
#include "flume/runtime/testing/string_listener.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Objector;

using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::InSequence;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

static const PbEntity kStringObjector = Entity<Objector>::Of<StringObjector>("").ToProtoMessage();

std::string ONE("1");
std::string TWO("2");
std::string THREE("3");
std::string FOUR("4");

ACTION_P(Fallback, handle) {
    CHECK_EQ(true, handle->Fallback());
}

TEST(GeneralDispatcherTest, EmptyListener) {
    GeneralDispatcher dispatcher("hehe", 0);

    dispatcher.BeginGroup("");
    dispatcher.FinishGroup();
    EXPECT_EQ(false, dispatcher.IsAcceptMore());
    EXPECT_EQ(false, dispatcher.EmitObject(NULL));
    EXPECT_EQ(false, dispatcher.EmitBinary(""));
    dispatcher.Done();
}

TEST(GeneralDispatcherTest, DispatchObjectOnly) {
    GeneralDispatcher dispatcher("hehe", 1);

    StringListener listener_0;
    Source::Handle* handle_0 = listener_0.RequireObject(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle_0);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_0, GotObject("1"));
        EXPECT_CALL(listener_0, GotObject("2"));
        EXPECT_CALL(listener_0, GotObject("3"));
        EXPECT_CALL(listener_0, GotDone());
    }

    StringListener listener_1_0;
    Source::Handle* handle_1_0 = listener_1_0.RequireObject(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1_0);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_1_0, GotObject("1"));
        EXPECT_CALL(listener_1_0, GotObject("2"));
        EXPECT_CALL(listener_1_0, GotDone());

        EXPECT_CALL(listener_1_0, GotObject("3"));
        EXPECT_CALL(listener_1_0, GotDone());
    }

    StringListener listener_1_1;
    Source::Handle* handle_1_1 = listener_1_1.RequireObject(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1_1);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_1_1, GotObject("1"))
            .WillOnce(InvokeWithoutArgs(handle_1_1, &Source::Handle::Done));
        EXPECT_CALL(listener_1_1, GotDone());

        EXPECT_CALL(listener_1_1, GotObject("3"));
        EXPECT_CALL(listener_1_1, GotDone());
    }

    dispatcher.BeginGroup("");

    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&ONE));
    EXPECT_TRUE(dispatcher.EmitObject(&TWO));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&THREE));
    dispatcher.Done();

    dispatcher.FinishGroup();
}

TEST(GeneralDispatcherTest, DispatchBinaryOnly) {
    GeneralDispatcher dispatcher("hehe", 1);

    StringListener listener_0;
    Source::Handle* handle_0 = listener_0.RequireBinary(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle_0);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_0, GotBinary("1"));
        EXPECT_CALL(listener_0, GotBinary("2"));
        EXPECT_CALL(listener_0, GotBinary("3"));
        EXPECT_CALL(listener_0, GotDone());
    }

    StringListener listener_1_0;
    Source::Handle* handle_1_0 = listener_1_0.RequireBinary(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1_0);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_1_0, GotBinary("1"));
        EXPECT_CALL(listener_1_0, GotBinary("2"));
        EXPECT_CALL(listener_1_0, GotDone());

        EXPECT_CALL(listener_1_0, GotBinary("3"));
        EXPECT_CALL(listener_1_0, GotDone());
    }

    StringListener listener_1_1;
    Source::Handle* handle_1_1 = listener_1_1.RequireBinary(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1_1);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_1_1, GotBinary("1"))
            .WillOnce(InvokeWithoutArgs(handle_1_1, &Source::Handle::Done));
        EXPECT_CALL(listener_1_1, GotDone());

        EXPECT_CALL(listener_1_1, GotBinary("3"));
        EXPECT_CALL(listener_1_1, GotDone());
    }

    dispatcher.BeginGroup("");

    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitBinary("1"));
    EXPECT_TRUE(dispatcher.EmitBinary("2"));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitBinary("3"));
    dispatcher.Done();

    dispatcher.FinishGroup();
}

TEST(GeneralDispatcherTest, DispatchObjectAndBinary) {
    GeneralDispatcher dispatcher("hehe", 1);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener_0;
    Source::Handle* handle_0 = listener_0.RequireObjectAndBinary(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle_0);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_0, GotObjectAndBinary(ONE, "1"));
        EXPECT_CALL(listener_0, GotObjectAndBinary(TWO, "2"))
            .WillOnce(InvokeWithoutArgs(handle_0, &Source::Handle::Done));
        EXPECT_CALL(listener_0, GotDone());
    }

    StringListener listener_1_0;
    Source::Handle* handle_1_0 = listener_1_0.RequireObject(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1_0);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_1_0, GotObject(ONE));
        EXPECT_CALL(listener_1_0, GotObject(TWO));
        EXPECT_CALL(listener_1_0, GotDone());

        EXPECT_CALL(listener_1_0, GotObject(THREE))
            .WillOnce(InvokeWithoutArgs(handle_1_0, &Source::Handle::Done));
        EXPECT_CALL(listener_1_0, GotDone());

        EXPECT_CALL(listener_1_0, GotObject(FOUR))
            .WillOnce(InvokeWithoutArgs(handle_1_0, &Source::Handle::Done));
        EXPECT_CALL(listener_1_0, GotDone());
    }

    StringListener listener_1_1;
    Source::Handle* handle_1_1 = listener_1_1.RequireBinary(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1_1);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_1_1, GotBinary("1"));
        EXPECT_CALL(listener_1_1, GotBinary("2"));
        EXPECT_CALL(listener_1_1, GotDone());

        EXPECT_CALL(listener_1_1, GotBinary("3"))
            .WillOnce(InvokeWithoutArgs(handle_1_1, &Source::Handle::Done));
        EXPECT_CALL(listener_1_1, GotDone());

        EXPECT_CALL(listener_1_1, GotBinary("4"))
            .WillOnce(InvokeWithoutArgs(handle_1_1, &Source::Handle::Done));
        EXPECT_CALL(listener_1_1, GotDone());
    }

    dispatcher.BeginGroup("");

    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&ONE));
    EXPECT_TRUE(dispatcher.EmitBinary(TWO));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.IsAcceptMore());
    EXPECT_FALSE(dispatcher.EmitObject(&THREE));
    dispatcher.Done();

    dispatcher.BeginGroup("c");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.IsAcceptMore());
    EXPECT_FALSE(dispatcher.EmitBinary(FOUR));
    dispatcher.Done();

    dispatcher.FinishGroup();
}

TEST(GeneralDispatcherTest, DispatchSingleIterator) {
    MockDatasetManager manager;
    MockDataset root(0), a(1), b(1);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, GetDataset("hehe")).WillOnce(Return(&root));

        EXPECT_CALL(root, Discard());

        a.AddData("1").AddData("2");
        EXPECT_CALL(root, GetChild(toft::StringPiece("a"))).WillOnce(Return(&a));
        EXPECT_CALL(a, Emit(toft::StringPiece("1")));
        EXPECT_CALL(a, Emit(toft::StringPiece("2")));
        EXPECT_CALL(a, Commit());
        EXPECT_CALL(a, NewIterator());
        EXPECT_CALL(a, Release());

        b.AddData("3");
        EXPECT_CALL(root, GetChild(toft::StringPiece("b"))).WillOnce(Return(&b));
        EXPECT_CALL(b, Emit(toft::StringPiece("3")));
        EXPECT_CALL(b, Commit());
        EXPECT_CALL(b, NewIterator());
        EXPECT_CALL(b, Release());

        EXPECT_CALL(root, Release());
    }

    GeneralDispatcher dispatcher("hehe", 1);
    dispatcher.SetDatasetManager(&manager);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireIterator(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotIterator(ElementsAre("1", "2")));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(listener, GotIterator(ElementsAre("3")));
        EXPECT_CALL(listener, GotDone());
    }

    dispatcher.BeginGroup("");

    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&ONE));
    EXPECT_TRUE(dispatcher.EmitBinary(TWO));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&THREE));
    dispatcher.Done();

    dispatcher.FinishGroup();
}

TEST(GeneralDispatcherTest, DispatchMultiLevelIterator) {
    MockDatasetManager manager;
    MockDataset root(0), a(1), b(1);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, GetDataset("hehe")).WillOnce(Return(&root));

        a.AddData("1").AddData("2");
        EXPECT_CALL(root, GetChild(toft::StringPiece("a"))).WillOnce(Return(&a));
        EXPECT_CALL(a, Emit(toft::StringPiece("1")));
        EXPECT_CALL(a, Emit(toft::StringPiece("2")));
        EXPECT_CALL(a, Commit());
        EXPECT_CALL(a, NewIterator());
        EXPECT_CALL(a, Release());

        b.AddData("3");
        EXPECT_CALL(root, GetChild(toft::StringPiece("b"))).WillOnce(Return(&b));
        EXPECT_CALL(b, Emit(toft::StringPiece("3")));
        EXPECT_CALL(b, Commit());
        EXPECT_CALL(b, NewIterator());
        EXPECT_CALL(b, Release());

        root.AddData("1").AddData("2").AddData("3");
        EXPECT_CALL(root, Commit());
        EXPECT_CALL(root, NewIterator());
        EXPECT_CALL(root, Release());
    }

    GeneralDispatcher dispatcher("hehe", 1);
    dispatcher.SetDatasetManager(&manager);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener_0;
    Source::Handle* handle_0 = listener_0.RequireIterator(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle_0);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_0, GotIterator(ElementsAre("1", "2", "3")));
        EXPECT_CALL(listener_0, GotDone());
    }

    StringListener listener_1;
    Source::Handle* handle_1 = listener_1.RequireIterator(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_1, GotIterator(ElementsAre("1", "2")));
        EXPECT_CALL(listener_1, GotDone());

        EXPECT_CALL(listener_1, GotIterator(ElementsAre("3")));
        EXPECT_CALL(listener_1, GotDone());
    }

    dispatcher.BeginGroup("");

    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&ONE));
    EXPECT_TRUE(dispatcher.EmitBinary(TWO));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&THREE));
    dispatcher.Done();

    dispatcher.FinishGroup();
}

TEST(GeneralDispatcherTest, CancelIterator) {
    const int kBigSize = 4 * 1024 * 1024;  // 4MB
    std::string one(kBigSize, '1');
    std::string two(kBigSize, '2');
    std::string three(kBigSize, '3');
    std::string four(kBigSize, '4');

    MockDatasetManager manager;
    MockDataset root(0), a(1), b(1);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, GetDataset("hehe")).WillOnce(Return(&root));
        EXPECT_CALL(root, Discard());

        EXPECT_CALL(root, GetChild(toft::StringPiece("a"))).WillOnce(Return(&a));
        EXPECT_CALL(a, Emit(toft::StringPiece(one)));
        EXPECT_CALL(a, Release());

        b.AddData(four);
        EXPECT_CALL(root, GetChild(toft::StringPiece("b"))).WillOnce(Return(&b));
        EXPECT_CALL(b, Emit(toft::StringPiece(four)));
        EXPECT_CALL(b, Commit());
        EXPECT_CALL(b, NewIterator());
        EXPECT_CALL(b, Release());

        EXPECT_CALL(root, Release());
    }

    GeneralDispatcher dispatcher("hehe", 1);
    dispatcher.SetObjector(kStringObjector);
    dispatcher.SetDatasetManager(&manager);

    StringListener listener_1;
    Source::Handle* handle_1 = listener_1.RequireIterator(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1);
    {
        InSequence in_sequence;

        // first group
        EXPECT_CALL(listener_1, GotDone());

        // second group
        EXPECT_CALL(listener_1, GotIterator(ElementsAre(four)));
        EXPECT_CALL(listener_1, GotDone());
    }

    StringListener listener_0;
    Source::Handle* handle_0 = listener_0.RequireBinary(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle_0);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_0, GotBinary(one));
        EXPECT_CALL(listener_0, GotBinary(two))
            .WillOnce(InvokeWithoutArgs(handle_1, &Source::Handle::Done));
        EXPECT_CALL(listener_0, GotBinary(three));
        EXPECT_CALL(listener_0, GotBinary(four));
        EXPECT_CALL(listener_0, GotDone());
    }

    dispatcher.BeginGroup("");

    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&one));
    EXPECT_TRUE(dispatcher.EmitObject(&two));
    EXPECT_TRUE(dispatcher.EmitObject(&three));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitBinary(four));
    dispatcher.Done();

    dispatcher.FinishGroup();
}

TEST(GeneralDispatcherTest, DispatchDataset) {
    MockDatasetManager manager;
    MockDataset root(0);
    {
        InSequence in_sequence;

        root.AddData("1").AddData("2").SetIsReady();
        EXPECT_CALL(root, NewIterator());
        EXPECT_CALL(root, Release());
    }

    GeneralDispatcher dispatcher("hehe", 0);
    dispatcher.SetDatasetManager(&manager);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireObject(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotObject("1"));
        EXPECT_CALL(listener, GotObject("2"));
        EXPECT_CALL(listener, GotDone());
    }

    dispatcher.BeginGroup("", &root);
    dispatcher.FinishGroup();
    EXPECT_FALSE(dispatcher.IsAcceptMore());
    dispatcher.Done();
}

TEST(GeneralDispatcherTest, DispatchCachedDataset) {
    MockDatasetManager manager;
    MockDataset root(0), a(1), b(1);
    {
        InSequence in_sequence;

        root.AddData("1").AddData("2").AddData("3").SetIsReady();
        EXPECT_CALL(manager, GetDataset("hehe")).WillOnce(Return(&root));

        a.AddData("1").AddData("2").SetIsReady();
        EXPECT_CALL(root, GetChild(toft::StringPiece("a"))).WillOnce(Return(&a));
        EXPECT_CALL(a, NewIterator());
        EXPECT_CALL(a, Release());

        b.AddData("3");
        EXPECT_CALL(root, GetChild(toft::StringPiece("b"))).WillOnce(Return(&b));
        EXPECT_CALL(b, Emit(toft::StringPiece("3")));
        EXPECT_CALL(b, Commit());
        EXPECT_CALL(b, Release());

        EXPECT_CALL(root, NewIterator());
        EXPECT_CALL(root, Release());
    }

    GeneralDispatcher dispatcher("hehe", 1);
    dispatcher.SetDatasetManager(&manager);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener_0;
    Source::Handle* handle_0 = listener_0.RequireIterator(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle_0);
    {
        EXPECT_CALL(listener_0, GotIterator(ElementsAre("1", "2", "3")));
        EXPECT_CALL(listener_0, GotDone());
    }

    StringListener listener_1;
    Source::Handle* handle_1 = listener_1.RequireObject(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle_1);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener_1, GotObject("1"));
        EXPECT_CALL(listener_1, GotObject("2"));
        EXPECT_CALL(listener_1, GotDone());

        EXPECT_CALL(listener_1, GotObject("3"));
        EXPECT_CALL(listener_1, GotDone());
    }

    dispatcher.BeginGroup("");

    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_FALSE(dispatcher.IsAcceptMore());
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.IsAcceptMore());
    EXPECT_TRUE(dispatcher.EmitObject(&THREE));
    dispatcher.Done();

    dispatcher.FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
