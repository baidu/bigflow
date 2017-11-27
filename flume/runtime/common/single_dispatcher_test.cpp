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

#include "flume/runtime/common/single_dispatcher.h"

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

std::string ONE("1");
std::string TWO("2");
std::string THREE("3");
std::string FOUR("4");

static const PbEntity kStringObjector = Entity<Objector>::Of<StringObjector>("").ToProtoMessage();

ACTION_P(Done, handle) {
    handle->Done();
}

ACTION_P(Fallback, handle) {
    CHECK_EQ(true, handle->Fallback());
}

TEST(SingleDispatcherTest, Intialize) {
    SingleDispatcher dispatcher("hehe", 0);
}

TEST(SingleDispatcherTest, DispatchObjectAtSameLevel) {
    SingleDispatcher dispatcher("hehe", 0);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireObject(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotObject("1"));
        EXPECT_CALL(listener, GotObject("2"));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(listener, GotObject("3"));
        EXPECT_CALL(listener, GotDone());
    }


    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&ONE));
    EXPECT_TRUE(dispatcher.EmitBinary(TWO));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&THREE));
    dispatcher.Done();
}

TEST(SingleDispatcherTest, DispatchBinaryAtSameLevel) {
    SingleDispatcher dispatcher("hehe", 0);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireBinary(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotBinary("1"));
        EXPECT_CALL(listener, GotBinary("2"));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(listener, GotBinary("3"));
        EXPECT_CALL(listener, GotDone());
    }


    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitBinary(ONE));
    EXPECT_TRUE(dispatcher.EmitObject(&TWO));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitBinary(THREE));
    dispatcher.Done();
}

TEST(SingleDispatcherTest, DispatchStreamAtHigherLevel) {
    SingleDispatcher dispatcher("hehe", 2);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireObjectAndBinary(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotObjectAndBinary(ONE, "1"));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(listener, GotObjectAndBinary(THREE, "3"));
        EXPECT_CALL(listener, GotDone());
    }

    dispatcher.BeginGroup("");
    {
        dispatcher.BeginGroup("0");
        {
            dispatcher.BeginGroup("a");
            dispatcher.FinishGroup();
            EXPECT_TRUE(dispatcher.EmitObject(&ONE));
            dispatcher.Done();

            handle->Done();

            dispatcher.BeginGroup("b");
            dispatcher.FinishGroup();
            EXPECT_FALSE(dispatcher.EmitObject(&TWO));
            dispatcher.Done();
        }
        dispatcher.FinishGroup();
    }
    {
        dispatcher.BeginGroup("0");
        {
            dispatcher.BeginGroup("a");
            dispatcher.FinishGroup();
            EXPECT_TRUE(dispatcher.EmitBinary(THREE));
            dispatcher.Done();
        }
        dispatcher.FinishGroup();
    }
    dispatcher.FinishGroup();
}

TEST(SingleDispatcherTest, DispatchIteratorAtSameLevel) {
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

    SingleDispatcher dispatcher("hehe", 1);
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
    {
        dispatcher.BeginGroup("a");
        dispatcher.FinishGroup();
        EXPECT_TRUE(dispatcher.EmitObject(&ONE));
        EXPECT_TRUE(dispatcher.EmitBinary(TWO));
        dispatcher.Done();
    }
    {
        dispatcher.BeginGroup("b");
        dispatcher.FinishGroup();
        EXPECT_TRUE(dispatcher.EmitObject(&THREE));
        dispatcher.Done();
    }
    dispatcher.FinishGroup();
}

TEST(SingleDispatcherTest, DispatchIteratorAtHigherLevel) {
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
        EXPECT_CALL(a, Release());

        b.AddData("3");
        EXPECT_CALL(root, GetChild(toft::StringPiece("b"))).WillOnce(Return(&b));
        EXPECT_CALL(b, Emit(toft::StringPiece("3")));
        EXPECT_CALL(b, Commit());
        EXPECT_CALL(b, Release());

        root.AddData("1").AddData("2").AddData("3");
        EXPECT_CALL(root, Commit());
        EXPECT_CALL(root, NewIterator());
        EXPECT_CALL(root, Release());
    }

    SingleDispatcher dispatcher("hehe", 1);
    dispatcher.SetDatasetManager(&manager);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireIterator(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotIterator(ElementsAre("1", "2", "3")));
        EXPECT_CALL(listener, GotDone());
    }

    dispatcher.BeginGroup("");
    {
        dispatcher.BeginGroup("a");
        dispatcher.FinishGroup();
        EXPECT_TRUE(dispatcher.EmitObject(&ONE));
        EXPECT_TRUE(dispatcher.EmitBinary(TWO));
        dispatcher.Done();
    }
    {
        dispatcher.BeginGroup("b");
        dispatcher.FinishGroup();
        EXPECT_TRUE(dispatcher.EmitObject(&THREE));
        dispatcher.Done();
    }
    dispatcher.FinishGroup();
}

TEST(SingleDispatcherTest, CancelIterator) {
    const int kBigSize = 4 * 1024 * 1024;  // 4MB
    std::string one(kBigSize, '1');
    std::string two(kBigSize, '2');
    std::string three(kBigSize, '3');
    StringObjector objector;

    MockDatasetManager manager;
    MockDataset root(0), a(1), b(1);
    {
        InSequence in_sequence;

        EXPECT_CALL(manager, GetDataset("hehe")).WillOnce(Return(&root));
        EXPECT_CALL(root, Discard());

        EXPECT_CALL(root, GetChild(toft::StringPiece("a"))).WillOnce(Return(&a));
        EXPECT_CALL(a, Emit(toft::StringPiece(one)));
        EXPECT_CALL(a, Emit(toft::StringPiece(two)));
        EXPECT_CALL(a, Discard());
        EXPECT_CALL(a, Release());

        b.AddData(three);
        EXPECT_CALL(root, GetChild(toft::StringPiece("b"))).WillOnce(Return(&b));
        EXPECT_CALL(b, Emit(toft::StringPiece(three)));
        EXPECT_CALL(b, Commit());
        EXPECT_CALL(b, NewIterator());
        EXPECT_CALL(b, Release());

        EXPECT_CALL(root, Release());
    }

    SingleDispatcher dispatcher("hehe", 1);
    dispatcher.SetDatasetManager(&manager);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireIterator(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        // first group
        EXPECT_CALL(listener, GotDone());

        // second group
        EXPECT_CALL(listener, GotIterator(ElementsAre(three)));
        EXPECT_CALL(listener, GotDone());
    }

    dispatcher.BeginGroup("");
    {
        dispatcher.BeginGroup("a");
        dispatcher.FinishGroup();
        EXPECT_TRUE(dispatcher.EmitObject(&one));
        EXPECT_TRUE(dispatcher.EmitObject(&two));

        handle->Done();
        EXPECT_EQ(false, dispatcher.IsAcceptMore());

        dispatcher.Done();
    }
    {
        dispatcher.BeginGroup("b");
        dispatcher.FinishGroup();
        EXPECT_TRUE(dispatcher.EmitBinary(three));
        dispatcher.Done();
    }
    dispatcher.FinishGroup();
}

TEST(SingleDispatcherTest, DispatchDataset) {
    MockDatasetManager manager;
    MockDataset root(0);
    {
        InSequence in_sequence;

        root.AddData("1").AddData("2").SetIsReady();
        EXPECT_CALL(root, NewIterator());
        EXPECT_CALL(root, Release());
    }

    SingleDispatcher dispatcher("hehe", 0);
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

TEST(SingleStreamDispatcherTest, DispatchObjectAtSameLevel) {
    SingleStreamDispatcher dispatcher("hehe", 0);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireObject(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotObject("1"));
        EXPECT_CALL(listener, GotObject("2"));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(listener, GotObject("3"));
        EXPECT_CALL(listener, GotDone());
    }


    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&ONE));
    EXPECT_TRUE(dispatcher.EmitBinary(TWO));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitObject(&THREE));
    dispatcher.Done();
}

TEST(SingleStreamDispatcherTest, DispatchBinaryAtSameLevel) {
    SingleStreamDispatcher dispatcher("hehe", 0);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireBinary(dispatcher.GetSource(0));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotBinary("1"));
        EXPECT_CALL(listener, GotBinary("2"));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(listener, GotBinary("3"));
        EXPECT_CALL(listener, GotDone());
    }


    dispatcher.BeginGroup("a");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitBinary(ONE));
    EXPECT_TRUE(dispatcher.EmitObject(&TWO));
    dispatcher.Done();

    dispatcher.BeginGroup("b");
    dispatcher.FinishGroup();
    EXPECT_TRUE(dispatcher.EmitBinary(THREE));
    dispatcher.Done();
}

TEST(SingleStreamDispatcherTest, DispatchStreamAtHigherLevel) {
    SingleStreamDispatcher dispatcher("hehe", 2);
    dispatcher.SetObjector(kStringObjector);

    StringListener listener;
    Source::Handle* handle = listener.RequireObjectAndBinary(dispatcher.GetSource(1));
    CHECK_NOTNULL(handle);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotObjectAndBinary(ONE, "1"));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(listener, GotObjectAndBinary(THREE, "3"));
        EXPECT_CALL(listener, GotDone());
    }

    dispatcher.BeginGroup("");
    {
        dispatcher.BeginGroup("0");
        {
            dispatcher.BeginGroup("a");
            dispatcher.FinishGroup();
            EXPECT_TRUE(dispatcher.EmitObject(&ONE));
            dispatcher.Done();

            handle->Done();

            dispatcher.BeginGroup("b");
            dispatcher.FinishGroup();
            EXPECT_FALSE(dispatcher.EmitObject(&TWO));
            dispatcher.Done();
        }
        dispatcher.FinishGroup();
    }
    {
        dispatcher.BeginGroup("0");
        {
            dispatcher.BeginGroup("a");
            dispatcher.FinishGroup();
            EXPECT_TRUE(dispatcher.EmitBinary(THREE));
            dispatcher.Done();
        }
        dispatcher.FinishGroup();
    }
    dispatcher.FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
