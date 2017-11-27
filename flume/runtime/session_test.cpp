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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "flume/runtime/session.h"

#include "boost/foreach.hpp"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace {

bool NodeIdCollectionEquals(const Session::NodeIdCollection& list1, const Session::NodeIdCollection& list2) {
    BOOST_FOREACH(const std::string& id, list1) {
        if (list2.end() == list2.find(id)) {
            return false;
        }
    }

    BOOST_FOREACH(const std::string& id, list2) {
        if (list1.end() == list1.find(id)) {
            return false;
        }
    }
    return true;
}

bool SessionEquals(const Session& session1, const Session& session2) {
    const Session::NodeIdCollection& cache_ids = session1.GetCachedNodeIds();
    BOOST_FOREACH(const std::string& id, cache_ids) {
        if (session1.GetCachePathFromId(id) != session2.GetCachePathFromId(id)) {
            return false;
        }
    }
    return NodeIdCollectionEquals(session1.GetSunkNodeIds(), session2.GetSunkNodeIds()) &&
        NodeIdCollectionEquals(session1.GetCachedNodeIds(), session2.GetCachedNodeIds());
}

}  // namespace

TEST(SessionTest, AddAndGet) {
    Session session;

    session.AddSunkNodeId("ACDC");
    session.AddSunkNodeId("ADCD");
    session.AddSunkNodeId("2268");

    session.AddCachedNodeId("386");
    session.AddCachedNodeId("486");
    session.AddCachedNodeId("386");

    session.SetCachePathForId("386", "//abc//def");
    session.SetCachePathForId("486", "//def//mmk");

    const Session::NodeIdCollection& sunk_node_ids = session.GetSunkNodeIds();
    const Session::NodeIdCollection& cached_node_ids = session.GetCachedNodeIds();

    EXPECT_EQ(3u, sunk_node_ids.size());
    EXPECT_EQ(2u, cached_node_ids.size());

    EXPECT_TRUE(sunk_node_ids.find("ACDC") != sunk_node_ids.end());
    EXPECT_TRUE(sunk_node_ids.find("ADCD") != sunk_node_ids.end());
    EXPECT_TRUE(sunk_node_ids.find("2268") != sunk_node_ids.end());
    EXPECT_TRUE(cached_node_ids.find("386") != cached_node_ids.end());
    EXPECT_TRUE(cached_node_ids.find("486") != cached_node_ids.end());

    EXPECT_EQ("//abc//def", session.GetCachePathFromId("386"));
    EXPECT_EQ("//def//mmk", session.GetCachePathFromId("486"));
}

TEST(SessionTest, Clone) {
    Session session;

    session.AddSunkNodeId("AA1");
    session.AddSunkNodeId("BB2");
    session.AddCachedNodeId("CC10");
    session.AddCachedNodeId("CC11");

    session.SetCachePathForId("CC10", "//cc");
    session.SetCachePathForId("CC11", "rtb");

    toft::scoped_ptr<Session> cloned(session.Clone());

    EXPECT_TRUE(SessionEquals(session, *cloned));
}

TEST(SessionDeathTest, SessionDeath) {
    Session session;

    session.AddCachedNodeId("CC10");
    session.AddCachedNodeId("CC11");

    EXPECT_DEATH(session.GetCachePathFromId("DD"), "Cannot find node ID");
    EXPECT_DEATH(session.SetCachePathForId("DD", "whatever"), "Cannot find node ID");
    EXPECT_DEATH(session.GetCachePathFromId("CC10"), "Cannot find path for node");

    session.SetCachePathForId("CC10", "abc");
    session.SetCachePathForId("CC10", "abc");

    EXPECT_DEATH(session.SetCachePathForId("CC10", "def"), "Failed: trying to replace");
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

