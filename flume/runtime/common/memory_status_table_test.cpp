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

#include "flume/runtime/common/memory_status_table.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/logical_plan.h"
#include "flume/core/partitioner.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/proto/logical_plan.pb.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::ElementsAre;
using ::testing::IsEmpty;

using core::LogicalPlan;
using core::EncodePartition;

std::vector<toft::StringPiece> AsKeys(const toft::StringPiece& arg0 = toft::StringPiece(),
                                      const toft::StringPiece& arg1 = toft::StringPiece(),
                                      const toft::StringPiece& arg2 = toft::StringPiece(),
                                      const toft::StringPiece& arg3 = toft::StringPiece()) {
    const toft::StringPiece args[] = { arg0, arg1, arg2, arg3 };

    std::vector<toft::StringPiece> keys;
    for (size_t i = 0; i < 4; ++i) {
        if (args[i].empty()) {
            break;
        }
        keys.push_back(args[i]);
    }
    return keys;
}

TEST(MemoryStatusTableTest, Empty) {
    std::auto_ptr<LogicalPlan> plan(new LogicalPlan());

    PbLogicalPlan message = plan->ToProtoMessage();
    MemoryStatusTable status_table;
    status_table.Initialize(message.scope(), message.node());
}

TEST(MemoryStatusTableTest, NodeOperations) {
    std::auto_ptr<LogicalPlan> plan(new LogicalPlan());
    LogicalPlan::Node* node = plan->Load("")->By<MockLoader>()->As<MockObjector>()->RemoveScope();

    PbLogicalPlan message = plan->ToProtoMessage();
    MemoryStatusTable status_table;
    status_table.Initialize(message.scope(), message.node());

    StatusTable::NodeVisitor* visitor =
            status_table.GetNodeVisitor(node->identity(), AsKeys("worker"));
    EXPECT_EQ(true, visitor->IsValid());
    EXPECT_EQ(false, visitor->Read(NULL));
    visitor->Update("value");
    visitor->Release();

    std::string value;
    visitor = status_table.GetNodeVisitor(node->identity(), AsKeys("worker"));
    EXPECT_EQ(true, visitor->IsValid());
    EXPECT_EQ(true, visitor->Read(&value));
    EXPECT_EQ("value", value);

    visitor->Invalidate();
    EXPECT_EQ(false, visitor->IsValid());
    visitor->Release();

    visitor = status_table.GetNodeVisitor(node->identity(), AsKeys("worker"));
    EXPECT_EQ(false, visitor->IsValid());
    visitor->Release();
}

void Update(MemoryStatusTable* table, const LogicalPlan::Node* node,
            const std::vector<toft::StringPiece>& keys, const std::string& value) {
    StatusTable::NodeVisitor* visitor = table->GetNodeVisitor(node->identity(), keys);
    CHECK(visitor->IsValid());
    visitor->Update(value);
    visitor->Release();
}

std::string Read(MemoryStatusTable* table, const LogicalPlan::Node* node,
                 const std::vector<toft::StringPiece>& keys) {
    std::string ret;

    StatusTable::NodeVisitor* visitor = table->GetNodeVisitor(node->identity(), keys);
    CHECK(visitor->IsValid());
    CHECK(visitor->Read(&ret));
    visitor->Release();

    return ret;
}

std::list<std::string> ListEntries(MemoryStatusTable* table,
                                   const LogicalPlan::Scope* scope,
                                   const std::vector<toft::StringPiece>& keys) {

    StatusTable::ScopeVisitor* visitor = table->GetScopeVisitor(scope->identity(), keys);
    CHECK_NOTNULL(visitor);

    StatusTable::Iterator* iterator = visitor->ListEntries();
    CHECK_NOTNULL(iterator);

    std::list<std::string> entries;
    while (iterator->HasNext()) {
        entries.push_back(iterator->NextValue().as_string());
    }

    visitor->Release();
    return entries;
}

TEST(MemoryStatusTableTest, ListEntries) {
    std::auto_ptr<LogicalPlan> plan(new LogicalPlan());

    LogicalPlan::Node* node_0 = plan->Load("")->By<MockLoader>()->As<MockObjector>();
    const LogicalPlan::Scope* scope_0 = node_0->scope();

    LogicalPlan::Node* node_1 = node_0->DistributeInto(10);
    const LogicalPlan::Scope* scope_1 = node_1->scope();

    LogicalPlan::Node* node_2 = node_0->GroupBy<MockKeyReader>();
    const LogicalPlan::Scope* scope_2 = node_2->scope();

    MemoryStatusTable status_table;
    PbLogicalPlan message = plan->ToProtoMessage();
    status_table.Initialize(message.scope(), message.node());

    Update(&status_table, node_1, AsKeys("worker", "split-0", EncodePartition(2)), "02");
    Update(&status_table, node_1, AsKeys("worker", "split-0", EncodePartition(0)), "00");
    Update(&status_table, node_2, AsKeys("worker", "split-0", "k1"), "k11");

    Update(&status_table, node_1, AsKeys("worker", "split-1", EncodePartition(1)), "11");
    Update(&status_table, node_2, AsKeys("worker", "split-1", "k1"), "k01");
    Update(&status_table, node_2, AsKeys("worker", "split-1", "k0"), "k00");

    EXPECT_THAT(ListEntries(&status_table, scope_1, AsKeys("worker", "split-0")),
                ElementsAre(EncodePartition(0), EncodePartition(2)));
    EXPECT_THAT(ListEntries(&status_table, scope_2, AsKeys("worker", "split-0")),
                ElementsAre("k1"));

    EXPECT_THAT(ListEntries(&status_table, scope_1, AsKeys("worker", "split-1")),
                ElementsAre(EncodePartition(1)));
    EXPECT_THAT(ListEntries(&status_table, scope_2, AsKeys("worker", "split-1")),
                ElementsAre("k0", "k1"));

    EXPECT_THAT(ListEntries(&status_table, scope_0, AsKeys("worker")),
                ElementsAre("split-0", "split-1"));
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
