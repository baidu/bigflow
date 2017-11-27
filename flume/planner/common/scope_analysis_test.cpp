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

#include "flume/planner/common/scope_analysis.h"

#include "boost/foreach.hpp"
#include "boost/range/algorithm.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Property;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class ScopeAnalysisTest : public PassTest {
protected:
    TagDescRef FakePartitioner() {
        class TagDescImpl : public TagDesc {
        public:
            virtual void Set(Unit* unit) {
                PbLogicalPlanNode* node = &unit->get<PbLogicalPlanNode>();
                PbEntity* entity = node->mutable_shuffle_node()->mutable_partitioner();
                entity->set_name("FakePartitioner");
                entity->set_config("fake-config");
            }

            virtual bool Test(Unit* unit) {
                return true;
            }
        };

        TagDesc* desc = new TagDescImpl();
        return TagDescRef(desc);
    }
};

TEST_F(ScopeAnalysisTest, EmptyPlan) {
    PlanDesc origin = PlanRoot() + Stage(TOPOLOGICAL_OPTIMIZING);

    PlanDesc expected = PlanRoot();

    ExpectPass<ScopeAnalysis>(origin, expected);
}

TEST_F(ScopeAnalysisTest, KeyScopes) {
    PlanDesc global_scope = DefaultScope();
    PlanDesc input_scope = InputScope();
    PlanDesc bucket_scope = BucketScope();
    PlanDesc group_scope = GroupScope();
    PlanDesc load_node = LoadNode();
    PlanDesc partitioner = ShuffleNode(PbShuffleNode::SEQUENCE);
    PlanDesc key_reader = ShuffleNode(PbShuffleNode::KEY);

    PlanDesc origin = (global_scope + Stage(TOPOLOGICAL_OPTIMIZING))[
        input_scope[
            load_node
        ],
        bucket_scope[
            partitioner,
            group_scope[
                key_reader
            ]
        ]
    ]
    | load_node >> partitioner >> key_reader;

    PlanDesc expected = (
        global_scope[
            input_scope[
                load_node +MatchTag<KeyScopes>(ElementsAre(
                        Property(&PbScope::id, global_scope.identity()),
                        Property(&PbScope::id, input_scope.identity())
                ))
            ] +MatchTag<KeyScopes>(ElementsAre(
                    Property(&PbScope::id, global_scope.identity())
            )),
            bucket_scope[
                partitioner +MatchTag<KeyScopes>(ElementsAre(
                        Property(&PbScope::id, global_scope.identity()),
                        Property(&PbScope::id, bucket_scope.identity())
                )),
                group_scope[
                    key_reader +MatchTag<KeyScopes>(ElementsAre(
                            Property(&PbScope::id, global_scope.identity()),
                            Property(&PbScope::id, bucket_scope.identity()),
                            Property(&PbScope::id, group_scope.identity())
                    ))
                ] +MatchTag<KeyScopes>(ElementsAre(
                        Property(&PbScope::id, global_scope.identity()),
                        Property(&PbScope::id, bucket_scope.identity())
                ))
            ] +MatchTag<KeyScopes>(ElementsAre(
                    Property(&PbScope::id, global_scope.identity())
            ))
        ] +MatchTag<KeyScopes>(IsEmpty())
    )
    | load_node >> partitioner >> key_reader;

    ExpectPass<ScopeAnalysis>(origin, expected);
}

TEST_F(ScopeAnalysisTest, HasPartitioner) {
    PlanDesc load_node = LoadNode();
    PlanDesc default_partitioner = ShuffleNode(PbShuffleNode::SEQUENCE);
    PlanDesc fake_partitioner = ShuffleNode(PbShuffleNode::SEQUENCE) + FakePartitioner();

    PlanDesc origin = (DefaultScope() + Stage(TOPOLOGICAL_OPTIMIZING))[
        InputScope()[
            load_node
        ],
        BucketScope()[
            default_partitioner
        ],
        BucketScope()[
            fake_partitioner
        ]
    ]
    | load_node >> default_partitioner
    | load_node >> fake_partitioner;

    PlanDesc expected = DefaultScope()[
        InputScope()[
            load_node
        ],
        BucketScope()[
            default_partitioner
        ] +NoTag<HasPartitioner>(),
        BucketScope()[
            fake_partitioner
        ] +NewTagDesc<HasPartitioner>()
    ]
    | load_node >> default_partitioner
    | load_node >> fake_partitioner;

    ExpectPass<ScopeAnalysis>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
