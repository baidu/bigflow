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

#include "flume/planner/common/add_common_executor_pass.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

using ::testing::Property;

class AddCommonExecutorPassTest : public PassTest {
public:
    typedef AddCommonExecutorPass::AddLogicalExecutorPass AddLogicalExecutorPass;
    typedef AddCommonExecutorPass::AddShuffleExecutorPass AddShuffleExecutorPass;
    typedef AddCommonExecutorPass::AddPartialExecutorPass AddPartialExecutorPass;
    typedef AddCommonExecutorPass::AddCacheWriterExecutorPass AddCacheWriterExecutorPass;
    typedef AddCommonExecutorPass::ShouldBeSeparated ShouldBeSeparated;

    static TagDescRef IgnoreGroup() {
        // always applied after PbNodeTag, so some checkes are omitted
        class TagDescImpl : public TagDesc {
        public:
            virtual void Set(Unit* unit)  {
                unit->get<PbLogicalPlanNode>().mutable_process_node()->set_is_ignore_group(true);
            }

            virtual bool Test(Unit* unit) {
                return unit->get<PbLogicalPlanNode>().process_node().is_ignore_group();
            }
        };
        return TagDescRef(static_cast<TagDesc*>(new TagDescImpl));
    }

    static TagDescRef HasSession() {
        class TagDescImpl : public TagDesc {
        public:
            virtual void Set(Unit* unit)  {
                unit->get<Session>().Assign(&m_session);
            }

            virtual bool Test(Unit* unit) {
                return !unit->get<Session>().is_null();
            }
            runtime::Session m_session;
        };
        return TagDescRef(static_cast<TagDesc*>(new TagDescImpl));
    }
    runtime::Session m_session;
};

TEST_F(AddCommonExecutorPassTest, AssignShuffleExecutor) {
    PlanDesc l = LeafUnit();
    PlanDesc c = ChannelUnit();
    PlanDesc sn = ShuffleNode();
    PlanDesc p = ProcessNode();
    PlanDesc s = SinkNode();
    PlanDesc sn1 = ShuffleNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc s1 = SinkNode();

    PlanDesc origin = (Task()[
        l,
        GroupScope()[
            c,
            sn,
            LogicalExecutor()[
                p +ExecutedByFather()
            ]
        ],
        GroupScope()[
            sn1,
            LogicalExecutor()[
                p1 +ExecutedByFather()
            ]
        ] +IsInfinite(),
        s,
        s1
    ] +HasSession())
    | l >> c >> p
    | l >> sn >> p
    | l >> sn1 >> p1
    | p >> s
    | p1 >> s1;

    PlanDesc expected = Task()[
        l,
        ShuffleExecutor(PbScope::GROUP)[
            c +ExecutedByFather(),
            sn +ExecutedByFather(),
            LogicalExecutor()[
                p +ExecutedByFather()
            ]
        ],
        StreamShuffleExecutor(PbScope::GROUP)[
            sn1 +ExecutedByFather(),
            LogicalExecutor()[
                p1 +ExecutedByFather()
            ]
        ],
        s,
        s1
    ]
    | l >> c >> p
    | l >> sn >> p
    | l >> sn1 >> p1
    | p >> s
    | p1 >> s1;
    ExpectPass<AddShuffleExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, AddLogicalExecutor) {
    PlanDesc l = LoadNode();
    PlanDesc u = UnionNode();
    PlanDesc p = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = (Task() + HasSession())[
        InputScope()[
            l +IsInfinite(),
            p,
            u,
            s
        ]
    ]
    | l >> p >> u >> s;

    PlanDesc expected = Task()[
        InputScope()[
            StreamLogicalExecutor()[
                l +ExecutedByFather() +NewTagDesc<DisablePriority>()
            ],
            LogicalExecutor()[
                p +ExecutedByFather() +NewTagDesc<DisablePriority>()
            ],
            LogicalExecutor()[
                u +ExecutedByFather() +NewTagDesc<DisablePriority>()
            ],
            LogicalExecutor()[
                s +ExecutedByFather() +NewTagDesc<DisablePriority>()
            ]
        ]
    ]
    | l >> p >> u >> s;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, FixShuffleChannel) {
    PlanDesc c_m0 = ChannelUnit();
    PlanDesc c_m1 = ChannelUnit();
    PlanDesc c_r0 = ChannelUnit();
    PlanDesc c_r1 = ChannelUnit();
    PlanDesc p = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = (Job() + HasSession()) [
        Task()[
            c_m0
        ],
        Task()[
            c_m1
        ],
        Task()[
            BucketScope()[
                GroupScope()[
                    c_r0, p,
                    c_r1
                ],
                s
            ]
        ]
    ]
    | c_m0 >> c_r0 >> p >> c_r1
    | c_m1 >> c_r1
    | c_r1 >> s;

    PlanDesc c_r0_ = ChannelUnit();
    PlanDesc c_r1_ = ChannelUnit();
    PlanDesc c_r0__ = ChannelUnit(c_r0.identity());
    PlanDesc c_r1__ = ChannelUnit();
    PlanDesc u = LeafUnit(c_r1.identity()) + UnitType(Unit::UNION_NODE);

    PlanDesc expected = Job() [
        Task()[
            c_m0
        ],
        Task()[
            c_m1
        ],
        Task()[
            ShuffleExecutor(PbScope::BUCKET)[
                c_r0_ +ExecutedByFather(),
                c_r1_ +ExecutedByFather(),
                ShuffleExecutor(PbScope::GROUP)[
                    c_r0__ +ExecutedByFather(),
                    LogicalExecutor()[
                        p +ExecutedByFather()
                    ],
                    c_r1__ +ExecutedByFather(),
                    LogicalExecutor()[
                        u +PbNodeTag(PbLogicalPlanNode::UNION_NODE) +ExecutedByFather()
                    ]
                ],
                LogicalExecutor()[
                    s +ExecutedByFather()
                ]
            ]
        ]
    ]
    | c_m0 >> c_r0_ >> c_r0__ >> p >> u
    | c_m1 >> c_r1_ >> c_r1__ >> u
    | u >> s;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, CreatePartialExecutor) {
    PlanDesc l = LoadNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = (Job() + HasSession()) [
        Task()[
            InputScope()[
                l
            ],
            p0,
            p1 +IsPartial()
        ],
        Task()[
            p2,
            s
        ]
    ]
    | l >> p0 >> p1 >> p2 >> s;

    PlanDesc expected = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ]
            ],
            LogicalExecutor()[
                p0
            ],
            PartialExecutor()[
                p1
            ]
        ],
        Task()[
            LogicalExecutor()[
                p2
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l >> p0 >> p1 >> p2 >> s;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, ExcludeNonStreamingProcessor) {
    PlanDesc l = LoadNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = (Job() + HasSession()) [
        Task()[
            InputScope()[
                l
            ],
            p0 +IsPartial(),
            p1 +IsPartial(),
            p2 +IsPartial()
        ],
        Task()[
            s
        ]
    ]
    | l >> PREPARED >> p0 >> p1 >> PREPARED >> p2 >> s;

    PlanDesc expected = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ]
            ],
            LogicalExecutor()[
                p0
            ],
            PartialExecutor()[
                p1,
                LogicalExecutor()[
                    p2
                ]
            ]
        ],
        Task()[
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l >> PREPARED >> p0 >> p1 >> PREPARED >> p2 >> s;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, PromoteNestedPartialUnits) {
    PlanDesc l = LoadNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc sn0 = ShuffleNode();
    PlanDesc sn1 = ShuffleNode();
    PlanDesc sn2 = ShuffleNode();
    PlanDesc u = UnionNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = (Job() + HasSession()) [
        Task()[
            InputScope()[
                l
            ],
            BucketScope()[
                sn0 +IsPartial(), p0 +IsPartial() +IgnoreGroup(),
                GroupScope()[
                    sn1 +IsPartial(), p1 +IsPartial()
                ],
                sn2 +IsPartial(), p2 +IsPartial() +IgnoreGroup()
            ],
            u +IsPartial()
        ],
        Task()[
            s
        ]
    ]
    | u >> s
    | l >> sn0 >> p0 >> sn1 >> p1 >> u
    | l >> sn2 >> PREPARED >> p2 >> u;

    // promoted units have changed their identity
    PlanDesc sn0_ = ShuffleNode();
    PlanDesc sn1_ = ShuffleNode();
    PlanDesc sn2_ = ShuffleNode();
    PlanDesc p0_ = ProcessNode();

    PlanDesc csn1 = ChannelUnit(sn1.identity());
    PlanDesc csn1_ = ChannelUnit();
    PlanDesc csn2 = ChannelUnit(sn2.identity());

    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ]
            ],
            PartialExecutor()[
                sn0_ +MatchTag<PbScope>(
                    Property(&PbScope::type, PbScope::BUCKET)
                ),
                sn1_ +MatchTag<PbScope>(
                    Property(&PbScope::type, PbScope::GROUP)
                ),
                sn2_ +MatchTag<PbScope>(
                    Property(&PbScope::type, PbScope::BUCKET)
                ),
                p0_,
                ShuffleExecutor(PbScope::BUCKET)[
                    csn1_,
                    ShuffleExecutor(PbScope::GROUP)[
                        csn1,
                        LogicalExecutor()[
                            p1
                        ]
                    ],
                    csn2,
                    LogicalExecutor()[
                        p2
                    ]
                ],
                LogicalExecutor()[
                    u
                ]
            ]
        ],
        Task()[
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | u >> s
    | l >> sn0_ >> p0_ >> sn1_ >> csn1_ >> csn1 >> p1 >> u
    | l >> sn2_ >> csn2 >> PREPARED >> p2 >> u;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, SeparateIndependantUnits) {
    PlanDesc l = LoadNode();
    PlanDesc sn_a = ShuffleNode();
    PlanDesc sn_b0 = ShuffleNode();
    PlanDesc sn_b1 = ShuffleNode();
    PlanDesc pa = ProcessNode();
    PlanDesc pb = ProcessNode();
    PlanDesc u = UnionNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = (Job() + HasSession()) [
        Task()[
            InputScope()[
                l
            ],
            BucketScope()[
                sn_a +IsPartial(), pa +IsPartial(),
                sn_b0 +IsPartial(),
                GroupScope()[
                    sn_b1 +IsPartial()
                ],
                pb +IsPartial()
            ]
        ],
        Task()[
            u, s
        ]
    ]
    | u >> s
    | l >> sn_a >> pa >> u
    | l >> sn_b0 >> sn_b1 >> pb >> u;

    // promoted units have changed their identity
    PlanDesc sn_a_ = ShuffleNode();
    PlanDesc sn_b0_ = ShuffleNode();
    PlanDesc sn_b1_ = ShuffleNode();
    PlanDesc csn_a = ChannelUnit();
    PlanDesc csn_b0 = ChannelUnit();
    PlanDesc csn_b1 = ChannelUnit();

    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ]
            ],
            PartialExecutor()[
                sn_a_,
                ShuffleExecutor(PbScope::BUCKET)[
                    csn_a,
                    LogicalExecutor()[
                        pa
                    ]
                ],
                sn_b0_,
                sn_b1_,
                ShuffleExecutor(PbScope::BUCKET)[
                    csn_b0,
                    ShuffleExecutor(PbScope::GROUP)[
                        csn_b1
                    ],
                    LogicalExecutor()[
                        pb
                    ]
                ]
            ]
        ],
        Task()[
            LogicalExecutor()[
                u
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | u >> s
    | l >> sn_a_ >> csn_a >> pa >> u
    | l >> sn_b0_ >> sn_b1_ >> csn_b0 >> csn_b1 >> pb >> u;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, SeparateIndependantUnitsInvalid) {
    PlanDesc l = LoadNode();
    PlanDesc pa = ProcessNode();
    PlanDesc pb = ProcessNode();
    PlanDesc u = UnionNode();
    PlanDesc s = SinkNode();

    PlanDesc sn_a = ShuffleNode();
    PlanDesc sn_b0 = ShuffleNode();
    PlanDesc sn_b1 = ShuffleNode();
    PlanDesc csn_a = ChannelUnit();
    PlanDesc csn_b0 = ChannelUnit();
    PlanDesc csn_b1 = ChannelUnit();
    PlanDesc origin = Job()[
        Task()[
            InputScope()[
                    l
            ],
            PartialExecutor()[
                sn_a,
                ShuffleExecutor(PbScope::BUCKET)[
                    csn_a, pa +NewTagDesc<ShouldBeSeparated>()
                ],
                sn_b0,
                sn_b1,
                ShuffleExecutor(PbScope::BUCKET)[
                    csn_b0,
                    ShuffleExecutor(PbScope::GROUP)[
                        csn_b1
                    ],
                    pb +NewTagDesc<ShouldBeSeparated>()
                ]
            ]
        ],
        Task()[
            u, s
        ]
    ]
    | u >> s
    | l >> sn_a >> csn_a >> pa >> u
    | l >> sn_b0 >> sn_b1 >> csn_b0 >> csn_b1 >> pb >> u;


    PlanDesc sn_a_ = ShuffleNode();
    PlanDesc sn_b0_ = ShuffleNode();
    PlanDesc sn_b1_ = ShuffleNode();
    PlanDesc csn_a_ = ChannelUnit();
    PlanDesc csn_b0_ = ChannelUnit();
    PlanDesc csn_b1_ = ChannelUnit();
    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ]
            ],
            PartialExecutor()[
                sn_a_,
                ShuffleExecutor(PbScope::BUCKET)[
                    csn_a_,
                    LogicalExecutor()[
                        pa +NoTag<ShouldBeSeparated>()
                    ]
                ],
                sn_b0_,
                sn_b1_,
                ShuffleExecutor(PbScope::BUCKET)[
                    csn_b0_,
                    ShuffleExecutor(PbScope::GROUP)[
                        csn_b1_
                    ],
                    LogicalExecutor()[
                        pb +NoTag<ShouldBeSeparated>()
                    ]
                ]
            ]
        ],
        Task()[
            LogicalExecutor()[
                u
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | u >> s
    | l >> sn_a_ >> csn_a_ >> pa >> u
    | l >> sn_b0_ >> sn_b1_ >> csn_b0_ >> csn_b1_ >> pb >> u;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, UpdatePartialExecutor) {
    PlanDesc l = LoadNode();
    PlanDesc p = ProcessNode();
    PlanDesc c = ChannelUnit();
    PlanDesc u = UnionNode();
    PlanDesc s0 = SinkNode();
    PlanDesc s1 = SinkNode();

    PlanDesc origin = (Job() + HasSession()) [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l +ExecutedByFather()
                ]
            ],
            PartialExecutor()[
                p +ExecutedByFather()
            ],
            ExternalExecutor()[
                c +IsPartial(), u +IsPartial()
            ]
        ],
        Task()[
            LogicalExecutor()[
                s0 +ExecutedByFather()
            ],
            LogicalExecutor()[
                s1 +ExecutedByFather()
            ]
        ]
    ]
    | l >> p >> c >> s0
    | l >> u >> s1;

    PlanDesc u_ = UnionNode();

    PlanDesc expected = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ]
            ],
            PartialExecutor()[
                p,
                u_,
                ExternalExecutor()[
                    c,
                    LogicalExecutor()[
                        u
                    ]
                ]
            ]
        ],
        Task()[
            LogicalExecutor()[
                s0
            ],
            LogicalExecutor()[
                s1
            ]
        ]
    ]
    | l >> p >> c >> s0
    | l >> u_ >> u >> s1;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, AddShuffleExecutorForLocalDistribute) {
    PlanDesc p = ProcessNode();
    PlanDesc sn = ShuffleNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            p,
            BucketScope()[
                sn,
                s
            ] +NewTagDesc<IsLocalDistribute>()
        ]
    ]
    | p >> sn >> s;

    PlanDesc expected = Job()[
        Task()[
            LogicalExecutor()[
                p +ExecutedByFather()
            ],
            ShuffleExecutor(PbScope::BUCKET)[
                sn +ExecutedByFather(),
                LogicalExecutor()[
                    s +ExecutedByFather()
                ]
            ]
        ]
    ]
    | p >> sn >> s;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

TEST_F(AddCommonExecutorPassTest, AddCacheWriterExecutor) {
    PlanDesc l = LoadNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = (Job() + HasSession()) [
        Task()[
            InputScope()[
                l +ShouldCache()
            ],
            p0,
            p1 +IsPartial() +ShouldCache()
        ],
        Task()[
            p2,
            s
        ]
    ]
    | l >> p0 >> p1 >> p2 >> s;

    PlanDesc c_l_cache = ChannelUnit();
    PlanDesc c_p1_cache = ChannelUnit();

    PlanDesc expected = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ],
                CacheWriterExecutor()[
                    c_l_cache
                ]
            ],
            LogicalExecutor()[
                p0
            ],
            LogicalExecutor()[
                p1
            ],
            CacheWriterExecutor()[
                c_p1_cache
            ]
        ],
        Task()[
            LogicalExecutor()[
                p2
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l >> c_l_cache
    | l >> p0 >> p1 >> c_p1_cache
    | l >> p0 >> p1 >> p2 >> s;

    ExpectPass<AddCommonExecutorPass>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
