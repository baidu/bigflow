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
//         Pan Yuchang <bigflow-opensource@baidu.com>

#include "flume/planner/common/promote_partial_units_pass.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

using ::testing::ElementsAre;

class PromotePartialUnitsPassTest : public PassTest {
protected:
    typedef PromotePartialUnitsPass::FullShuffleCountAnalysis FullShuffleCountAnalysis;
    typedef PromotePartialUnitsPass::PromotableAnalysis PromotableAnalysis;
    typedef PromotePartialUnitsPass::PromoteUnitsPass PromoteUnitsPass;

    class FullShuffleCountImpl : public TagDesc {
    public:
        explicit FullShuffleCountImpl(int count) : m_count(count) {}

    private:
        typedef PromotePartialUnitsPass::FullShuffleCount FullShuffleCount;

        void Set(Unit* unit) {
            *unit->get<FullShuffleCount>() = m_count;
        }

        bool Test(Unit* unit) {
            if (!unit->has<FullShuffleCount>()) {
                return false;
            }
            return *unit->get<FullShuffleCount>() == m_count;
        }

        int m_count;
    };

    TagDescRef FullShuffleCount(int count) {
        return TagDescRef(new FullShuffleCountImpl(count));
    }

    class ShouldPromoteImpl : public TagDesc {
    public:
        explicit ShouldPromoteImpl(bool expectation) : m_expectation(expectation) {}

    private:
        typedef PromotePartialUnitsPass::ShouldPromote ShouldPromote;

        void Set(Unit* unit) {
            if (m_expectation) {
                unit->set<ShouldPromote>();
            }
        }

        bool Test(Unit* unit) {
            return unit->has<ShouldPromote>() == m_expectation;
        }

        bool m_expectation;
    };

    TagDescRef ShouldPromote(bool expectation) {
        return TagDescRef(new ShouldPromoteImpl(expectation));
    }

    class IsPromotedImpl : public TagDesc {
    public:
        explicit IsPromotedImpl(bool expectation) : m_expectation(expectation) {}

    private:
        typedef PromotePartialUnitsPass::IsPromoted IsPromoted;

        void Set(Unit* unit) {
            if (m_expectation) {
                unit->set<IsPromoted>();
            }
        }

        bool Test(Unit* unit) {
            return unit->has<IsPromoted>() == m_expectation;
        }

        bool m_expectation;
    };

    TagDescRef IsPromoted(bool expectation) {
        return TagDescRef(new IsPromotedImpl(expectation));
    }
};

TEST_F(PromotePartialUnitsPassTest, SimpleShuffleCount) {
    PlanDesc root = Job(), task1 = Task(), task2 = Task();
    PlanDesc input = InputScope(), group1 = GroupScope();
    PlanDesc load = LoadNode(), sn1 = ShuffleNode(), sn2 = ShuffleNode(),
             p1 = ProcessNode(), p2 = ProcessNode();

    PlanDesc origin = root[
        task1[
            load
        ],
        task2[
            group1[
                sn1,
                sn2,
                p1,
                p2
            ]
        ]
    ]
    | load >> sn1 >> PARTIAL >> p1
    | load >> sn2 >> p2;

    PlanDesc expected = root[
        task1[
            load
        ],
        task2[
            group1[
                sn1 +FullShuffleCount(0),
                sn2 +FullShuffleCount(1),
                p1,
                p2
            ]
        ]
    ]
    | load >> sn1 >> PARTIAL >> p1
    | load >> sn2 >> p2;

    ExpectPass<FullShuffleCountAnalysis>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, NestedShuffleCount) {
    PlanDesc root = Job(), task1 = Task(), task2 = Task();
    PlanDesc input = InputScope(), group1 = GroupScope(),
             group2 = GroupScope(), group3 = GroupScope();
    PlanDesc load = LoadNode(), sn11 = ShuffleNode(), sn12 = ShuffleNode(),
             u = UnionNode(), sn21 = ShuffleNode(), sn31 = ShuffleNode(),
             p1 = ProcessNode(), p2 = ProcessNode();

    PlanDesc origin = root[
        task1[
            load
        ],
        task2[
            group1[
                sn11,
                sn12,
                u,
                group2[
                    sn21,
                    p1
                ],
                group3[
                    sn31,
                    p2
                ]
            ]
        ]
    ]
    | load >> sn11 >> u
    | load >> sn12 >> u
    | u >> sn21 >> p1
    | u >> sn31 >> p2;

    PlanDesc expected = root[
        task1[
            load
        ],
        task2[
            group1[
                sn11 +FullShuffleCount(2),
                sn12 +FullShuffleCount(2),
                u +FullShuffleCount(2),
                group2[
                    sn21 +FullShuffleCount(1),
                    p1
                ],
                group3[
                    sn31 +FullShuffleCount(1),
                    p2
                ]
            ]
        ]
    ]
    | load >> sn11 >> u
    | load >> sn12 >> u
    | u >> sn21 >> p1
    | u >> sn31 >> p2;

    ExpectPass<FullShuffleCountAnalysis>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, AnalyzeHeadingShuffleNode) {
    PlanDesc load = LoadNode();
    PlanDesc sn = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc p = ProcessNode();

    PlanDesc origin = PlanRoot()[
        Task()[
            InputScope()[
                load
            ]
        ],
        Task()[
            BucketScope()[
                sn,
                p
            ]
        ]
    ]
    | load >> sn >> p;

    PlanDesc expected = PlanRoot()[
        Task()[
            InputScope()[
                load +ShouldPromote(false)
            ]
        ],
        Task()[
            BucketScope()[
                sn +ShouldPromote(true),
                p +ShouldPromote(false)
            ]
        ]
    ]
    | load >> sn >> p;

    ExpectPass<PromotableAnalysis>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, AnalyzeNestedShuffleNode) {
    PlanDesc load = LoadNode();
    PlanDesc sn0 = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc sn1 = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc sn2 = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc sn3 = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc c = ChannelUnit();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc p3 = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            InputScope()[
                load
            ],
            GroupScope()[
                sn0
            ]
        ],
        Task()[
            GroupScope()[
                c,
                GroupScope()[
                    sn1, p1
                ],
                GroupScope()[
                    sn2, p2
                ],
                GroupScope()[
                    sn3, p3
                ]
            ]
        ]
    ]
    | load >> sn0 >> c
    | c >> sn1 >> p1
    | c >> sn2 >> p2
    | c >> sn3 >> PARTIAL >> p3;

    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                load +ShouldPromote(false)
            ],
            GroupScope()[
                sn0 +ShouldPromote(false)
            ]
        ],
        Task()[
            GroupScope()[
                c +ShouldPromote(false),
                GroupScope()[
                    sn1 +ShouldPromote(false), p1 +ShouldPromote(false)
                ],
                GroupScope()[
                    sn2 +ShouldPromote(false), p2 +ShouldPromote(false)
                ],
                GroupScope()[
                    sn3 +ShouldPromote(true), p3 +ShouldPromote(false)
                ]
            ]
        ]
    ]
    | load >> sn0 >> c
    | c >> sn1 >> p1
    | c >> sn2 >> p2
    | c >> sn3 >> PARTIAL >> p3;

    ExpectPass<PromotableAnalysis>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, AnalyzeUnionNode) {
    PlanDesc root = Job(), task1 = Task(), task2 = Task(), input = InputScope();
    PlanDesc load = LoadNode(), c = ChannelUnit(), u = UnionNode(), p = ProcessNode();

    PlanDesc origin = root[
        task1[
            input[
                load
            ]
        ],
        task2[
            c,
            u,
            p
        ]
    ]
    | load >> c >> u >> p;

    PlanDesc expected = root[
        task1[
            input[
                load +ShouldPromote(false)
            ]
        ],
        task2[
            c +ShouldPromote(false),
            u +ShouldPromote(true),
            p +ShouldPromote(false)
        ]
    ]
    | load >> c >> u >> p;


    ExpectPass<PromotableAnalysis>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, AnalyzeProcessNode) {
    PlanDesc load = LoadNode();
    PlanDesc c1 = ChannelUnit();
    PlanDesc c2 = ChannelUnit();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            InputScope()[
                load
            ]
        ],
        Task()[
            c1,
            c2,
            p1,
            p2
        ]
    ]
    | load >> c1
    | load >> c2
    | c1 >> PARTIAL >> p1
    | c1 >> PARTIAL >> p2
    | c2 >> PARTIAL >> p1
    | c2 >> p2;

    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                load +ShouldPromote(false)
            ]
        ],
        Task()[
            c1 +ShouldPromote(false),
            c2 +ShouldPromote(false),
            p1 +ShouldPromote(true),
            p2 +ShouldPromote(false)
        ]
    ]
    | load >> c1
    | load >> c2
    | c1 >> PARTIAL >> p1
    | c1 >> PARTIAL >> p2
    | c2 >> PARTIAL >> p1
    | c2 >> p2;

    ExpectPass<PromotableAnalysis>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, RemoveOnlyChannelPath) {
    PlanDesc load = LoadNode();
    PlanDesc c = ChannelUnit(load.identity());
    PlanDesc p = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            InputScope()[
                load
            ]
        ],
        Task()[
            GroupScope()[
                c
            ]
        ],
        Task()[
            p
        ]
    ]
    | load >> c >> p;

    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                load
            ]
        ],
        Task()[
            GroupScope()[
                c
            ]
        ],
        Task()[
            p
        ]
    ]
    | load >> c
    | load >> p;

    ExpectPass<PromoteUnitsPass>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, CopyUnitWithSimpleFrom) {
    PlanDesc load = LoadNode();
    PlanDesc p = ProcessNode();
    PlanDesc target = LeafUnit();
    PlanDesc copy = LeafUnit(target.identity());

    PlanDesc origin = Job()[
        Task()[
            InputScope()[
                load
            ]
        ],
        Task()[
            GroupScope()[
                target +PbNodeTag(PbLogicalPlanNode::SHUFFLE_NODE)
                    +UnitType(Unit::SHUFFLE_NODE) +ShouldPromote(true),
                p
            ]
        ]
    ]
    | load >> target >> PARTIAL >> p;

    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                load
            ],
            GroupScope()[
                copy +UnitType(Unit::SHUFFLE_NODE) +ShouldPromote(false) +IsPromoted(true)
            ]
        ],
        Task()[
            GroupScope()[
                target +UnitType(Unit::CHANNEL) +ShouldPromote(false) + IsPromoted(false),
                p
            ]
        ]
    ]
    | load >> copy >> target >> PARTIAL >> p;

    ExpectPass<PromoteUnitsPass>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, CopyUnitWithMultiFrom) {
    PlanDesc l1 = LoadNode();
    PlanDesc l2 = LoadNode();
    PlanDesc target = LeafUnit();
    PlanDesc copy1 = LeafUnit(target.identity());
    PlanDesc copy2 = LeafUnit(target.identity());

    PlanDesc origin = Job()[
        Task()[
            InputScope()[
                l1
            ]
        ],
        Task()[
            InputScope()[
                l2
            ]
        ],
        Task()[
            target +UnitType(Unit::UNION_NODE) +ShouldPromote(true)
        ]
    ]
    | l1 >> target
    | l2 >> target;

    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                l1
            ],
            copy1 +UnitType(Unit::UNION_NODE) +ShouldPromote(false) + IsPromoted(true)
        ],
        Task()[
            InputScope()[
                l2
            ],
            copy2 +UnitType(Unit::UNION_NODE) +ShouldPromote(false) + IsPromoted(true)
        ],
        Task()[
            target +UnitType(Unit::CHANNEL) +ShouldPromote(false) + IsPromoted(false)
        ]
    ]
    | l1 >> copy1 >> target
    | l2 >> copy2 >> target;

    ExpectPass<PromoteUnitsPass>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, CopyUnitCrossChannel) {
    PlanDesc src = LeafUnit();
    PlanDesc channel = ChannelUnit(src.identity());
    PlanDesc target = LeafUnit();
    PlanDesc dst = SinkNode();
    PlanDesc copy = LeafUnit(target.identity());

    PlanDesc origin = Job()[
        Task()[
            src
        ],
        Task()[
            channel,
            target +PbNodeTag(PbLogicalPlanNode::UNION_NODE)
                   +UnitType(Unit::UNION_NODE) +ShouldPromote(true),
            dst
        ]
    ]
    | src >> channel >> target >> dst;

    PlanDesc expected = Job()[
        Task()[
            src,
            copy +UnitType(Unit::UNION_NODE) +ShouldPromote(false) +IsPromoted(true)
        ],
        Task()[
            channel,
            target +UnitType(Unit::CHANNEL) +ShouldPromote(false) +IsPromoted(false),
            dst
        ]
    ]
    | src >> copy >> target >> dst
    | src >> channel;

    ExpectPass<PromoteUnitsPass>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, CopyUnitInSameTask) {
    PlanDesc s1 = LeafUnit();
    PlanDesc s2 = LeafUnit();
    PlanDesc p = ProcessNode();
    PlanDesc c = ChannelUnit(s2.identity());
    PlanDesc target = LeafUnit();
    PlanDesc remote_copy = LeafUnit(target.identity());
    PlanDesc local_copy = LeafUnit(target.identity());

    PlanDesc origin = Job()[
        Task()[
            s1,
            s2
        ],
        Task()[
            p,
            c,
            target +PbNodeTag(PbLogicalPlanNode::UNION_NODE)
                   +UnitType(Unit::UNION_NODE) +ShouldPromote(true)
        ]
    ]
    | s1 >> p >> target
    | s2 >> c >> target;

    PlanDesc expected = Job()[
        Task()[
            s1,
            s2,
            remote_copy +UnitType(Unit::UNION_NODE) +ShouldPromote(false) +IsPromoted(true)
        ],
        Task()[
            p,
            local_copy +UnitType(Unit::UNION_NODE) +ShouldPromote(false) +IsPromoted(true),
            c,
            target +UnitType(Unit::CHANNEL) +ShouldPromote(false) +IsPromoted(false)
        ]
    ]
    | s1 >> p >> local_copy >> target
    | s2 >> remote_copy >> target
    | s2 >> c;


    ExpectPass<PromoteUnitsPass>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, SimpleRecursivePromote) {
    PlanDesc l = LeafUnit();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc c = ChannelUnit(p2.identity());
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            l
        ],
        Task()[
            p1,
            p2,
            s
        ]
    ]
    | l >> PARTIAL >> p1 >> PARTIAL >> p2 >> s;

    PlanDesc expected = Job()[
        Task()[
            l,
            p1,
            p2
        ],
        Task()[
            c,
            s
        ]
    ]
    | l >> PARTIAL >> p1 >> PARTIAL >> p2 >> c >> s;

    ExpectPass<PromotePartialUnitsPass>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, SpecialRecursivePromote) {
    PlanDesc l1 = LeafUnit();
    PlanDesc l2 = LeafUnit();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc c = ChannelUnit(p2.identity());
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            l1,
            l2
        ],
        Task()[
            p1,
            p2,
            s
        ]
    ]
    | l1 >> PARTIAL >> p1 >> PARTIAL >> p2 >> s
    | l2 >> PARTIAL >> p2;

    PlanDesc expected = Job()[
        Task()[
            l1,
            l2,
            p1,
            p2
        ],
        Task()[
            c,
            s
        ]
    ]
    | l1 >> PARTIAL >> p1 >> PARTIAL >> p2 >> c >> s
    | l2 >> PARTIAL >> p2;

    ExpectPass<PromotePartialUnitsPass>(origin, expected);
}

TEST_F(PromotePartialUnitsPassTest, RecursiveAndNestedPromote) {
    PlanDesc l1 = LoadNode();
    PlanDesc l2 = LoadNode();
    PlanDesc l3 = LoadNode();
    PlanDesc sn1 = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc sn2 = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc sn3 = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc u = UnionNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc p3 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            InputScope()[
                l1
            ]
        ],
        Task()[
            InputScope()[
                l2
            ]
        ],
        Task()[
            InputScope()[
                l3
            ]
        ],
        Task()[
            u,
            GroupScope()[
                sn1,
                sn2,
                GroupScope()[
                    sn3,
                    p1,
                    p2
                ],
                p3,
                s
            ]
        ]
    ]
    | l2 >> u
    | l3 >> u
    | l1 >> sn1 >> PARTIAL >> p3
    | u >> sn2 >> sn3 >> PARTIAL >> p1 >> p2 >> PARTIAL >> p3
    | p3 >> s;

    PlanDesc u_ = UnionNode();
    PlanDesc sn2_ = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc sn3_ = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc p1_ = ProcessNode();
    PlanDesc p3_ = ProcessNode(p3.identity());
    PlanDesc c_p1 = ChannelUnit(p1.identity());
    PlanDesc c_p3 = ChannelUnit();

    PlanDesc expected = Job()[
        Task()[
            InputScope()[
                l1
            ],
            GroupScope()[
                sn1,
                p3
            ]
        ]
        | l1 >> sn1 >> PARTIAL >> p3,
        Task()[
            InputScope()[
                l2
            ],
            u,
            GroupScope()[
                sn2,
                GroupScope()[
                    sn3,
                    p1
                ]
            ]
        ]
        | l2 >> u >> sn2 >> sn3 >> PARTIAL >> p1,
        Task()[
            InputScope()[
                l3
            ],
            u_,
            GroupScope()[
                sn2_,
                GroupScope()[
                    sn3_,
                    p1_
                ]
            ]
        ]
        | l3 >> u_ >> sn2_ >> sn3_ >> PARTIAL >> p1_,
        Task()[
            GroupScope()[
                GroupScope()[
                    c_p1,
                    p2
                ],
                p3_,
                c_p3,
                s
            ]
        ]
        | p1 >> c_p1
        | p1_ >> c_p1
        | c_p1 >> p2 >> p3_ >> c_p3
        | p3 >> c_p3
        | c_p3 >> s
    ];

    ExpectPass<PromotePartialUnitsPass>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
