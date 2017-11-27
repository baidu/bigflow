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

#include "flume/planner/common/prepared_analysis.h"

#include "gtest/gtest.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

class PreparedAnalysisTest : public PassTest {
protected:
    TagDescRef LeastPreparedInputs(int value) {
        class LeastPreparedInputsImpl : public TagDesc {
        public:
            explicit LeastPreparedInputsImpl(int value) : m_least_prepared_inputs(value) {}

            virtual void Set(Unit* unit) {
                CHECK(unit->has<PbLogicalPlanNode>());

                PbProcessNode* node = unit->get<PbLogicalPlanNode>().mutable_process_node();
                node->set_least_prepared_inputs(m_least_prepared_inputs);
            }

            virtual bool Test(Unit* unit) {
                LOG(ERROR) << "not implemented!";
                return true;
            }

        private:
            int m_least_prepared_inputs;
        };

        TagDesc* impl = new LeastPreparedInputsImpl(value);
        return TagDescRef(impl);
    }
};

TEST_F(PreparedAnalysisTest, DependantShuffleExecutor) {
    PlanDesc output_channel_0 = ChannelUnit();
    PlanDesc output_channel_1 = ChannelUnit();
    PlanDesc input_channel_0 = ChannelUnit();
    PlanDesc input_channel_1 = ChannelUnit();
    PlanDesc merge_channel_0 = ChannelUnit();
    PlanDesc merge_channel_0_ = ChannelUnit();
    PlanDesc merge_channel_1 = ChannelUnit();
    PlanDesc merge_channel_1_ = ChannelUnit();
    PlanDesc shuffle_node = ShuffleNode();
    PlanDesc process_node = ProcessNode();
    PlanDesc union_node = UnionNode();
    PlanDesc sink_node = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            output_channel_0,
            output_channel_1
        ],
        Task()[
            ExternalExecutor()[
                input_channel_0,
                input_channel_1
            ],
            ShuffleExecutor(PbScope::GROUP)[
                merge_channel_0, ShuffleExecutor(PbScope::GROUP)[
                    merge_channel_0_,
                    LogicalExecutor()[
                        process_node
                    ]
                ],
                merge_channel_1, ShuffleExecutor(PbScope::GROUP)[
                    merge_channel_1_,
                    shuffle_node,
                    LogicalExecutor()[
                        union_node
                    ]
                ]
            ],
            LogicalExecutor()[
                sink_node
            ]
        ]
    ]
    | output_channel_0 >> input_channel_0 >> merge_channel_0
    | output_channel_1 >> input_channel_1 >> merge_channel_1
    | merge_channel_0 >> merge_channel_0_ >> process_node >> shuffle_node >> union_node
    | merge_channel_1 >> merge_channel_1_ >> union_node
    | union_node >> sink_node;

    PlanDesc expected = Job()[
        Task()[
            output_channel_0,
            output_channel_1
        ],
        Task()[
            ExternalExecutor()[
                input_channel_0 +NewValueTag<PreparePriority>(0),
                input_channel_1 +NewValueTag<PreparePriority>(0)
            ],
            ShuffleExecutor(PbScope::GROUP)[
                merge_channel_0 +NewValueTag<PreparePriority>(0),
                ShuffleExecutor(PbScope::GROUP)[
                    merge_channel_0_ +NewValueTag<PreparePriority>(0),
                    LogicalExecutor()[
                        process_node
                    ] +NoTag<PreparePriority>()
                ] +NewValueTag<PreparePriority>(0),
                merge_channel_1 +NewValueTag<PreparePriority>(1),
                ShuffleExecutor(PbScope::GROUP)[
                    shuffle_node +NewValueTag<PreparePriority>(0),
                    merge_channel_1_ +NewValueTag<PreparePriority>(1),
                    LogicalExecutor()[
                        union_node
                    ] +NoTag<PreparePriority>()
                ] +NewValueTag<PreparePriority>(1)
            ] +NewValueTag<PreparePriority>(0),
            LogicalExecutor()[
                sink_node
            ] +NoTag<PreparePriority>()
        ]
    ]
    | output_channel_0 >> input_channel_0 >> merge_channel_0
    | output_channel_1 >> input_channel_1 >> merge_channel_1
    | merge_channel_0 >> merge_channel_0_ >> process_node >> shuffle_node >> union_node
    | merge_channel_1 >> merge_channel_1_ >> union_node
    | union_node >> sink_node;

    ExpectPass<PreparedAnalysis>(origin, expected);
}

TEST_F(PreparedAnalysisTest, Prepared) {
    PlanDesc l = LoadNode();
    PlanDesc sn0 = ShuffleNode(PbShuffleNode::KEY);
    PlanDesc sn1 = ShuffleNode(PbShuffleNode::BROADCAST);
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc pa = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            ExternalExecutor()[
                l
            ],
            ShuffleExecutor(PbScope::GROUP)[
                sn0,
                sn1,
                LogicalExecutor()[
                    p0
                ],
                LogicalExecutor()[
                    p1
                ],
                LogicalExecutor()[
                    pa
                ],
                LogicalExecutor()[
                    s
                ]
            ]
        ]
    ]
    | l >> sn0 >> PREPARED >> p0 >> pa
    | l >> sn1 >> p1 >> PREPARED >> pa
    | pa >> s;

    PlanDesc expected = Job()[
        Task()[
            ExternalExecutor()[
                l +NoTag<NeedPrepare>()
            ] +NoTag<PreparePriority>(),
            ShuffleExecutor(PbScope::GROUP)[
                sn0 +NewValueTag<PreparePriority>(1) +NewTagDesc<NeedPrepare>(),
                sn1 +NewValueTag<PreparePriority>(0) +NoTag<NeedPrepare>(),
                LogicalExecutor()[
                    p0 +NoTag<NeedPrepare>()
                ] +NoTag<PreparePriority>(),
                LogicalExecutor()[
                    p1 +NewTagDesc<NeedPrepare>()
                ] +NoTag<PreparePriority>(),
                LogicalExecutor()[
                    pa +NoTag<NeedPrepare>()
                ] +NoTag<PreparePriority>(),
                LogicalExecutor()[
                    s +NoTag<NeedPrepare>()
                ] +NoTag<PreparePriority>()
            ] +NoTag<PreparePriority>()
        ]
    ]
    | l >> sn0 >> PREPARED >> p0 >> pa
    | l >> sn1 >> p1 >> PREPARED >> pa
    | pa >> s;

    ExpectPass<PreparedAnalysis>(origin, expected);
}

TEST_F(PreparedAnalysisTest, UpdatePreparedNeedsForAmbiguousPriority) {
    PlanDesc l0 = LoadNode();
    PlanDesc l1 = LoadNode();
    PlanDesc p = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            ExternalExecutor()[
                l0
            ],
            ExternalExecutor()[
                l1
            ],
            LogicalExecutor()[
                p
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l0 >> PREPARED >> p
    | l1 >> p
    | p >> s;

    PlanDesc expected = Job()[
        Task()[
            ExternalExecutor()[
                l0 +NewTagDesc<NeedPrepare>()
            ],
            ExternalExecutor()[
                l1 +NewTagDesc<NeedPrepare>()
            ],
            LogicalExecutor()[
                p
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l0 >> PREPARED >> p
    | l1 >> p
    | p >> s;

    ExpectPass<PreparedAnalysis>(origin, expected);
}

TEST_F(PreparedAnalysisTest, MergeJoin) {
    PlanDesc cout_0 = ChannelUnit();
    PlanDesc cout_1 = ChannelUnit();
    PlanDesc cin_0 = ChannelUnit();
    PlanDesc cin_1 = ChannelUnit();
    PlanDesc c0 = ChannelUnit();
    PlanDesc c1 = ChannelUnit();
    PlanDesc p = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            cout_0, cout_1
        ],
        Task()[
            ExternalExecutor()[
                cin_0, cin_1
            ],
            ShuffleExecutor(PbScope::GROUP)[
                c0,
                c1,
                LogicalExecutor()[
                    p
                ],
                LogicalExecutor()[
                    s
                ]
            ]
        ]
    ]
    | cout_0 >> cin_0 >> c0 >> PREPARED >> p
    | cout_1 >> cin_1 >> c1 >> p
    | p >> s;

    PlanDesc expected = Job()[
        Task()[
            cout_0, cout_1
        ],
        Task()[
            ExternalExecutor()[
                cin_0, cin_1
            ],
            ShuffleExecutor(PbScope::GROUP)[
                c0 +NewTagDesc<NeedPrepare>() +NewValueTag<PreparePriority>(0),
                c1 +NoTag<NeedPrepare>() +NewValueTag<PreparePriority>(1),
                LogicalExecutor()[
                    p
                ],
                LogicalExecutor()[
                    s
                ]
            ]
        ]
    ]
    | cout_0 >> cin_0 >> c0 >> PREPARED >> p
    | cout_1 >> cin_1 >> c1 >> p
    | p >> s;

    ExpectPass<PreparedAnalysis>(origin, expected);
}

TEST_F(PreparedAnalysisTest, PrepareAndNonPreparedInSamePriority) {
    PlanDesc cout_0 = ChannelUnit();
    PlanDesc cin_0 = ChannelUnit();
    PlanDesc c0 = ChannelUnit();
    PlanDesc c1 = ChannelUnit();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            cout_0
        ],
        Task()[
            ExternalExecutor()[
                cin_0
            ],
            ShuffleExecutor(PbScope::GROUP)[
                c0,
                LogicalExecutor()[
                    p0
                ],
                LogicalExecutor()[
                    p1
                ],
                LogicalExecutor()[
                    p2
                ],
                LogicalExecutor()[
                    s
                ]
            ]
        ]
    ]
    | cout_0 >> cin_0 >> c0 >> p0 >> p1 >> PREPARED >> p2 >> s
    | p0 >> p2;

    PlanDesc expected = Job()[
        Task()[
            cout_0
        ],
        Task()[
            ExternalExecutor()[
                cin_0
            ],
            ShuffleExecutor(PbScope::GROUP)[
                c0,
                LogicalExecutor()[
                    p0  +NewTagDesc<NeedPrepare>() +NewValueTag<PreparePriority>(0)
                ],
                LogicalExecutor()[
                    p1  +NewTagDesc<NeedPrepare>() +NewValueTag<PreparePriority>(0)
                ],
                LogicalExecutor()[
                    p2
                ],
                LogicalExecutor()[
                    s
                ]
            ]
        ]
    ]
    | cout_0 >> cin_0 >> c0 >> p0 >> p1 >> PREPARED >> p2 >> s
    | p0 >> p2;

    ExpectPass<PreparedAnalysis>(origin, expected);
}

TEST_F(PreparedAnalysisTest, OnePreparedNeedCycleTest) {
    PlanDesc cout_0 = ChannelUnit();
    PlanDesc cout_1 = ChannelUnit();
    PlanDesc cin_0 = ChannelUnit();
    PlanDesc cin_1 = ChannelUnit();
    PlanDesc c0 = ChannelUnit();
    PlanDesc c1 = ChannelUnit();
    PlanDesc p0 = ProcessNode();
    PlanDesc u = UnionNode();
    PlanDesc pf = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            cout_0, cout_1
        ],
        Task()[
            ExternalExecutor()[
                cin_0, cin_1
            ],
            ShuffleExecutor(PbScope::GROUP)[
                c0, c1,
                LogicalExecutor()[
                    p0
                ],
                LogicalExecutor()[
                    u
                ],
                LogicalExecutor()[
                    pf
                ]
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | cout_0 >> cin_0 >> c0 >> p0 >> PREPARED >> pf
    | cout_0 >> cin_0 >> c0 >> u
    | cout_1 >> cin_1 >> c1 >> u
    | u >> pf
    | pf >> s;

    PlanDesc expected = Job()[
        Task()[
            cout_0, cout_1
        ],
        Task()[
            ExternalExecutor()[
                cin_0, cin_1
            ],
            ShuffleExecutor(PbScope::GROUP)[
                c0 +NewValueTag<PreparePriority>(0),
                c1 +NewValueTag<PreparePriority>(1),
                LogicalExecutor()[
                    p0 +NewTagDesc<NeedPrepare>() +NewValueTag<PreparePriority>(0)
                ],
                LogicalExecutor()[
                    u +NewTagDesc<NeedPrepare>() +NewValueTag<PreparePriority>(0)
                ],
                LogicalExecutor()[
                    pf
                ]
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | cout_0 >> cin_0 >> c0 >> p0 >> PREPARED >> pf
    | cout_0 >> cin_0 >> c0 >> u
    | cout_1 >> cin_1 >> c1 >> u
    | u >> pf
    | pf >> s;

    ExpectPass<PreparedAnalysis>(origin, expected);
}

TEST_F(PreparedAnalysisTest, TwoPreparedNeedsCycleTest) {
    PlanDesc cout_0 = ChannelUnit();
    PlanDesc cout_1 = ChannelUnit();
    PlanDesc cout_2 = ChannelUnit();
    PlanDesc cin_0 = ChannelUnit();
    PlanDesc cin_1 = ChannelUnit();
    PlanDesc cin_2 = ChannelUnit();
    PlanDesc c0 = ChannelUnit();
    PlanDesc c1 = ChannelUnit();
    PlanDesc c2 = ChannelUnit();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc pa = ProcessNode();
    PlanDesc pb = ProcessNode();
    PlanDesc pf = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            cout_0, cout_1, cout_2
        ],
        Task()[
            ExternalExecutor()[
                cin_0, cin_1, cin_2
            ],
            ShuffleExecutor(PbScope::GROUP)[
                c0, c1, c2,
                LogicalExecutor()[
                    p0
                ],
                LogicalExecutor()[
                    p1
                ],
                LogicalExecutor()[
                    pa
                ],
                LogicalExecutor()[
                    pb
                ],
                LogicalExecutor()[
                    pf
                ]
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | cout_0 >> cin_0 >> c0 >> p0
    | cout_1 >> cin_1 >> c1 >> p1
    | cout_2 >> cin_2 >> c2
    | p0 >> PREPARED >> pa
    | p0 >> pb
    | p1 >> PREPARED >> pb
    | p1 >> pa
    | pa >> PREPARED >> pf
    | pb >> PREPARED >> pf
    | c2 >> pf
    | pf >> s;

    PlanDesc expected = Job()[
        Task()[
            cout_0, cout_1, cout_2
        ],
        Task()[
            ExternalExecutor()[
                cin_0, cin_1, cin_2
            ],
            ShuffleExecutor(PbScope::GROUP)[
                c0,
                c1,
                c2 +NewValueTag<PreparePriority>(2),
                LogicalExecutor()[
                    p0
                ],
                LogicalExecutor()[
                    p1
                ],
                LogicalExecutor()[
                    pa
                ],
                LogicalExecutor()[
                    pb
                ],
                LogicalExecutor()[
                    pf
                ]
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | cout_0 >> cin_0 >> c0 >> p0
    | cout_1 >> cin_1 >> c1 >> p1
    | cout_2 >> cin_2 >> c2
    | p0 >> PREPARED >> pa
    | p0 >> pb
    | p1 >> PREPARED >> pb
    | p1 >> pa
    | pa >> PREPARED >> pf
    | pb >> PREPARED >> pf
    | c2 >> pf
    | pf >> s;

    ExpectPass<PreparedAnalysis>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu


