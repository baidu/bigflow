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

#include "flume/planner/common/build_common_executor_pass.h"

#include "boost/foreach.hpp"
#include "boost/range/algorithm.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/unit.h"
#include "flume/proto/transfer_encoding.pb.h"

namespace baidu {
namespace flume {
namespace planner {

using ::testing::_;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Property;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class BuildCommonExecutorPassTest : public PassTest {
protected:
    class StreamShuffleExecutorMessageImpl : public TagDesc {
    public:
        explicit StreamShuffleExecutorMessageImpl(PbStreamShuffleExecutor::Type type) :
                _type(type) {}

        virtual void Set(Unit* unit) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* unit) {
            if (!unit->has<PbExecutor>()) {
                return false;
            }
            const PbExecutor& message = unit->get<PbExecutor>();

            if (message.type() != PbExecutor::STREAM_SHUFFLE
                    || !message.has_stream_shuffle_executor()) {
                return false;
            }
            const PbStreamShuffleExecutor& sub_message = message.stream_shuffle_executor();

            if (!unit->has<PbScope>() || sub_message.scope().id() != unit->get<PbScope>().id()) {
                return false;
            }

            return true;
        }

    private:
        PbStreamShuffleExecutor::Type _type;
    };

    TagDescRef StreamShuffleExecutorMessage(PbStreamShuffleExecutor::Type type) {
        return TagDescRef(new StreamShuffleExecutorMessageImpl(type));
    }

    class StreamLogicalExecutorMessageImpl : public TagDesc {
    public:
        virtual void Set(Unit* unit) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* unit) {
            if (unit->children().size() != 1) {
                LOG(ERROR) << "1";
                return false;
            }
            Unit* child = unit->children().front();

            if (!unit->has<PbExecutor>()) {
                LOG(ERROR) << "2";
                return false;
            }
            const PbExecutor& message = unit->get<PbExecutor>();

            if (message.type() != PbExecutor::STREAM_LOGICAL) {
                LOG(ERROR) << "3";
                return false;
            }
            const PbStreamLogicalExecutor& sub_message = message.stream_logical_executor();

            if (sub_message.node().id() != child->identity()) {
                LOG(ERROR) << "4";
                return false;
            }

            return true;
        }
    };

    TagDescRef StreamLogicalExecutorMessage() {
        return TagDescRef(new StreamLogicalExecutorMessageImpl());
    }

    class StreamProcessorExecutorMessageImpl : public TagDesc {
    public:
        explicit StreamProcessorExecutorMessageImpl() {}

        virtual void Set(Unit* unit) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* unit) {
            if (unit->children().size() != 1) {
                return false;
            }
            Unit* child = unit->children().front();

            if (!unit->has<PbExecutor>()) {
                return false;
            }
            const PbExecutor& message = unit->get<PbExecutor>();

            if (message.type() != PbExecutor::STREAM_PROCESSOR) {
                return false;
            }

            const PbStreamProcessorExecutor& sub_message = message.stream_processor_executor();
            if (sub_message.processor().name() !=
                    child->get<PbLogicalPlanNode>().process_node().processor().name()) {
                return false;
            }

            return sub_message.identity() == child->identity();
        }
    };

    TagDescRef StreamProcessorExecutorMessage() {
        return TagDescRef(new StreamProcessorExecutorMessageImpl());
    }

    class ShuffleExecutorMessageImpl : public TagDesc {
    public:
        explicit ShuffleExecutorMessageImpl(PbShuffleExecutor::Type type) : m_type(type) {}

        virtual void Set(Unit* unit) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* unit) {
            if (!unit->has<PbExecutor>()) {
                return false;
            }
            const PbExecutor& message = unit->get<PbExecutor>();

            if (message.type() != PbExecutor::SHUFFLE || !message.has_shuffle_executor()) {
                return false;
            }
            const PbShuffleExecutor& sub_message = message.shuffle_executor();

            if (!unit->has<PbScope>() || sub_message.scope().id() != unit->get<PbScope>().id()) {
                return false;
            }

            return true;
        }

    private:
        PbShuffleExecutor::Type m_type;
    };

    TagDescRef ShuffleExecutorMessage(PbShuffleExecutor::Type type) {
        return TagDescRef(new ShuffleExecutorMessageImpl(type));
    }

    class CacheWriterExecutorMessageImpl : public TagDesc {
    public:
        virtual void Set(Unit* unit) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* unit) {
            if (unit->children().size() != 1) {
                return false;
            }
            Unit* child = unit->children().front();

            if (!unit->has<PbExecutor>()) {
                return false;
            }
            const PbExecutor& message = unit->get<PbExecutor>();

            if (message.type() != PbExecutor::WRITE_CACHE) {
                return false;
            }
            const PbWriteCacheExecutor& sub_message = message.write_cache_executor();

            if (child->direct_needs().size() != 1) {
                return false;
            }

            if (sub_message.from() != child->direct_needs().front()->identity()) {
                return false;
            }

            // TODO(wangcong09) check path
            return true;
        }
    };

    TagDescRef CacheWriterExecutorMessage() {
        return TagDescRef(new CacheWriterExecutorMessageImpl());
    }

    class LogicalExecutorMessageImpl : public TagDesc {
    public:
        virtual void Set(Unit* unit) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* unit) {
            if (unit->children().size() != 1) {
                return false;
            }
            Unit* child = unit->children().front();

            if (!unit->has<PbExecutor>()) {
                return false;
            }
            const PbExecutor& message = unit->get<PbExecutor>();

            if (message.type() != PbExecutor::LOGICAL) {
                return false;
            }
            const PbLogicalExecutor& sub_message = message.logical_executor();

            if (sub_message.node().id() != child->identity()) {
                return false;
            }

            return true;
        }
    };

    TagDescRef LogicalExecutorMessage() {
        return TagDescRef(new LogicalExecutorMessageImpl());
    }

    class DummyFromImpl : public TagDesc {
    public:
        explicit DummyFromImpl(const std::string& from) : m_from(from) {}

        virtual void Set(Unit* unit) {
            unit->get<PbLogicalPlanNode>().mutable_process_node()->add_input()->set_from(m_from);
        }

        virtual bool Test(Unit* unit) {
            return true;
        }

    private:
        std::string m_from;
    };

    TagDescRef DummyFrom(const std::string& from) {
        return TagDescRef(new DummyFromImpl(from));
    }


    class ProcessorExecutorMessageImpl : public TagDesc {
    public:
        explicit ProcessorExecutorMessageImpl() {}

        virtual void Set(Unit* unit) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* unit) {
            if (unit->children().size() != 1) {
                return false;
            }
            Unit* child = unit->children().front();

            if (!unit->has<PbExecutor>()) {
                return false;
            }
            const PbExecutor& message = unit->get<PbExecutor>();

            if (message.type() != PbExecutor::PROCESSOR) {
                return false;
            }

            const PbProcessorExecutor& sub_message = message.processor_executor();
            if (sub_message.processor().name() !=
                    child->get<PbLogicalPlanNode>().process_node().processor().name()) {
                return false;
            }

            return sub_message.identity() == child->identity();
        }
    };

    TagDescRef ProcessorExecutorMessage() {
        return TagDescRef(new ProcessorExecutorMessageImpl());
    }

    class StreamShuffleEdge : public EdgeDesc {
    public:
        virtual void Set(Unit* from, Unit* to) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* from, Unit* to) {
            if (to->type() != Unit::SHUFFLE_NODE
                    || to->father()->type() != Unit::STREAM_SHUFFLE_EXECUTOR) {
                return false;
            }

            if (to->direct_needs().size() != 1 && to->direct_needs().front() == from) {
                return false;
            }

            const PbStreamShuffleExecutor& message =
                    to->father()->get<PbExecutor>().stream_shuffle_executor();
            for (int i = 0; i < message.node_size(); ++i) {
                const PbLogicalPlanNode& node = message.node(i);
                if (node.id() == to->identity()
                        && node.shuffle_node().from() == from->identity()) {
                    return true;
                }
            }

            return false;
        }
    };

    class ShuffleEdge : public EdgeDesc {
    public:
        virtual void Set(Unit* from, Unit* to) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* from, Unit* to) {
            if (to->type() != Unit::SHUFFLE_NODE
                    || to->father()->type() != Unit::SHUFFLE_EXECUTOR) {
                return false;
            }

            if (to->direct_needs().size() != 1 && to->direct_needs().front() == from) {
                return false;
            }

            const PbShuffleExecutor& message = to->father()->get<PbExecutor>().shuffle_executor();
            for (int i = 0; i < message.node_size(); ++i) {
                const PbLogicalPlanNode& node = message.node(i);
                if (node.id() == to->identity()
                        && node.shuffle_node().from() == from->identity()) {
                    return true;
                }
            }

            return false;
        }
    };

    typedef PbShuffleExecutor::MergeSource MergeSource;

    class MergeEdge : public EdgeDesc {
    public:
        explicit MergeEdge() {}

        virtual void Set(Unit* from, Unit* to) {
            LOG(ERROR) << "not implemented!";
        }

        virtual bool Test(Unit* from, Unit* to) {
            if (to->type() != Unit::CHANNEL || to->father()->type() != Unit::SHUFFLE_EXECUTOR) {
                return false;
            }

            if (to->direct_needs().size() != 1 && to->direct_needs().front() == from) {
                return false;
            }

            const PbShuffleExecutor& message = to->father()->get<PbExecutor>().shuffle_executor();
            for (int i = 0; i < message.source_size(); ++i) {
                const PbShuffleExecutor::MergeSource& source = message.source(i);
                if (source.output() == to->identity() && source.input() == from->identity()) {
                    return true;
                }
            }

            return false;
        }
    };

    static const EdgeDescRef STREAM_SHUFFLE;
    static const EdgeDescRef SHUFFLE;
    static const EdgeDescRef MERGE;
};

const EdgeDescRef
BuildCommonExecutorPassTest::STREAM_SHUFFLE(new StreamShuffleEdge());

const EdgeDescRef
BuildCommonExecutorPassTest::SHUFFLE(new ShuffleEdge());

const EdgeDescRef
BuildCommonExecutorPassTest::MERGE(new MergeEdge());

TEST_F(BuildCommonExecutorPassTest, LocalStreamShuffleExecutor) {
    PlanDesc c = ChannelUnit();
    PlanDesc sn = ShuffleNode();
    PlanDesc p = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            c,
            StreamShuffleExecutor(PbScope::GROUP)[
                sn,
                LogicalExecutor()[
                    p
                ]
            ]
        ]
    ]
    | c >> sn >> p;

    PlanDesc expected = Job()[
        Task()[
            c,
            StreamShuffleExecutor(PbScope::GROUP)[
                sn,
                LogicalExecutor()[
                    p
                ]
            ] +StreamShuffleExecutorMessage(PbStreamShuffleExecutor::LOCAL)
        ]
    ]
    | c >> STREAM_SHUFFLE >> sn >> p;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, LocalDistributeStreamShuffleExecutor) {
    PlanDesc p0 = ProcessNode();
    PlanDesc sn = ShuffleNode();

    PlanDesc origin = Job()[
        Task()[
            p0,
            StreamShuffleExecutor(PbScope::BUCKET)[
                sn
            ] +NewTagDesc<IsLocalDistribute>()
        ]
    ]
    | p0 >> sn;

    PlanDesc expected = Job()[
        Task()[
            p0,
            StreamShuffleExecutor(PbScope::BUCKET)[
                sn
            ] +StreamShuffleExecutorMessage(PbStreamShuffleExecutor::LOCAL_DISTRIBUTE)
        ]
    ]
    | p0 >> STREAM_SHUFFLE >> sn;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, DistributeAsBatchStreamShuffleExecutor) {
    PlanDesc p0 = ProcessNode();
    PlanDesc sn = ShuffleNode();

    PlanDesc origin = Job()[
        Task()[
            p0,
            StreamShuffleExecutor(PbScope::BUCKET)[
                sn
            ] +NewTagDesc<IsDistributeAsBatch>()
        ]
    ]
    | p0 >> sn;

    PlanDesc expected = Job()[
        Task()[
            p0,
            StreamShuffleExecutor(PbScope::BUCKET)[
                sn
            ] +StreamShuffleExecutorMessage(PbStreamShuffleExecutor::DISTRIBUTE_AS_BATCH)
        ]
    ]
    | p0 >> STREAM_SHUFFLE >> sn;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, LocalShuffleExecutor) {
    PlanDesc c = ChannelUnit();
    PlanDesc sn = ShuffleNode();
    PlanDesc p = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            c,
            ShuffleExecutor(PbScope::GROUP)[
                sn,
                LogicalExecutor()[
                    p
                ]
            ]
        ]
    ]
    | c >> sn >> p;

    PlanDesc expected = Job()[
        Task()[
            c,
            ShuffleExecutor(PbScope::GROUP)[
                sn,
                LogicalExecutor()[
                    p
                ]
            ] +ShuffleExecutorMessage(PbShuffleExecutor::LOCAL)
        ]
    ]
    | c >> SHUFFLE >> sn >> p;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, MergeShuffleExecutor) {
    PlanDesc c0_0 = ChannelUnit();
    PlanDesc c0_1 = ChannelUnit();
    PlanDesc c0_2 = ChannelUnit();
    PlanDesc c1_0 = ChannelUnit();
    PlanDesc c1_1 = ChannelUnit();
    PlanDesc sn = ShuffleNode();
    PlanDesc p = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            c0_0,
            c1_0,
            ShuffleExecutor(PbScope::GROUP)[
                c0_1,
                ShuffleExecutor(PbScope::GROUP)[
                    c0_2,
                    LogicalExecutor()[
                        p
                    ]
                ],
                c1_1,
                ShuffleExecutor(PbScope::GROUP)[
                    sn
                ]
            ]
        ]
    ]
    | c0_0 >> c0_1 >> c0_2 >> p
    | c1_0 >> c1_1 >> sn;

    PlanDesc expected = Job()[
        Task()[
            c0_0 +NoTag<PbTransferEncoder>(),
            c1_0 +NoTag<PbTransferEncoder>(),
            ShuffleExecutor(PbScope::GROUP)[
                c0_1,
                ShuffleExecutor(PbScope::GROUP)[
                    c0_2,
                    LogicalExecutor()[
                        p
                    ]
                ] +ShuffleExecutorMessage(PbShuffleExecutor::MERGE),
                c1_1,
                ShuffleExecutor(PbScope::GROUP)[
                    sn
                ] +ShuffleExecutorMessage(PbShuffleExecutor::LOCAL)
            ] +ShuffleExecutorMessage(PbShuffleExecutor::COMBINE)
        ]
    ]
    | c0_0 >> MERGE >> c0_1 >> MERGE >> c0_2 >> p
    | c1_0 >> MERGE >> c1_1 >> SHUFFLE >> sn;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, LocalDistributeExecutor) {
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc sn = ShuffleNode();
    PlanDesc c = ChannelUnit();

    PlanDesc origin = Job()[
        Task()[
            p0, p1,
            ShuffleExecutor(PbScope::BUCKET)[
                sn,
                c +PbNodeTag(PbLogicalPlanNode::UNION_NODE)
            ] +NewTagDesc<IsLocalDistribute>()
        ]
    ]
    | p0 >> sn
    | p1 >> c;

    typedef PbShuffleExecutor::MergeSource MergeSource;

    PlanDesc expected = Job()[
        Task()[
            p0, p1,
            ShuffleExecutor(PbScope::BUCKET)[
                sn,
                c
            ] +ShuffleExecutorMessage(PbShuffleExecutor::LOCAL_DISTRIBUTE)
        ]
    ]
    | p0 >> SHUFFLE >> sn
    | p1 >> MERGE >> c;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, StreamLogicalExecutor) {
    PlanDesc l = LoadNode();
    PlanDesc u = UnionNode();
    PlanDesc p = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            StreamLogicalExecutor()[
                l
            ],
            StreamLogicalExecutor()[
                u
            ],
            StreamLogicalExecutor()[
                p
            ],
            StreamLogicalExecutor()[
                s
            ]
        ]
    ]
    | l >> u >> p >> s;

    PlanDesc expected = Job()[
        Task()[
            StreamLogicalExecutor()[
                l
            ]
            +StreamLogicalExecutorMessage(),
            StreamLogicalExecutor()[
                u
            ]
            +StreamLogicalExecutorMessage(),
            StreamLogicalExecutor()[
                p
            ]
            +StreamProcessorExecutorMessage(),
            StreamLogicalExecutor()[
                s
            ]
            +StreamLogicalExecutorMessage()
        ]
    ]
    | l >> u >> p >> s;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, LogicalExecutor) {
    PlanDesc l = LoadNode();
    PlanDesc u = UnionNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc p3 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            LogicalExecutor()[
                l
            ],
            LogicalExecutor()[
                u
            ],
            LogicalExecutor()[
                p1
            ],
            LogicalExecutor()[
                p2 +DummyFrom("dummy-from")
            ],
            LogicalExecutor()[
                p3 +NewValueTag< ::baidu::flume::planner::PartialKeyNumber >(1)
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l >>  u >> PREPARED >> p1 >> p2 >> p3 >> s;

    typedef PbProcessorExecutor::Source Source;

    PlanDesc expected = Job()[
        Task()[
            LogicalExecutor()[
                l
            ]
            +LogicalExecutorMessage(),
            LogicalExecutor()[
                u
            ]
            +LogicalExecutorMessage(),
            LogicalExecutor()[
                p1
            ]
            +ProcessorExecutorMessage() + MatchTag<PbExecutor>(
                Property(&PbExecutor::processor_executor, AllOf(
                    Property(&PbProcessorExecutor::source, ElementsAre(
                        AllOf(
                            Property(&Source::identity, u.identity()),
                            Property(&Source::type, PbProcessorExecutor::REQUIRE_ITERATOR),
                            Property(&Source::is_prepared, true)
                        )
                    )),
                    Property(&PbProcessorExecutor::has_partial_key_number, false)
                ))
            ),
            LogicalExecutor()[
                p2
            ]
            +ProcessorExecutorMessage() + MatchTag<PbExecutor>(
                Property(&PbExecutor::processor_executor, AllOf(
                    Property(&PbProcessorExecutor::source, ElementsAre(
                        AllOf(
                            Property(&Source::identity, p1.identity()),
                            Property(&Source::type, PbProcessorExecutor::REQUIRE_STREAM),
                            Property(&Source::is_prepared, false)
                        ),
                        AllOf(
                            Property(&Source::identity, "dummy-from"),
                            Property(&Source::type, PbProcessorExecutor::DUMMY),
                            Property(&Source::is_prepared, false)
                        )
                    )),
                    Property(&PbProcessorExecutor::has_partial_key_number, false)
                ))
            ),
            LogicalExecutor()[
                p3
            ]
            +ProcessorExecutorMessage() + MatchTag<PbExecutor>(
                Property(&PbExecutor::processor_executor, AllOf(
                    Property(&PbProcessorExecutor::source, ElementsAre(
                        AllOf(
                            Property(&Source::identity, p2.identity()),
                            Property(&Source::type, PbProcessorExecutor::REQUIRE_STREAM),
                            Property(&Source::is_prepared, false)
                        )
                    )),
                    Property(&PbProcessorExecutor::has_partial_key_number, true),
                    Property(&PbProcessorExecutor::has_partial_key_number, 1)
                ))
            ),
            LogicalExecutor()[
                s
            ]
            +LogicalExecutorMessage()
        ]
    ]
    | l >> u >> PREPARED >> p1 >> p2 >> p3 >> s;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, PartialExecutor) {
    PlanDesc sn0 = ShuffleNode();
    PlanDesc sn1 = ShuffleNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc c0 = ChannelUnit();
    PlanDesc c1 = ChannelUnit();
    PlanDesc ce = ChannelUnit();

    PlanDesc origin = Job()[
        Task()[
            PartialExecutor()[
                p0 +NewTagDesc<DisablePriority>() + ScopeLevelTag(0),
                sn0 +PbScopeTag(PbScope::GROUP) + ScopeLevelTag(1),
                sn1 +PbScopeTag(PbScope::BUCKET) + ScopeLevelTag(2),
                ShuffleExecutor(PbScope::GROUP)[
                    c0,
                    ShuffleExecutor(PbScope::BUCKET)[
                        c1 +IsPartial()
                    ],
                    LogicalExecutor()[
                        p1
                    ]
                ],
                ExternalExecutor()[
                    ce
                ]
            ]
        ]
    ]
    | p0 >> sn0 >> sn1 >> c0 >> c1 >> p1 >> ce
    | p0 >> ce;

    PlanDesc expected = Job()[
        Task()[
            PartialExecutor()[
                p0,
                sn0,
                sn1,
                ShuffleExecutor(PbScope::GROUP) [
                    c0,
                    ShuffleExecutor(PbScope::BUCKET) [
                        c1
                    ],
                    LogicalExecutor()[
                        p1
                    ]
                ],
                ExternalExecutor()[
                    ce
                ]
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::type, PbExecutor::PARTIAL),
                Property(&PbExecutor::has_partial_executor, true),
                Property(&PbExecutor::partial_executor, AllOf(
                        Property(&PbPartialExecutor::scope, UnorderedElementsAre(
                            Property(&PbScope::id, sn0.identity()),
                            Property(&PbScope::id, sn1.identity())
                        )),
                        Property(&PbPartialExecutor::node, UnorderedElementsAre(
                            Property(&PbLogicalPlanNode::id, p0.identity()),
                            Property(&PbLogicalPlanNode::id, sn0.identity()),
                            Property(&PbLogicalPlanNode::id, sn1.identity())
                        )),
                        Property(&PbPartialExecutor::output, UnorderedElementsAre(
                                AllOf(
                                    Property(&PbPartialExecutor::Output::identity, p0.identity()),
                                    Property(&PbPartialExecutor::Output::has_objector, true),
                                    Property(&PbPartialExecutor::Output::priority_size, 0),
                                    Property(&PbPartialExecutor::Output::need_hash, false),
                                    Property(&PbPartialExecutor::Output::need_buffer, false)
                                ),
                                AllOf(
                                    Property(&PbPartialExecutor::Output::identity, sn1.identity()),
                                    Property(&PbPartialExecutor::Output::has_objector, true),
                                    Property(&PbPartialExecutor::Output::priority,
                                             ElementsAre(0, 0, 0)),
                                    Property(&PbPartialExecutor::Output::need_hash, false),
                                    Property(&PbPartialExecutor::Output::need_buffer, true)
                                )
                        ))
                ))
            ))
        ]
    ]
    | p0 >> sn0 >> sn1 >> c0 >> c1 >> p1 >> ce
    | p0 >> ce;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

TEST_F(BuildCommonExecutorPassTest, CacheWriterExecutor) {
    PlanDesc p1 = ProcessNode();
    PlanDesc c_p1 = ChannelUnit();
    PlanDesc sn = ShuffleNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc c_p2 = ChannelUnit();

    PbJobConfig message;
    message.set_tmp_data_path_output("/nowhere");
    JobConfig job_config;
    job_config.Assign(&message);

    PlanDesc origin = (Job()[
        Task()[
            LogicalExecutor()[
                p1
            ],
            CacheWriterExecutor()[
                c_p1
            ],
            ShuffleExecutor(PbScope::GROUP)[
                sn,
                LogicalExecutor()[
                    p2
                ],
                CacheWriterExecutor()[
                    c_p2
                ]
            ]
        ]
    ] +NewTagDesc<JobConfig>(job_config))
        | p1 >> c_p1
        | p1 >> sn >> p2 >> c_p2;

    PlanDesc expected = (Job()[
        Task()[
            LogicalExecutor()[
                p1
            ],
            CacheWriterExecutor()[
                c_p1
            ] +CacheWriterExecutorMessage()  // TODO(wangcong09) check path
            ,
            ShuffleExecutor(PbScope::GROUP)[
                sn,
                LogicalExecutor()[
                    p2
                ],
                CacheWriterExecutor()[
                    c_p2
                ] +CacheWriterExecutorMessage()
            ]
        ]
    ] +NewTagDesc<JobConfig>(job_config))
    | p1 >> c_p1
    | p1 >> sn >> p2 >> c_p2;

    ExpectPass<BuildCommonExecutorPass>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
