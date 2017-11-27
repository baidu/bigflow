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

#include "flume/planner/common/executor_dependency_analysis.h"

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

class ExecutorDependencyAnalysisTest : public PassTest {
protected:
};

TEST_F(ExecutorDependencyAnalysisTest, EmptyTask) {
    PlanDesc origin = Task();

    PlanDesc expected = Task();

    ExpectPass<ExecutorDependencyAnalysis>(origin, expected);
}

TEST_F(ExecutorDependencyAnalysisTest, ExecutorDag) {
    PlanDesc executor_0 = LogicalExecutor();
    PlanDesc executor_1 = LogicalExecutor();
    PlanDesc executor_2 = LogicalExecutor();
    PlanDesc executor_3 = LogicalExecutor();
    PlanDesc node_0 = LoadNode();
    PlanDesc node_1 = ProcessNode();
    PlanDesc node_2 = ProcessNode();
    PlanDesc node_3 = ProcessNode();

    PlanDesc origin = PlanRoot() [
        Task() [
            executor_0[
                node_0
            ],
            executor_1[
                node_1
            ],
            executor_2[
                node_2
            ],
            executor_3[
                node_3
            ]
        ]
    ]
    | node_0 >> node_1
    | node_0 >> node_2
    | node_1 >> node_3
    | node_2 >> node_3;

    PlanDesc expected = PlanRoot() [
        Task() [
            executor_0[
                node_0
            ]
                +MatchTag<SubExecutors>(IsEmpty())
                +MatchTag<LeadingExecutors>(IsEmpty())
                +MatchTag<UpstreamDispatchers>(IsEmpty()),
            executor_1[
                node_1
            ]
                +MatchTag<SubExecutors>(IsEmpty())
                +MatchTag<LeadingExecutors>(UnorderedElementsAre(
                    Property(&Unit::identity, executor_0.identity())
                ))
                +MatchTag<UpstreamDispatchers>(IsEmpty()),
            executor_2[
                node_2
            ]
                +MatchTag<SubExecutors>(IsEmpty())
                +MatchTag<LeadingExecutors>(UnorderedElementsAre(
                    Property(&Unit::identity, executor_0.identity())
                ))
                +MatchTag<UpstreamDispatchers>(IsEmpty()),
            executor_3[
                node_3
            ]
                +MatchTag<SubExecutors>(IsEmpty())
                +MatchTag<LeadingExecutors>(UnorderedElementsAre(
                    Property(&Unit::identity, executor_1.identity()),
                    Property(&Unit::identity, executor_2.identity())
                ))
                +MatchTag<UpstreamDispatchers>(IsEmpty())
        ]
            +MatchTag<SubExecutors>(UnorderedElementsAre(
                Property(&Unit::identity, executor_0.identity()),
                Property(&Unit::identity, executor_1.identity()),
                Property(&Unit::identity, executor_2.identity()),
                Property(&Unit::identity, executor_3.identity())
            ))
            +NoTag<LeadingExecutors>()
            +NoTag<UpstreamDispatchers>()
    ]
    | node_0 >> node_1
    | node_0 >> node_2
    | node_1 >> node_3
    | node_2 >> node_3;

    ExpectPass<ExecutorDependencyAnalysis>(origin, expected);
}

TEST_F(ExecutorDependencyAnalysisTest, NestedExecutor) {
    PlanDesc external_executor = ExternalExecutor();
    PlanDesc merge_shuffle_executor_0 = ShuffleExecutor(PbScope::GROUP);
    PlanDesc merge_shuffle_executor_1 = ShuffleExecutor(PbScope::GROUP);
    PlanDesc combine_shuffle_executor = ShuffleExecutor(PbScope::GROUP);
    PlanDesc processor_executor = LogicalExecutor();
    PlanDesc union_executor = LogicalExecutor();
    PlanDesc sinker_executor = LogicalExecutor();
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
            external_executor[
                input_channel_0,
                input_channel_1
            ],
            merge_shuffle_executor_0[
                merge_channel_0, merge_shuffle_executor_1[
                    merge_channel_0_,
                    processor_executor[
                        process_node
                    ]
                ],
                merge_channel_1, combine_shuffle_executor[
                    merge_channel_1_,
                    shuffle_node,
                    union_executor[
                        union_node
                    ]
                ]
            ],
            sinker_executor[
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
            external_executor[
                input_channel_0,
                input_channel_1
            ]
                +MatchTag<SubExecutors>(IsEmpty())
                +MatchTag<LeadingExecutors>(IsEmpty())
                +MatchTag<UpstreamDispatchers>(IsEmpty()),
            merge_shuffle_executor_0[
                merge_channel_0, merge_shuffle_executor_1[
                    merge_channel_0_,
                    processor_executor[
                        process_node
                    ]
                        +MatchTag<SubExecutors>(IsEmpty())
                        +MatchTag<LeadingExecutors>(IsEmpty())
                        +MatchTag<UpstreamDispatchers>(UnorderedElementsAre(
                            Property(&Unit::identity, merge_channel_0_.identity())
                        ))
                ],
                merge_channel_1, combine_shuffle_executor[
                    merge_channel_1_,
                    shuffle_node,
                    union_executor[
                        union_node
                    ]
                        +MatchTag<SubExecutors>(IsEmpty())
                        +MatchTag<LeadingExecutors>(IsEmpty())
                        +MatchTag<UpstreamDispatchers>(UnorderedElementsAre(
                            Property(&Unit::identity, merge_channel_1_.identity()),
                            Property(&Unit::identity, shuffle_node.identity())
                        ))
                ]
                    +MatchTag<SubExecutors>(UnorderedElementsAre(
                        Property(&Unit::identity, union_executor.identity())
                    ))
                    +MatchTag<LeadingExecutors>(UnorderedElementsAre(
                        Property(&Unit::identity, merge_shuffle_executor_1.identity())
                    ))
                    +MatchTag<UpstreamDispatchers>(UnorderedElementsAre(
                        Property(&Unit::identity, merge_channel_0.identity()),
                        Property(&Unit::identity, merge_channel_1.identity())
                    ))
            ]
                +MatchTag<SubExecutors>(UnorderedElementsAre(
                    Property(&Unit::identity, merge_shuffle_executor_1.identity()),
                    Property(&Unit::identity, combine_shuffle_executor.identity())
                ))
                +MatchTag<LeadingExecutors>(IsEmpty())
                +MatchTag<UpstreamDispatchers>(UnorderedElementsAre(
                    Property(&Unit::identity, input_channel_0.identity()),
                    Property(&Unit::identity, input_channel_1.identity())
                )),
            sinker_executor[
                sink_node
            ]
                +MatchTag<SubExecutors>(IsEmpty())
                +MatchTag<LeadingExecutors>(UnorderedElementsAre(
                    Property(&Unit::identity, merge_shuffle_executor_0.identity())
                ))
                +MatchTag<UpstreamDispatchers>(UnorderedElementsAre(
                    Property(&Unit::identity, input_channel_0.identity()),
                    Property(&Unit::identity, input_channel_1.identity())
                ))
        ]
            +MatchTag<SubExecutors>(UnorderedElementsAre(
                Property(&Unit::identity, external_executor.identity()),
                Property(&Unit::identity, merge_shuffle_executor_0.identity()),
                Property(&Unit::identity, sinker_executor.identity())
            ))
            +NoTag<LeadingExecutors>()
            +NoTag<UpstreamDispatchers>()
    ]
    | output_channel_0 >> input_channel_0 >> merge_channel_0
    | output_channel_1 >> input_channel_1 >> merge_channel_1
    | merge_channel_0 >> merge_channel_0_ >> process_node >> shuffle_node >> union_node
    | merge_channel_1 >> merge_channel_1_ >> union_node
    | union_node >> sink_node;

    ExpectPass<ExecutorDependencyAnalysis>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
