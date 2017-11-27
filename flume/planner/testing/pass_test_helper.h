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
// Author: Pan Yuchang(BDG)<bigflow-opensource@baidu.com>
//         Wen Xiang <bigflow-opensource@baidu.com>

#ifndef FLUME_PLANNER_TESTING_PASS_TEST_HELPER_H_
#define FLUME_PLANNER_TESTING_PASS_TEST_HELPER_H_

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/edge_desc.h"
#include "flume/planner/testing/plan_desc.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

class PassTest : public ::testing::Test {
public:
    PassTest();
    virtual void SetUp();

    template<typename PassType>
    void ValidatePass();

    template<typename PassType>
    void UnregisterApplyPass();

    template<typename PassType>
    void ExpectPass(const PlanDesc& origin, const PlanDesc& expected);

    template<typename PassType>
    void ExpectPass(PassType *pass, const PlanDesc& origin, const PlanDesc& expected);

    template<typename PassTypeFirst, typename PassTypeSecond>
    void ExpectTwoPass(const PlanDesc& origin, const PlanDesc& expected);

    void Draw(Plan* plan);
    void Draw(const std::string& name, Plan* plan);

protected:
    // basic methods for creating plan units
    static PlanDesc ControlUnit();
    static PlanDesc ControlUnit(const std::string& identity);
    static PlanDesc LeafUnit();
    static PlanDesc LeafUnit(const std::string& identity);

    // for macro units
    static PlanDesc PlanRoot();
    static PlanDesc Job();
    static PlanDesc Task();

    // for scopes
    static PlanDesc DefaultScope();
    static PlanDesc InputScope();
    static PlanDesc GroupScope();
    static PlanDesc BucketScope();
    static PlanDesc BucketScope(const std::string& identity);

    // for leaf units
    static PlanDesc DummyUnit();
    static PlanDesc DummyUnit(const std::string& identity);
    static PlanDesc ChannelUnit();
    static PlanDesc ChannelUnit(const std::string& identity);

    static PlanDesc EmptyGroupFixer();
    static PlanDesc EmptyGroupFixer(const std::string& identity);

    // for logical plan nodes
    static PlanDesc ShuffleNode();
    static PlanDesc ShuffleNode(const std::string& identity);
    static PlanDesc ShuffleNode(PbShuffleNode::Type tyep);
    static PlanDesc UnionNode();
    static PlanDesc UnionNode(const std::string& identity);
    static PlanDesc ProcessNode();
    static PlanDesc ProcessNode(const std::string& identity);
    static PlanDesc LoadNode();
    static PlanDesc SinkNode();

    // for executors
    static PlanDesc ExternalExecutor();
    static PlanDesc ShuffleExecutor(PbScope::Type type);
    static PlanDesc LogicalExecutor();
    static PlanDesc PartialExecutor();
    static PlanDesc CacheWriterExecutor();

    static PlanDesc StreamExternalExecutor();
    static PlanDesc StreamShuffleExecutor(PbScope::Type type);
    static PlanDesc StreamLogicalExecutor();

    // common tags
    static TagDescRef ExecutedByFather();
    static TagDescRef IsInfinite();
    static TagDescRef IsPartial();
    static TagDescRef ShouldCache();

    // edge descs
    const EdgeDescRef PARTIAL;
    const EdgeDescRef PREPARED;

private:
    void DumpDebugFigure(const std::string& figure);

    std::string m_dir;
    std::string m_filename;
    int m_dot_sequence;
    DrawPlanPass m_drawer;

    Plan m_plan;
    PassManager m_pass_manager;

    runtime::Session m_session;
};


template<typename PassType>
void PassTest::ValidatePass() {
    m_pass_manager.Validate<PassType>();
}

template<typename PassType>
void PassTest::UnregisterApplyPass() {
    m_pass_manager.UnregisterApplyPass<PassType>();
}

template<typename PassType>
void PassTest::ExpectPass(const PlanDesc& origin, const PlanDesc& expected) {
    origin.to_plan(&m_plan);
    Draw(&m_plan);

    m_pass_manager.Apply<PassType>();
    Draw("result", &m_plan);

    EXPECT_TRUE(expected.is_plan(&m_plan));
    if (HasFailure()) {
        Plan plan;
        expected.to_plan(&plan);
        Draw("expected", &plan);
    }
}

template<typename PassTypeFirst, typename PassTypeSecond>
void PassTest::ExpectTwoPass(const PlanDesc& origin, const PlanDesc& expected) {
    origin.to_plan(&m_plan);
    Draw(&m_plan);

    m_pass_manager.Apply<PassTypeFirst>();
    m_pass_manager.Apply<PassTypeSecond>();
    Draw("result", &m_plan);

    EXPECT_TRUE(expected.is_plan(&m_plan));
    if (HasFailure()) {
        Plan plan;
        expected.to_plan(&plan);
        Draw("expected", &plan);
    }
}

template<typename PassType>
void PassTest::ExpectPass(PassType *pass, const PlanDesc& origin, const PlanDesc& expected) {
    origin.to_plan(&m_plan);
    Draw(&m_plan);

    m_pass_manager.RegisterPass(pass);
    m_pass_manager.Apply<PassType>();
    Draw("result", &m_plan);

    EXPECT_TRUE(expected.is_plan(&m_plan));
    if (HasFailure()) {
        Plan plan;
        expected.to_plan(&plan);
        Draw("expected", &plan);
    }
}


}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_TESTING_PASS_TEST_HELPER_H_
