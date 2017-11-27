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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/testing/pass_test_helper.h"

#include <fstream>  // NOLINT(readability/streams)

#include "boost/algorithm/string.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/make_shared.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/shared_ptr.hpp"

#include "toft/base/closure.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/common/tags.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/partial_executor.h"

namespace baidu {
namespace flume {
namespace planner {

PassTest::PassTest() : PARTIAL(PartialEdge()), PREPARED(PreparedEdge()),
    m_dot_sequence(0), m_pass_manager(&m_plan) {}

void PassTest::SetUp() {
    m_dir = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    system(("mkdir -p " + m_dir).c_str());

    m_drawer.RegisterListener(toft::NewPermanentClosure(this, &PassTest::DumpDebugFigure));
    m_pass_manager.SetDebugPass(&m_drawer);
}

void PassTest::DumpDebugFigure(const std::string& figure) {
    std::string path;
    if (m_filename.empty()) {
        path = m_dir + "/" + boost::lexical_cast<std::string>(m_dot_sequence++) + ".dot";
    } else {
        path = m_dir + "/" + m_filename + ".dot";
    }
    std::ofstream stream(path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
    stream.write(figure.data(), figure.size());
}

void PassTest::Draw(Plan* plan) {
    Draw("", plan);
}

void PassTest::Draw(const std::string& name, Plan* plan) {
    std::string old = m_filename;

    m_filename = name;
    m_drawer.Run(plan);
    m_filename = old;
}

PlanDesc PassTest::ControlUnit() {
    return PlanDesc(/*is_leaf =*/ false);
}

PlanDesc PassTest::ControlUnit(const std::string& identity) {
    return PlanDesc(/*is_leaf =*/ false, identity);
}

PlanDesc PassTest::LeafUnit() {
    return PlanDesc(/*is_leaf =*/ true);
}

PlanDesc PassTest::LeafUnit(const std::string& identity) {
    return PlanDesc(/*is_leaf =*/ true, identity);
}

PlanDesc PassTest::PlanRoot() {
    return ControlUnit() + UnitType(Unit::PLAN);
}

PlanDesc PassTest::Job() {
    return ControlUnit() + UnitType(Unit::JOB);
}

PlanDesc PassTest::Task() {
    return ControlUnit() + UnitType(Unit::TASK) + PbScopeTag(PbScope::DEFAULT);
}

PlanDesc PassTest::DefaultScope() {
    return ControlUnit() + UnitType(Unit::SCOPE) + PbScopeTag(PbScope::DEFAULT);
}

PlanDesc PassTest::InputScope() {
    return ControlUnit() + UnitType(Unit::SCOPE) + PbScopeTag(PbScope::INPUT);
}

PlanDesc PassTest::GroupScope() {
    return ControlUnit() + UnitType(Unit::SCOPE) + PbScopeTag(PbScope::GROUP);
}

PlanDesc PassTest::BucketScope() {
    return ControlUnit() + UnitType(Unit::SCOPE) + PbScopeTag(PbScope::BUCKET);
}

PlanDesc PassTest::BucketScope(const std::string& identity) {
    return ControlUnit(identity) + UnitType(Unit::SCOPE) + PbScopeTag(PbScope::BUCKET);
}

PlanDesc PassTest::ExternalExecutor() {
    return ControlUnit() + UnitType(Unit::EXTERNAL_EXECUTOR);
}

PlanDesc PassTest::ShuffleExecutor(PbScope::Type type) {
    return ControlUnit() + UnitType(Unit::SHUFFLE_EXECUTOR) + PbScopeTag(type);
}

PlanDesc PassTest::LogicalExecutor() {
    return ControlUnit() + UnitType(Unit::LOGICAL_EXECUTOR);
}

PlanDesc PassTest::PartialExecutor() {
    return ControlUnit()
        + UnitType(Unit::PARTIAL_EXECUTOR)
        + NewTaskSingleton<runtime::PartialExecutor>();
}

PlanDesc PassTest::CacheWriterExecutor() {
    return ControlUnit() + UnitType(Unit::CACHE_WRITER);
}

PlanDesc PassTest::StreamExternalExecutor() {
    return ControlUnit() + UnitType(Unit::STREAM_EXTERNAL_EXECUTOR);
}

PlanDesc PassTest::StreamShuffleExecutor(PbScope::Type type) {
    return ControlUnit() + UnitType(Unit::STREAM_SHUFFLE_EXECUTOR) + PbScopeTag(type);
}

PlanDesc PassTest::StreamLogicalExecutor() {
    return ControlUnit() + UnitType(Unit::STREAM_LOGICAL_EXECUTOR);
}

PlanDesc PassTest::DummyUnit() {
    return LeafUnit() + UnitType(Unit::DUMMY);
}

PlanDesc PassTest::DummyUnit(const std::string& identity) {
    return LeafUnit(identity) + UnitType(Unit::DUMMY);
}

PlanDesc PassTest::ChannelUnit() {
    return LeafUnit() + UnitType(Unit::CHANNEL);
}

PlanDesc PassTest::ChannelUnit(const std::string& identity) {
    return LeafUnit(identity) + UnitType(Unit::CHANNEL);
}

PlanDesc PassTest::EmptyGroupFixer() {
    return LeafUnit() + UnitType(Unit::EMPTY_GROUP_FIXER);
}

PlanDesc PassTest::EmptyGroupFixer(const std::string& identity) {
    return LeafUnit(identity) + UnitType(Unit::EMPTY_GROUP_FIXER);
}

PlanDesc PassTest::ShuffleNode() {
    return LeafUnit() + UnitType(Unit::SHUFFLE_NODE) + PbNodeTag(PbLogicalPlanNode::SHUFFLE_NODE);
}

PlanDesc PassTest::ShuffleNode(const std::string& identity) {
    return LeafUnit(identity) + UnitType(Unit::SHUFFLE_NODE)
            + PbNodeTag(PbLogicalPlanNode::SHUFFLE_NODE);
}

PlanDesc PassTest::ShuffleNode(PbShuffleNode::Type type) {
    return ShuffleNode() + PbShuffleNodeTag(type);
}

PlanDesc PassTest::UnionNode() {
    return LeafUnit() + UnitType(Unit::UNION_NODE) + PbNodeTag(PbLogicalPlanNode::UNION_NODE);
}

PlanDesc PassTest::UnionNode(const std::string& identity) {
    return LeafUnit(identity) + UnitType(Unit::UNION_NODE)
                + PbNodeTag(PbLogicalPlanNode::UNION_NODE);
}

PlanDesc PassTest::ProcessNode() {
    return LeafUnit() + UnitType(Unit::PROCESS_NODE) + PbNodeTag(PbLogicalPlanNode::PROCESS_NODE);
}

PlanDesc PassTest::ProcessNode(const std::string& identity) {
    return LeafUnit(identity) + UnitType(Unit::PROCESS_NODE)
                + PbNodeTag(PbLogicalPlanNode::PROCESS_NODE);
}

PlanDesc PassTest::LoadNode() {
    return LeafUnit() + UnitType(Unit::LOAD_NODE) + PbNodeTag(PbLogicalPlanNode::LOAD_NODE);
}

PlanDesc PassTest::SinkNode() {
    return LeafUnit() + UnitType(Unit::SINK_NODE) + PbNodeTag(PbLogicalPlanNode::SINK_NODE);
}

TagDescRef PassTest::ExecutedByFather() {
    return NewTagDesc< ::baidu::flume::planner::ExecutedByFather >();
}

TagDescRef PassTest::IsInfinite() {
    return NewTagDesc< ::baidu::flume::planner::IsInfinite >();
}

TagDescRef PassTest::IsPartial() {
    return NewTagDesc< ::baidu::flume::planner::IsPartial >();
}

TagDescRef PassTest::ShouldCache() {
    return NewTagDesc< ::baidu::flume::planner::ShouldCache >();
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

