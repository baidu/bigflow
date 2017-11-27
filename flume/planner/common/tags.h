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
//
// Common tags shared by all passes

#ifndef FLUME_PLANNER_COMMON_TAGS_H_
#define FLUME_PLANNER_COMMON_TAGS_H_

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "flume/planner/common/util.h"
#include "flume/planner/unit.h"

#include "flume/proto/config.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/session.h"

namespace baidu {
namespace flume {
namespace planner {

enum OptimizingStage {
    LOGICAL_OPTIMIZING = 1,
    TOPOLOGICAL_OPTIMIZING = 1 << 1,
    RUNTIME_OPTIMIZING = 1 << 2,
    TRANSLATION_OPTIMIZING = 1 << 3
};

template<typename T>
class TaskSingleton : public Pointer<Unit> {};

struct HasBroadcastShuffleNode {};

// indicate this unit is already 'claimed' by its father, so no extra unit is needed to
// execute this unit.
struct ExecutedByFather {};

struct IsInfinite {};

struct IsPartial {};

struct IsLocalDistribute {};

struct IsDistributeAsBatch {};

struct MustKeep {};

// Origin/Copies is a pair of tags for unit copying
struct Origin : public Pointer<Unit> {};  // where this unit is copied from
struct Copies : public std::map<Unit*, Unit*> {};  // copies of this unit in each task

// for channel unit
struct OriginIdentity : Value<std::string> {};

// for process node
struct PartialKeyNumber : public Value<int> {};

// for flume/planner/common/executor_dependency_analysis.h
struct SubExecutors : public std::set<Unit*> {};
struct LeadingExecutors : public std::set<Unit*> {};
struct UpstreamDispatchers : public std::set<Unit*> {};

// for flume/planner/common/prepared_analysis.h
struct DisablePriority {};  // a hint catched by PreparedAnalysis
struct PreparedNeeds : public std::set<Unit*> {};
struct NonPreparedNeeds : public std::set<Unit*> {};
struct PreparePriority : public Value<uint32_t> {};
struct NeedPrepare {};

// set by flume/planner/common/scope_analysis.h
struct HasPartitioner {};  // indicate this scope has partitioner
struct KeyScopes : public std::vector<PbScope> {};

struct ShouldCache {};

struct JobConfig : public Pointer<PbJobConfig> {};

struct Session : public Pointer<runtime::Session> {};
struct NewSession : public Pointer<runtime::Session> {};

struct HasSideEffect {}; // indicate this node has side effect

struct ShouldKeep {}; //  indicate this should not be removed in any pass

struct CacheKeyNumber {
    int value;
};

struct ScopeLevel { int level; };

struct LoadCacheChannel {};
struct GroupGenerator {};

struct IsBroadcast {};

struct Environment : public Pointer<PbEntity> {};

struct Debug{}; // indicate this pass should show debug info (like draw dots)

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_COMMON_TAGS_H_

