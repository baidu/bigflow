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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>

#include "flume/planner/common/group_generator_analysis.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "flume/planner/common/clean_tag_rule.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

struct HasLoadCacheChannel {};

class  ScopeHasLoadCacheChannelAnalysisRule: public RuleDispatcher::Rule {
public:

    virtual bool Accept(Plan* plan, Unit* unit) {
        if (unit->father() == NULL) {
            return false;
        }

        if (unit->father()->type() != Unit::SCOPE &&
            unit->father()->type() != Unit::SHUFFLE_EXECUTOR) {
            return false;
        }

        if (unit->type() != Unit::SHUFFLE_NODE && unit->type() != Unit::CHANNEL) {
            return false;
        }

        // TODO : BROADCAST is not group generator

        return unit->has<LoadCacheChannel>();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        unit->father()->set<HasLoadCacheChannel>();
        return true;
    }
};

class  GroupGeneratorAnalysisRule: public RuleDispatcher::Rule {
public:

    virtual bool Accept(Plan* plan, Unit* unit) {

        if (unit->type() != Unit::SHUFFLE_NODE && unit->type() != Unit::CHANNEL) {
            return false;
        }

        CHECK_NOTNULL(unit->father());
        if (unit->has<LoadCacheChannel>()) {
            return true;
        }

        return !unit->father()->has<HasLoadCacheChannel>();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        if (unit->has<GroupGenerator>()) {
            return false;
        }

        unit->get<GroupGenerator>(); // set flag
        return true;
    }
};

}  // namespace

bool GroupGeneratorAnalysis::Run(Plan* plan) {
    DepthFirstDispatcher has_load_cache_channel_analysis(DepthFirstDispatcher::PRE_ORDER);
    has_load_cache_channel_analysis.AddRule(new CleanTagRule<HasLoadCacheChannel>());
    has_load_cache_channel_analysis.AddRule(new ScopeHasLoadCacheChannelAnalysisRule());
    has_load_cache_channel_analysis.Run(plan);

    DepthFirstDispatcher group_generator_analysis(DepthFirstDispatcher::PRE_ORDER);
    group_generator_analysis.AddRule(new CleanTagRule<GroupGenerator>());
    group_generator_analysis.AddRule(new GroupGeneratorAnalysisRule());
    group_generator_analysis.Run(plan);
    return false;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
