/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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

#include "flume/planner/pass_manager.h"

#include <algorithm>
#include <deque>

#include "boost/foreach.hpp"
#include "boost/tuple/tuple.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/pass.h"
#include "flume/planner/plan.h"

DEFINE_int32(flume_planner_max_steps, 1000,
             "top limit of applied passes which are causing changes to plan.");

namespace baidu {
namespace flume {
namespace planner {

PassManager::PassMap* PassManager::s_depend_map = NULL;
PassManager::PassMap* PassManager::s_apply_map = NULL;
PassManager::PassMap* PassManager::s_invalidate_map = NULL;
PassManager::PassMap* PassManager::s_preserve_map = NULL;
PassManager::PassSet* PassManager::s_white_passes = NULL;
PassManager::PassSet* PassManager::s_stable_passes = NULL;
PassManager::PassSet* PassManager::s_recursive_passes = NULL;
std::map<std::string, int32_t>* PassManager::s_pass_stage = NULL;

void PassManager::InitPassRelations() {
    if (s_depend_map == NULL) {
        s_depend_map = new PassMap();
    }

    if (s_apply_map == NULL) {
        s_apply_map = new PassMap();
    }

    if (s_invalidate_map == NULL) {
        s_invalidate_map = new PassMap();
    }

    if (s_preserve_map == NULL) {
        s_preserve_map = new PassMap();
    }

    if (s_white_passes == NULL) {
        s_white_passes = new PassSet();
    }

    if (s_stable_passes == NULL) {
        s_stable_passes = new PassSet();
    }

    if (s_recursive_passes == NULL) {
        s_recursive_passes = new PassSet();
    }

    if (s_pass_stage == NULL) {
        s_pass_stage = new std::map<std::string, int32_t>;
    }
}

inline PassManager::PassSet PassManager::LookUp(const PassMap* map, const std::string& name) {
    PassMap::const_iterator begin, end;
    boost::tie(begin, end) = map->equal_range(name);

    PassSet result;
    for (PassMap::const_iterator ptr = begin; ptr != end; ++ptr) {
        result.insert(ptr->second);
    }
    return result;
}

inline PassManager::PassSet PassManager::Union(const PassSet& set1, const PassSet& set2) {
    PassSet result;
    std::set_union(set1.begin(), set1.end(),
                   set2.begin(), set2.end(),
                   std::inserter(result, result.end()));
    return result;
}

inline PassManager::PassSet PassManager::Diff(const PassSet& set1, const PassSet& set2) {
    PassSet result;
    std::set_difference(set1.begin(), set1.end(),
                        set2.begin(), set2.end(),
                        std::inserter(result, result.end()));
    return result;
}

inline PassManager::PassSet PassManager::Intersect(const PassSet& set1, const PassSet& set2) {
    PassSet result;
    std::set_intersection(set1.begin(), set1.end(),
                          set2.begin(), set2.end(),
                          std::inserter(result, result.end()));
    return result;
}

inline bool PassManager::Includes(const PassSet& set1, const PassSet& set2) {
    return std::includes(set1.begin(), set1.end(), set2.begin(), set2.end());
}

void PassManager::ShowPassRelations(std::ostream* stream) {
    InitPassRelations();

    *stream << "Pass relations: " << std::endl;

    for (PassMap::iterator ptr = s_depend_map->begin();
            ptr != s_depend_map->end(); ++ptr) {
        *stream << "\tRELY_PASS: " << ptr->first << " -> " << ptr->second << std::endl;
    }

    for (PassSet::iterator ptr = s_white_passes->begin();
            ptr != s_white_passes->end(); ++ptr) {
        *stream << "\tPRESERVE_BY_DEFAULT: " << *ptr << std::endl;
    }

    for (PassSet::iterator ptr = s_stable_passes->begin();
            ptr != s_stable_passes->end(); ++ptr) {
        *stream << "\tHOLD_BY_DEFAULT: " << *ptr << std::endl;
    }

    for (PassSet::iterator ptr = s_recursive_passes->begin();
            ptr != s_recursive_passes->end(); ++ptr) {
        *stream << "\tRECURSIVE: " << *ptr << std::endl;
    }

    for (PassMap::iterator ptr = s_preserve_map->begin();
            ptr != s_preserve_map->end(); ++ptr) {
        *stream << "\tPRESERVE_PASS: " << ptr->first << " -> " << ptr->second << std::endl;
    }

    for (PassMap::iterator ptr = s_invalidate_map->begin();
            ptr != s_invalidate_map->end(); ++ptr) {
        *stream << "\tINVALIDATE_PASS: " << ptr->first << " -> " << ptr->second << std::endl;
    }
}

PassManager::PassManager(Plan* plan) : m_plan(plan), m_steps(0), m_debug_pass(NULL) {
    InitPassRelations();
}

void PassManager::DeprecatedApplyRepeatedly(const std::string& pass) {
    PassSet needed_passes = LookUp(s_depend_map, pass);
    do {
        RunPasses(needed_passes);
        m_valid_passes.erase(pass);
    } while (RunPass(pass));
}

// TODO(wenxiang): applying passes in topological order according to invalidating relations.
bool PassManager::RunPasses(const PassSet& passes) {
    bool is_changed = false;

    // run all passes in order
    while (!Includes(m_valid_passes, passes)) {
        PassSet ready_passes = CollectReadyPasses(passes);

        // first apply passes who are immune from other passes
        BOOST_FOREACH(std::string pass, SelectSafePasses(ready_passes)) {
            is_changed |= RecursiveRunPass(pass);
        }

        BOOST_FOREACH(std::string pass, ready_passes) {
            is_changed |= RecursiveRunPass(pass);
        }
    }

    return is_changed;
}

// breadth-first search all related passes which are ready to run.
PassManager::PassSet PassManager::CollectReadyPasses(const PassSet& needed_passes) {
    PassSet processed_passes;
    std::deque<std::string> processing_queue(needed_passes.begin(), needed_passes.end());

    PassSet ready_passes;
    while (!processing_queue.empty()) {
        const std::string pass = processing_queue.front();
        processing_queue.pop_front();

        if (m_valid_passes.count(pass) != 0 || processed_passes.count(pass) != 0) {
            continue;
        }
        processed_passes.insert(pass);

        bool is_ready = true;
        PassMap::iterator ptr, end;
        for (boost::tie(ptr, end) = s_depend_map->equal_range(pass); ptr != end; ++ptr) {
            if (m_valid_passes.count(ptr->second) == 0) {
                processing_queue.push_back(ptr->second);
                is_ready = false;
            }
        }

        if (is_ready) {
            ready_passes.insert(pass);
        }
    }
    return ready_passes;
}

inline PassManager::PassSet PassManager::SelectSafePasses(const PassSet& passes) {
    PassSet safe_passes = passes;
    for (PassSet::iterator i = passes.begin(); i != passes.end() && !safe_passes.empty(); ++i) {
        safe_passes = QueryPreservedPasses(*i, safe_passes);
    }
    return safe_passes;
}

inline PassManager::PassSet PassManager::QueryPreservedPasses(const std::string& pass,
                                                              const PassSet& total_passes) {
    // invalidate all other passes by default
    PassSet result;

    // for PRESERVE_BY_DEFAULT
    if (s_white_passes->count(pass) != 0) {
        result = total_passes;
    }

    // for HOLD_BY_DEFAULT
    result = Union(result, Intersect(total_passes, *s_stable_passes));

    // for PRESERVE_PASS
    result = Union(result, Intersect(total_passes, LookUp(s_preserve_map, pass)));

    // for INVALIDATE_PASS
    result = Diff(result, LookUp(s_invalidate_map, pass));

    // applied pass will re-validate itself.
    if (total_passes.count(pass) != 0 && s_recursive_passes->count(pass) == 0) {
        result.insert(pass);
    }

    return result;
}

inline bool PassManager::RecursiveRunPass(const std::string& pass) {
    bool is_changed = false;
    while (m_valid_passes.count(pass) == 0
            && Includes(m_valid_passes, LookUp(s_depend_map, pass))) {
        is_changed |= RunPass(pass);
    }
    return is_changed;
}

namespace {

std::string get_stage_name(OptimizingStage stage) {
    const static std::string stage_names[] = {
        "LOGICAL_OPTIMIZING",
        "TOPOLOGICAL_OPTIMIZING",
        "RUNTIME_OPTIMIZING",
        "TRANSLATION_OPTIMIZING"
    };
    uint32_t n = static_cast<uint32_t>(stage);
    int pos[] = {-1, 0, 1, -1, 2, -1, -1, -1, 3};
    CHECK_LE(pos[n], 3);
    CHECK_GE(pos[n], 0);
    return stage_names[pos[n]];
}

} // namespace

inline bool PassManager::RunPass(const std::string& name) {
    DCHECK(Includes(m_valid_passes, LookUp(s_depend_map, name)));
    DCHECK_EQ(0, m_valid_passes.count(name));
    toft::scoped_ptr<Pass> deleter;

    Pass* pass = NULL;
    if (m_external_passes.count(name) != 0) {
        pass = m_external_passes[name];
    } else {
        deleter.reset(flume::Reflection<Pass>::New(name));
        pass = deleter.get();
    }

    LOG(INFO) << "Applying pass " << name;

    if (s_pass_stage->count(name) != 0) {
        OptimizingStage stage = m_plan->Root()->get<OptimizingStage>();
        CHECK((*s_pass_stage)[name] & stage) << "Pass [" << name << "] shouldn't have ran at stage "
                << get_stage_name(stage) <<", it may be a bug in planner.";
    }

    bool is_changed = pass->Run(m_plan);
    if (is_changed) {
        ++m_steps;
        m_valid_passes = QueryPreservedPasses(name, m_valid_passes);
    }

    if (m_steps > FLAGS_flume_planner_max_steps) {
        LOG(FATAL) << "Too many passes are applied, a dead loop may happens!";
    }

    if (s_recursive_passes->count(name) == 0 || !is_changed) {
        m_valid_passes.insert(name);
    }

    if (m_debug_pass != NULL && (is_changed || m_plan->Root()->has<Debug>())) {
        DrawPlanPass::AddMessage(m_plan->Root(), "Apply " + name);
        m_debug_pass->Run(m_plan);
    }
    m_plan->Root()->clear<Debug>();

    if (m_not_run_pass_name != name && m_valid_passes.count(name) != 0) {
        PassSet apply_passes = LookUp(s_apply_map, name);
        RunPasses(apply_passes);
    }

    return is_changed;
}

void PassManager::SetDebugPass(DrawPlanPass* pass) {
    m_debug_pass = pass;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
