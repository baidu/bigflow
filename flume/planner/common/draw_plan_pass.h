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
// Author: Guo Yezhi <guoyezhi@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>
//
// Build dot description of mid-plan graph.

#ifndef FLUME_PLANNER_COMMON_DRAW_PLAN_PASS_H_
#define FLUME_PLANNER_COMMON_DRAW_PLAN_PASS_H_

#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "toft/base/closure.h"

#include "flume/planner/pass.h"

namespace baidu {
namespace flume {
namespace planner {

class Unit;

class DrawPlanPass : public Pass {
public:
    static void Clear(Unit* unit);
    static void UpdateLabel(Unit* unit, const std::string& name, const std::string& description);
    static void RemoveLabel(Unit* unit, const std::string& name);
    static void AddMessage(Unit* unit, const std::string& message); // show up only in next draw

    virtual ~DrawPlanPass();

    // take ownership of passed callback
    void RegisterListener(toft::Closure<void (const std::string&)>* callback);

    virtual bool Run(Plan* plan);

private:
    class Impl;

    boost::ptr_vector<toft::Closure<void (const std::string&)> > m_listeners;
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_COMMON_DRAW_PLAN_PASS_H_

