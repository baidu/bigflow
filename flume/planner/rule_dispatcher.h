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
// Author: Zhou Kai <zhoukai01@baidu.com>

#ifndef FLUME_PLANNER_RULE_DISPATCHER_H_
#define FLUME_PLANNER_RULE_DISPATCHER_H_

#include "flume/planner/dispatcher.h"
#include "flume/planner/unit.h"
#include "boost/ptr_container/ptr_vector.hpp"

namespace baidu {
namespace flume {
namespace planner {

class Plan;
class Unit;

class RuleDispatcher : public Dispatcher {
public:
    class Rule {
    public:
        virtual ~Rule() {}
        // return true if this Rule could Run(plan, unit)
        virtual bool Accept(Plan* plan, Unit* unit) = 0;
        // return true if plan is modified
        virtual bool Run(Plan* plan, Unit* unit) = 0;
    };

    virtual bool Run(Plan* plan);

    // take ownership of rule
    virtual void AddRule(Rule* rule);

protected:
    virtual bool Dispatch(Plan* plan, Unit* unit);

    boost::ptr_vector<Rule> m_rules;
};

template<Unit::Type kType>
class TypeRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == kType;
    }
};

class LeafRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->is_leaf();
    }
};

class ControlRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return !unit->is_leaf();
    }
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_RULE_DISPATCHER_H_
