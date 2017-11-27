/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

//
// Created by zhangyuncong on 2017/9/7.
//

#include "flume/planner/spark/testing/testing_helper.h"

#include "flume/planner/common/draw_plan_pass.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class SparkTaskTypeImpl : public TagDesc {
public:
    explicit SparkTaskTypeImpl(PbSparkTask::Type type) : _type(type){
    }

    virtual void Set(Unit* unit) {
        PbSparkTask& message = unit->get<PbSparkTask>();
        message.set_type(_type);
    }

    virtual bool Test(Unit* unit) {
        if (!unit->has<PbSparkTask>()) {
            return false;
        }

        const PbSparkTask& message = unit->get<PbSparkTask>();
        return message.type() == _type;
    }

private:
    PbSparkTask::Type _type;
};

TagDescRef SparkTaskType(PbSparkTask::Type type) {
    return TagDescRef(new SparkTaskTypeImpl(type));
}

class CacheFromImpl : public TagDesc {
public:
    explicit CacheFromImpl(const std::string &node_id) : _node_id(node_id){
    }

    virtual void Set(Unit* unit) {
        PbSparkTask& message = unit->get<PbSparkTask>();
        DrawPlanPass::UpdateLabel(unit ,"131-expect-cache-node-id", "Expect cache_node_id:" + _node_id);
        message.set_cache_node_id(_node_id);
    }

    virtual bool Test(Unit* unit) {
        if (!unit->has<PbSparkTask>()) {
            return false;
        }

        const PbSparkTask& message = unit->get<PbSparkTask>();
        return message.cache_node_id() == _node_id;
    }

private:
    std::string _node_id;
};

class CacheEffectiveKeyNumImpl : public TagDesc {
public:
    explicit CacheEffectiveKeyNumImpl(const int32_t &effective_key_num) : _effective_key_num(effective_key_num){
    }

    virtual void Set(Unit* unit) {
        PbSparkTask& message = unit->get<PbSparkTask>();
        message.set_cache_effective_key_number(_effective_key_num);
    }

    virtual bool Test(Unit* unit) {
        if (!unit->has<PbSparkTask>()) {
            return false;
        }

        const PbSparkTask& message = unit->get<PbSparkTask>();
        return message.cache_effective_key_number() == _effective_key_num;
    }

private:
    int32_t _effective_key_num;
};

TagDescRef CacheFrom(const std::string& node_id) {
    return TagDescRef(new CacheFromImpl(node_id));
}

TagDescRef CacheEffectiveKeyNum(const int32_t& effective_key_num) {
    return TagDescRef(new CacheEffectiveKeyNumImpl(effective_key_num));
}

PlanDesc PassTest::CacheTaskFrom(const std::string& node_id) {
    return Task() + SparkTaskType(PbSparkTask::CACHE) + CacheFrom(node_id);
}

PlanDesc PassTest::CacheTaskFrom(const PlanDesc* desc,
                                 const PlanDesc* desc1 ) {
    if(desc1 != NULL) {
        CHECK_EQ(desc->identity(), desc1->identity());
    }
    return CacheTaskFrom(desc->identity());
}

} // spark
} // planner
} // flume
} // baidu
