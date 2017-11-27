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

#include "flume/planner/common/prune_cached_path_pass.h"

#include "flume/planner/common/cache_util.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/runtime/common/file_cache_manager.h"

namespace baidu {
namespace flume {
namespace planner {

// Cache has no dependency on other nodes, thus we can simpliy
// remove all the upstreams for cache nodes
namespace {

class RemoveCacheUpstreams : public RuleDispatcher::Rule {
public:
    RemoveCacheUpstreams() { }
    virtual ~RemoveCacheUpstreams() { }

    virtual bool Accept(Plan *plan, Unit *unit) {
        return unit->type() == Unit::CACHE_READER;
    }

    virtual bool Run(Plan *plan, Unit *unit) {
        // step-1: generate load scope and load node
        //
        Unit *load_scope = plan->NewUnit(/*Is leaf*/false);
        PbScope &pb_scope = load_scope->get<PbScope>();
        pb_scope.set_type(PbScope::INPUT);
        pb_scope.set_id(load_scope->identity());
        pb_scope.set_father(plan->Root()->identity());
        PbInputScope *pb_input_scope = pb_scope.mutable_input_scope();
        *(pb_input_scope->mutable_spliter()) = CacheLoaderPbEntity();
        pb_input_scope->add_uri(InputUri(plan, unit));
        load_scope->set_type(Unit::SCOPE);

        Unit *load_node = plan->NewUnit(true);
        PbLogicalPlanNode &load_logical_node = load_node->get<PbLogicalPlanNode>();
        load_logical_node.set_id(load_node->identity());
        load_logical_node.set_type(PbLogicalPlanNode::LOAD_NODE);
        load_logical_node.set_scope(load_scope->identity());
        *(load_logical_node.mutable_objector()) = CacheRecordObjectorEntity();
        PbLoadNode *pb_load_node = load_logical_node.mutable_load_node();
        *(pb_load_node->mutable_loader()) = CacheLoaderPbEntity();
        pb_load_node->add_uri(InputUri(plan, unit));
        load_node->set_type(Unit::LOAD_NODE);

        plan->AddControl(plan->Root(), load_scope);
        plan->AddControl(load_scope, load_node);

        // step-2: new a process node to process what load node generate
        //
        Unit *cache_reader = plan->NewUnit(true);
        PbLogicalPlanNode &cache_logical_node = cache_reader->get<PbLogicalPlanNode>();
        cache_logical_node.set_id(cache_reader->identity());
        cache_logical_node.set_type(PbLogicalPlanNode::PROCESS_NODE);
        cache_logical_node.set_scope(load_scope->identity());
        *(cache_logical_node.mutable_objector()) = CacheRecordObjectorEntity();
        PbProcessNode *pb_process_node = cache_logical_node.mutable_process_node();
        PbProcessNode::Input *input = pb_process_node->add_input();
        input->set_from(load_node->identity());
        *(pb_process_node->mutable_processor()) = CacheReaderPbEntity();

        cache_reader->set_type(Unit::PROCESS_NODE);
        plan->AddDependency(load_node, cache_reader);
        plan->AddControl(load_scope, cache_reader);

        // step-3: generate new shuffle node if necessary
        //
        std::list<Unit*> upstream_scopes;
        uint32_t depth = 0, level = 0;
        Unit *origin = cache_reader;
        for (Unit *scope = unit->father(); scope != plan->Root(); scope = scope->father()) {
            if (scope->type() <= Unit::TASK) continue;
            upstream_scopes.push_front(scope);
            ++depth;
        }
        typedef std::list<Unit*>::iterator LIT;
        for (LIT it = upstream_scopes.begin();
             it != upstream_scopes.end(); ++it) {
            Unit *scope = *it;
            CHECK(scope->has<PbScope>());
            ++level;
            const PbScope &pb_scope = scope->get<PbScope>();
            // Background: we can have shuffle scope under load socpe,
            // not vice verse. Besides load scope can not be nested.
            // In one word load scope can only be the outer most scope
            CHECK(pb_scope.type() != PbScope::INPUT || level == 1);
            // If outer most scope is input scope, we do nothing, the node added from step1-2
            // will be deleted by RemoveUnsinkPass
            if (pb_scope.type() == PbScope::INPUT) return false;
            // for shuffle scope
            Unit *new_shuffle_node = NewShuffleUnitAndLogicalNode(plan, origin, level, pb_scope);
            plan->AddControl(scope, new_shuffle_node);
            plan->AddDependency(origin, new_shuffle_node);
            origin = new_shuffle_node;
        }

        // step-4: if unit is process node, make sure it will strip all the keys
        //
        unit->set_type(Unit::PROCESS_NODE);
        PbLogicalPlanNode &unit_logical_node = unit->get<PbLogicalPlanNode>();
        CHECK(unit_logical_node.type() != PbLogicalPlanNode::LOAD_NODE
              && unit_logical_node.type() != PbLogicalPlanNode::SINK_NODE);

        if (unit_logical_node.type() == PbLogicalPlanNode::UNION_NODE) {
            unit_logical_node.clear_union_node();
        } else if (unit_logical_node.type() == PbLogicalPlanNode::SHUFFLE_NODE) {
            unit_logical_node.clear_shuffle_node();
        }
        unit_logical_node.set_cache(false);
        unit_logical_node.set_type(PbLogicalPlanNode::PROCESS_NODE);
        PbProcessNode *unit_pb_process_node  = unit_logical_node.mutable_process_node();
        unit_pb_process_node->clear_input();
        CHECK(unit_logical_node.has_objector());
        *(unit_pb_process_node->mutable_processor()) = StripKeyProcessorEntity(
            unit_logical_node.objector().SerializeAsString());

        // step-5: cut off current unit with its direct_needs
        // we'll apply RemoveUnsinkPass immediately
        typedef std::vector<Unit*> Vec;
        Vec needs = unit->direct_needs();
        for (Vec::iterator it = needs.begin(); it != needs.end(); ++it) {
            Unit *need = *it;
            plan->RemoveDependency(need, unit);
        }

        // step-6: link unit with its new origin
        plan->AddDependency(origin, unit);
        unit_pb_process_node->add_input()->set_from(origin->identity());
        unit_pb_process_node->mutable_input(0)->set_is_partial(true);

        return true;
    }

private:
    std::string InputUri(Plan *plan, Unit *unit) {
        const PbJobConfig *job_config = plan->Root()->get<JobConfig>();
        DLOG(INFO)<<"Input Path: "<<job_config->tmp_data_path_input();
        return runtime::FileCacheManager::GetCachedNodePath(job_config->tmp_data_path_input(), unit->identity());
    }

    Unit* NewShuffleUnitAndLogicalNode(Plan *plan, Unit *source,
                                       uint32_t level, const PbScope &pb_scope) {
        Unit *shuffle_node = plan->NewUnit(true);
        shuffle_node->set_type(Unit::SHUFFLE_NODE);

        PbLogicalPlanNode &shuffle_logical_node = shuffle_node->get<PbLogicalPlanNode>();
        shuffle_logical_node.set_id(shuffle_node->identity());
        shuffle_logical_node.set_type(PbLogicalPlanNode::SHUFFLE_NODE);
        shuffle_logical_node.set_scope(pb_scope.id());
        // Consistent with CacheReader
        *(shuffle_logical_node.mutable_objector()) = CacheRecordObjectorEntity();

        PbShuffleNode *pb_shuffle_node = shuffle_logical_node.mutable_shuffle_node();
        pb_shuffle_node->set_from(source->identity());
        if (pb_scope.type() == PbScope::GROUP) {
            pb_shuffle_node->set_type(PbShuffleNode::KEY);
            *(pb_shuffle_node->mutable_key_reader()) = LevelKeyReaderEntity(level);
        } else {
            pb_shuffle_node->set_type(PbShuffleNode::SEQUENCE);
            *(pb_shuffle_node->mutable_key_reader()) = LevelPartitionerEntity(level);
        }
        shuffle_node->set<LoadCacheChannel>();
        return shuffle_node;
    }
};
}  // anoymous namespace

bool PruneCachedPathPass::Run(Plan *plan) {
    RuleDispatcher rule_dispatcher;
    rule_dispatcher.AddRule(new RemoveCacheUpstreams());
    return rule_dispatcher.Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
