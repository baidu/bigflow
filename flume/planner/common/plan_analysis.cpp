/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Xu Yao <bigflow-opensource@baidu.com>

#include "flume/planner/common/plan_analysis.h"
#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace planner {

class PlanAnalysis::Impl {
public:
    void get_entities_from_physical_plan(
            const PbPhysicalPlan& physical_plan,
            EntityMap* entities) {
        for (int i = 0; i < physical_plan.job_size(); ++i) {
            get_entities_from_job(physical_plan.job(i), entities);
        }
    }

    void get_entities_from_logical_plan(
            const PbLogicalPlan& logical_plan,
            EntityMap* entities) {
        for (int i = 0; i < logical_plan.node_size(); ++i) {
            get_entities_from_logical_plan_node(logical_plan.node(i), entities);
        }

        for (int i = 0; i < logical_plan.scope_size(); ++i) {
            get_entities_from_scope(logical_plan.scope(i), entities);
        }
    }

    void set_entities_to_physical_plan(
            const EntityMap& entities,
            PbPhysicalPlan* physical_plan) {
        for (int i = 0; i < physical_plan->job_size(); ++i) {
            set_entities_to_job(entities, physical_plan->mutable_job(i));
        }
    }

    void set_entities_to_logical_plan(
            const EntityMap& entities,
            PbLogicalPlan* logical_plan) {
        for (int i = 0; i < logical_plan->node_size(); ++i) {
            set_entities_to_logical_plan_node(entities, logical_plan->mutable_node(i));
        }

        for (int i = 0; i < logical_plan->scope_size(); ++i) {
            set_entities_to_scope(entities, logical_plan->mutable_scope(i));
        }
    }

private:
    void set_entity(const EntityMap& entities, PbEntity* entity) {
        CHECK(entity->has_id()) << "The entity don't have a valid id";
        EntityMap::const_iterator found = entities.find(entity->id());
        CHECK(found != entities.end())
                << "Can't found the id in map, id[" << entity->id() << "]";
        entity->CopyFrom(found->second);
    }

    void set_entities_to_logical_plan_node(
            const EntityMap& entities,
            PbLogicalPlanNode* node) {
        if (node->has_objector()) {
            PbEntity* objector = node->mutable_objector();
            set_entity(entities, objector);
        }

        switch (node->type()) {
        case PbLogicalPlanNode::UNION_NODE:
            break;
        case PbLogicalPlanNode::LOAD_NODE: {
                PbEntity* loader = node->mutable_load_node()->mutable_loader();
                set_entity(entities, loader);
                break;
            }
        case PbLogicalPlanNode::SINK_NODE: {
                PbEntity* sinker = node->mutable_sink_node()->mutable_sinker();
                set_entity(entities, sinker);
                break;
            }
        case PbLogicalPlanNode::PROCESS_NODE: {
                PbEntity* processor = node->mutable_process_node()->mutable_processor();
                set_entity(entities, processor);
                break;
            }
        case PbLogicalPlanNode::SHUFFLE_NODE: {
                PbShuffleNode* shuffle_node = node->mutable_shuffle_node();
                switch (shuffle_node->type()) {
                case PbShuffleNode::BROADCAST:
                    break;
                case PbShuffleNode::KEY: {
                        if (shuffle_node->has_key_reader()) {
                            PbEntity* key_reader = shuffle_node->mutable_key_reader();
                            set_entity(entities, key_reader);
                        }
                        break;
                    }
                case PbShuffleNode::SEQUENCE: {
                        if (shuffle_node->has_partitioner()) {
                            PbEntity* partitioner = shuffle_node->mutable_partitioner();
                            set_entity(entities, partitioner);
                        }
                        break;
                    }
                default:
                    LOG(FATAL) << "unexpected shuffle type " << shuffle_node->type();
                }
                break;
            }
        default:
            LOG(FATAL) << "unexpected node type " << node->type();
        }
    }

    void get_entities_from_logical_plan_node(
            const PbLogicalPlanNode& node,
            EntityMap* entities) {
        if (node.has_objector()) {
            const PbEntity& objector = node.objector();
            entities->operator[](objector.id()) = objector;
        }

        switch (node.type()) {
        case PbLogicalPlanNode::UNION_NODE:
            break;
        case PbLogicalPlanNode::LOAD_NODE: {
                const PbEntity& loader = node.load_node().loader();
                entities->operator[](loader.id()) = loader;
                break;
            }
        case PbLogicalPlanNode::SINK_NODE: {
                const PbEntity& sinker = node.sink_node().sinker();
                entities->operator[](sinker.id()) = sinker;
                break;
            }
        case PbLogicalPlanNode::PROCESS_NODE: {
                const PbEntity& processor = node.process_node().processor();
                entities->operator[](processor.id()) = processor;
                break;
            }
        case PbLogicalPlanNode::SHUFFLE_NODE: {
                const PbShuffleNode& shuffle_node = node.shuffle_node();
                switch (shuffle_node.type()) {
                case PbShuffleNode::BROADCAST:
                    break;
                case PbShuffleNode::KEY: {
                        if (shuffle_node.has_key_reader()) {
                            const PbEntity& key_reader = shuffle_node.key_reader();
                            entities->operator[](key_reader.id()) = key_reader;
                        }
                        break;
                    }
                case PbShuffleNode::SEQUENCE: {
                        if (shuffle_node.has_partitioner()) {
                            const PbEntity& partitioner = shuffle_node.partitioner();
                            entities->operator[](partitioner.id()) = partitioner;
                        }
                        break;
                    }
                default:
                    LOG(FATAL) << "unexpected shuffle type " << shuffle_node.type();
                }
                break;
            }
        default:
            LOG(FATAL) << "unexpected node type " << node.type();
        }
    }

    void set_entities_to_scope(
            const EntityMap& entities,
            PbScope* scope) {
        switch (scope->type()) {
        case PbScope::DEFAULT:
        case PbScope::GROUP:
        case PbScope::BUCKET:
            break;
        case PbScope::INPUT: {
                PbEntity* spliter = scope->mutable_input_scope()->mutable_spliter();
                set_entity(entities, spliter);
                break;
            }
        default:
            LOG(FATAL) << "unexpected scope type " << scope->type();
        }
    }

    void get_entities_from_scope(
            const PbScope& scope,
            EntityMap* entities) {
        switch (scope.type()) {
        case PbScope::DEFAULT:
        case PbScope::GROUP:
        case PbScope::BUCKET:
            break;
        case PbScope::INPUT: {
                const PbEntity& spliter = scope.input_scope().spliter();
                entities->operator[](spliter.id()) = spliter;
                break;
            }
        default:
            LOG(FATAL) << "unexpected scope type " << scope.type();
        }
    }

    void set_entities_to_executor(
            const EntityMap& entities,
            PbExecutor* executor) {
        for (int i = 0; i < executor->child_size(); ++i) {
            set_entities_to_executor(entities, executor->mutable_child(i));
        }

        for (int i = 0; i < executor->dispatcher_size(); ++i) {
            PbExecutor::Dispatcher* dispatcher = executor->mutable_dispatcher(i);
            if (dispatcher->has_objector()) {
                PbEntity* objector = dispatcher->mutable_objector();
                set_entity(entities, objector);
            }
        }

        switch (executor->type()) {
        case PbExecutor::TASK:
        case PbExecutor::EXTERNAL:
        case PbExecutor::WRITE_CACHE:
        case PbExecutor::CREATE_EMPTY_RECORD:
        case PbExecutor::FILTER_EMPTY_RECORD:
        case PbExecutor::STREAM_TASK:
        case PbExecutor::STREAM_EXTERNAL:
            break;
        case PbExecutor::PROCESSOR: {
                PbProcessorExecutor* processor_executor =
                        executor->mutable_processor_executor();
                PbEntity* processor = processor_executor->mutable_processor();
                set_entity(entities, processor);
                break;
            }
        case PbExecutor::LOGICAL: {
                PbLogicalExecutor* logical_executor =
                        executor->mutable_logical_executor();
                PbLogicalPlanNode* node = logical_executor->mutable_node();
                set_entities_to_logical_plan_node(entities, node);
                break;
            }
        case PbExecutor::SHUFFLE: {
                PbShuffleExecutor* shuffle_executor =
                        executor->mutable_shuffle_executor();
                for (int i = 0; i < shuffle_executor->node_size(); ++i) {
                    PbLogicalPlanNode* node = shuffle_executor->mutable_node(i);
                    set_entities_to_logical_plan_node(entities, node);
                }
                set_entities_to_scope(entities, shuffle_executor->mutable_scope());
                break;
            }
        case PbExecutor::PARTIAL: {
                PbPartialExecutor* partial_executor =
                        executor->mutable_partial_executor();
                for (int i = 0; i < partial_executor->node_size(); ++i) {
                    PbLogicalPlanNode* node = partial_executor->mutable_node(i);
                    set_entities_to_logical_plan_node(entities, node);
                }
                for (int i = 0; i < partial_executor->scope_size(); ++i) {
                    set_entities_to_scope(entities, partial_executor->mutable_scope(i));
                }
                for (int i = 0; i < partial_executor->output_size(); ++i) {
                    PbPartialExecutor::Output* output = partial_executor->mutable_output(i);
                    PbEntity* objector = output->mutable_objector();
                    set_entity(entities, objector);
                }
                break;
            }
        case PbExecutor::STREAM_PROCESSOR: {
                PbStreamProcessorExecutor* stream_processor_executor =
                        executor->mutable_stream_processor_executor();
                PbEntity* processor = stream_processor_executor->mutable_processor();
                set_entity(entities, processor);
                break;
            }
        case PbExecutor::STREAM_LOGICAL: {
                PbStreamLogicalExecutor* stream_logical_executor =
                        executor->mutable_stream_logical_executor();
                PbLogicalPlanNode* node = stream_logical_executor->mutable_node();
                set_entities_to_logical_plan_node(entities, node);
                break;
            }
        // case PbExecutor::WINDOW:
        case PbExecutor::STREAM_SHUFFLE: {
                PbStreamShuffleExecutor* stream_shuffle_executor =
                        executor->mutable_stream_shuffle_executor();
                for (int i = 0; i < stream_shuffle_executor->node_size(); ++i) {
                    PbLogicalPlanNode* node = stream_shuffle_executor->mutable_node(i);
                    set_entities_to_logical_plan_node(entities, node);
                }
                set_entities_to_scope(entities, stream_shuffle_executor->mutable_scope());
                break;
            }

        default:
            LOG(FATAL) << "unexpected executor type " << executor->type();
        }
    }

    void get_entities_from_executor(
            const PbExecutor& executor,
            EntityMap* entities) {
        for (int i = 0; i < executor.child_size(); ++i) {
            get_entities_from_executor(executor.child(i), entities);
        }

        for (int i = 0; i < executor.dispatcher_size(); ++i) {
            const PbExecutor::Dispatcher& dispatcher = executor.dispatcher(i);
            if (dispatcher.has_objector()) {
                const PbEntity& objector = dispatcher.objector();
                entities->operator[](objector.id()) = objector;
            }
        }

        switch (executor.type()) {
        case PbExecutor::TASK:
        case PbExecutor::EXTERNAL:
        case PbExecutor::WRITE_CACHE:
        case PbExecutor::CREATE_EMPTY_RECORD:
        case PbExecutor::FILTER_EMPTY_RECORD:
        case PbExecutor::STREAM_TASK:
        case PbExecutor::STREAM_EXTERNAL:
            break;
        case PbExecutor::PROCESSOR: {
                const PbProcessorExecutor& processor_executor = executor.processor_executor();
                const PbEntity& processor = processor_executor.processor();
                entities->operator[](processor.id()) = processor;
                break;
            }
        case PbExecutor::LOGICAL: {
                const PbLogicalExecutor& logical_executor = executor.logical_executor();
                get_entities_from_logical_plan_node(logical_executor.node(), entities);
                break;
            }
        case PbExecutor::SHUFFLE: {
                const PbShuffleExecutor& shuffle_executor = executor.shuffle_executor();
                for (int i = 0; i < shuffle_executor.node_size(); ++i) {
                    get_entities_from_logical_plan_node(shuffle_executor.node(i), entities);
                }
                get_entities_from_scope(shuffle_executor.scope(), entities);
                break;
            }
        case PbExecutor::PARTIAL: {
                const PbPartialExecutor& partial_executor = executor.partial_executor();
                for (int i = 0; i < partial_executor.node_size(); ++i) {
                    get_entities_from_logical_plan_node(partial_executor.node(i), entities);
                }
                for (int i = 0; i < partial_executor.scope_size(); ++i) {
                    get_entities_from_scope(partial_executor.scope(i), entities);
                }
                for (int i = 0; i < partial_executor.output_size(); ++i) {
                    const PbPartialExecutor::Output& output = partial_executor.output(i);
                    const PbEntity& objector = output.objector();
                    entities->operator[](objector.id()) = objector;
                }
                break;
            }
        case PbExecutor::STREAM_PROCESSOR: {
                const PbStreamProcessorExecutor& stream_processor_executor =
                        executor.stream_processor_executor();
                const PbEntity& processor = stream_processor_executor.processor();
                entities->operator[](processor.id()) = processor;
                break;
            }
        case PbExecutor::STREAM_LOGICAL: {
                const PbStreamLogicalExecutor& stream_logical_executor =
                        executor.stream_logical_executor();
                get_entities_from_logical_plan_node(stream_logical_executor.node(), entities);
                break;
            }
        case PbExecutor::STREAM_SHUFFLE: {
                const PbStreamShuffleExecutor& stream_shuffle_executor =
                        executor.stream_shuffle_executor();
                for (int i = 0; i < stream_shuffle_executor.node_size(); ++i) {
                    get_entities_from_logical_plan_node(stream_shuffle_executor.node(i), entities);
                }
                get_entities_from_scope(stream_shuffle_executor.scope(), entities);
                break;
            }
        default:
            LOG(FATAL) << "unexpected executor type " << executor.type();
        }
    }

    void set_entities_to_local_task(
            const EntityMap& entities,
            PbLocalTask* local_task) {
        set_entities_to_executor(entities, local_task->mutable_root());
    }

    void get_entities_from_local_task(
            const PbLocalTask& local_task,
            EntityMap* entities) {
        get_entities_from_executor(local_task.root(), entities);
    }

    void set_entities_to_local_input(
            const EntityMap& entities,
            PbLocalInput* local_input) {
        PbEntity* spliter = local_input->mutable_spliter();
        set_entity(entities, spliter);
    }

    void get_entities_from_local_input(
            const PbLocalInput& local_input,
            EntityMap* entities) {
        const PbEntity& spliter = local_input.spliter();
        entities->operator[](spliter.id()) = spliter;
    }

    void set_entities_to_local_job(
            const EntityMap& entities,
            PbLocalJob* local_job) {
        set_entities_to_local_task(entities, local_job->mutable_task());

        for (int i = 0; i < local_job->input_size(); ++i) {
            set_entities_to_local_input(entities, local_job->mutable_input(i));
        }
    }

    void get_entities_from_local_job(
            const PbLocalJob& local_job,
            EntityMap* entities) {
        get_entities_from_local_task(local_job.task(), entities);

        for (int i = 0; i < local_job.input_size(); ++i) {
            get_entities_from_local_input(local_job.input(i), entities);
        }
    }

    void set_entities_to_tm_task(
            const EntityMap& entities,
            PbTmTask* tm_task) {
        set_entities_to_executor(entities, tm_task->mutable_root());

        if (tm_task->has_stream_input()) {
            PbTmTask::StreamInput* stream_input = tm_task->mutable_stream_input();
            PbEntity* spliter = stream_input->mutable_spliter();
            set_entity(entities, spliter);
        }
    }

    void get_entities_from_tm_task(
            const PbTmTask& tm_task,
            EntityMap* entities) {
        get_entities_from_executor(tm_task.root(), entities);

        if (tm_task.has_stream_input()) {
            const PbTmTask::StreamInput& stream_input = tm_task.stream_input();
            const PbEntity& spliter = stream_input.spliter();
            entities->operator[](spliter.id()) = spliter;
        }
    }

    void set_entities_to_tm_job(
            const EntityMap& entities,
            PbTmJob* tm_job) {
        for (int i = 0; i < tm_job->task_size(); ++i) {
            set_entities_to_tm_task(entities, tm_job->mutable_task(i));
        }
    }

    void get_entities_from_tm_job(
            const PbTmJob& tm_job,
            EntityMap* entities) {
        for (int i = 0; i < tm_job.task_size(); ++i) {
            get_entities_from_tm_task(tm_job.task(i), entities);
        }
    }

    void set_entities_to_job(
            const EntityMap& entities,
            PbJob* job) {
        switch (job->type()) {
        case PbJob::LOCAL:
            set_entities_to_local_job(entities, job->mutable_local_job());
            break;
        case PbJob::TM:
            set_entities_to_tm_job(entities, job->mutable_tm_job());
            break;
        default:
            LOG(FATAL) << "unexpected job type " << job->type();
        }
    }

    void get_entities_from_job(
            const PbJob& job,
            EntityMap* entities) {
        switch (job.type()) {
        case PbJob::LOCAL:
            get_entities_from_local_job(job.local_job(), entities);
            break;
        case PbJob::TM:
            get_entities_from_tm_job(job.tm_job(), entities);
            break;
        default:
            LOG(FATAL) << "unexpected job type " << job.type();
        }
    }
};

PlanAnalysis::PlanAnalysis() : _impl(new PlanAnalysis::Impl()) {}

PlanAnalysis::~PlanAnalysis() {}

void PlanAnalysis::get_entities_from_physical_plan(
        const PbPhysicalPlan& physical_plan,
        EntityMap* entities) {
    _impl->get_entities_from_physical_plan(physical_plan, entities);
}

void PlanAnalysis::get_entities_from_logical_plan(
        const PbLogicalPlan& logical_plan,
        EntityMap* entities) {
    _impl->get_entities_from_logical_plan(logical_plan, entities);
}

void PlanAnalysis::set_entities_to_physical_plan(
        const EntityMap& entities,
        PbPhysicalPlan* physical_plan) {
    _impl->set_entities_to_physical_plan(entities, physical_plan);
}

void PlanAnalysis::set_entities_to_logical_plan(
        const EntityMap& entities,
        PbLogicalPlan* logical_plan) {
    _impl->set_entities_to_logical_plan(entities, logical_plan);
}


}  // namespace planner
}  // namespace flume
}  // namespace baidu

