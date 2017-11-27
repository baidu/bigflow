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
// Author: Pan Yuchang <panyuchang@baidu.com>
//         Zhang Yuncong <zhangyuncong@baidu.com>
//         Wang Cong <wangcong09@baidu.com>

#include "flume/planner/spark/build_transfer_executor_pass.h"

#include <limits>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/range/algorithm.hpp"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/common/util.h"
#include "flume/planner/spark/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/io/io_format.h"
#include "flume/util/reflection.h"

namespace baidu {
namespace flume {

namespace runtime {
namespace spark {
class ShuffleInputExecutor;
}  // namspace spark
}  // namespace runtime

namespace planner {
namespace spark {

namespace {
struct TransferTag : public Value<uint32_t> {};
struct TransferScope : public PbScope {};
struct TransferPort : public Value<std::string> {};
struct TransferType: public Value<PbShuffleNode::Type> {};
}  // namespace

using google::protobuf::RepeatedFieldBackInserter;

class BuildTransferExecutorPass::BuildHadoopInputPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new BuildHadoopInputRule());
        dispatcher.AddRule(new BuildCacheInputRule());
        return dispatcher.Run(plan);
    }

    class BuildHadoopInputRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::EXTERNAL_EXECUTOR
                    && unit->get<ExternalExecutor>() == HADOOP_INPUT;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            using runtime::TextInputFormat;
            using runtime::SequenceFileAsBinaryInputFormat;

            CHECK(unit->get<PbScope>().has_input_scope());
            const PbInputScope& input_scope = unit->get<PbScope>().input_scope();

            PbSparkTask::PbHadoopInput* input = &unit->get<PbSparkTask::PbHadoopInput>();
            *input->mutable_spliter() = input_scope.spliter();
            *input->mutable_uri() = input_scope.uri();

            // when using Hadoop InputFormat, HadoopInputExecutor will delegating loader's
            // output. We reuse external id as source identity at that case
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::LOAD_NODE) {
                    input->set_id(child->identity());
                }
            }

            std::string loader = input_scope.spliter().name();
            std::string config = input_scope.spliter().config();
            if (loader == PrettyTypeName<TextInputFormat>()) {
                input->set_input_format("TextInputFormat");
            } else if (loader == PrettyTypeName<SequenceFileAsBinaryInputFormat>()) {
                input->set_input_format("SequenceFileAsBinaryInputFormat");
            } else {
                // HadoopInputExecutor
                input->set_id(unit->identity());
            }

            PbExecutor* executor = &unit->get<PbExecutor>();
            executor->set_type(PbExecutor::EXTERNAL);
            executor->mutable_external_executor()->set_id(input->id());

            return false;
        }
    };

    class BuildCacheInputRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::EXTERNAL_EXECUTOR
                   && unit->get<ExternalExecutor>() == CACHE_INPUT;
        }

        virtual bool Run(Plan* plan, Unit* unit) {

            PbSparkTask::PbCacheInput* input = &unit->get<PbSparkTask::PbCacheInput>();
            CacheNodeInfo cache_node_info = unit->get<CacheNodeInfo>();
            input->set_cache_node_id(cache_node_info.cache_node_id);
            input->set_key_num(cache_node_info.key_num);

            // CacheInputExecutor will delegating loader's
            // output. We reuse external id as source identity at that case
            for(Unit* child : unit->children()) {
                if (child->type() == Unit::LOAD_NODE) {
                    input->set_id(child->identity());
                }
            }

            PbExecutor* executor = &unit->get<PbExecutor>();
            executor->set_type(PbExecutor::EXTERNAL);
            executor->mutable_external_executor()->set_id(input->id());

            return false;
        }
    };
};

bool BuildTransferExecutorPass::BuildHadoopInputPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class BuildTransferExecutorPass::BuildShuffleInputPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new BuildShuffleInputPass);
        return dispatcher.Run(plan);
    }

    class BuildShuffleInputPass : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::EXTERNAL_EXECUTOR
                    && unit->get<ExternalExecutor>() == SHUFFLE_INPUT;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbExecutor &executor = unit->get<PbExecutor>();
            executor.set_type(PbExecutor::EXTERNAL);
            executor.mutable_external_executor()->set_id(unit->identity());

            PbSparkTask::PbShuffleInput* reduce_input = &unit->get<PbSparkTask::PbShuffleInput>();
            reduce_input->Clear();
            reduce_input->set_id(unit->identity());
            reduce_input->mutable_decoder()->CopyFrom(unit->get<PbTransferDecoder>());

            if (plan->Root()->has<JobConfig>() && !plan->Root()->get<JobConfig>().is_null()) {
                JobConfig& config = plan->Root()->get<JobConfig>();
                if (config->has_must_keep_empty_group()) {
                    reduce_input->set_must_keep_empty_group(config->must_keep_empty_group());
                }
            }

            reduce_input->clear_channel();
            BOOST_FOREACH(Unit* child, unit->children()) {
                PbSparkTask::PbShuffleInput::Channel* channel = reduce_input->add_channel();
                channel->set_identity(child->identity());
                channel->set_port(*child->get<TransferPort>());
                channel->set_priority(*child->get<TransferTag>());
                child->set<PbSparkTask::PbShuffleInput::Channel>(*channel);
            }

            return true;
        }
    };
};

bool BuildTransferExecutorPass::BuildShuffleInputPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class BuildTransferExecutorPass::BuildShuffleOutputPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new BuildShuffleOutputPass);
        return dispatcher.Run(plan);
    }

    class BuildShuffleOutputPass : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::EXTERNAL_EXECUTOR
                    && unit->get<ExternalExecutor>() == SHUFFLE_OUTPUT;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbExecutor &executor = unit->get<PbExecutor>();
            executor.set_type(PbExecutor::EXTERNAL);
            executor.mutable_external_executor()->set_id(unit->identity());

            PbSparkTask::PbShuffleOutput& reduce_output = unit->get<PbSparkTask::PbShuffleOutput>();
            reduce_output.set_id(unit->identity());
            reduce_output.clear_channel();

            BOOST_FOREACH(Unit* child , unit->children()) {
                PbSparkTask::PbShuffleOutput::Channel* channel = reduce_output.add_channel();

                CHECK_EQ(1, child->direct_needs().size());
                Unit* from = child->direct_needs().front();
                channel->set_from(from->identity());

                CHECK_EQ(1, child->direct_users().size());
                Unit* target = child->direct_users().front();
                CHECK_EQ(Unit::CHANNEL, target->type());

                PbScope target_transfer_scope = target->get<TransferScope>();
                channel->mutable_transfer_scope()->CopyFrom(target->get<TransferScope>());
                if (target->has<TransferType>()) {
                    PbShuffleNode::Type sn_type = *(target->get<TransferType>());

                    if (sn_type == PbShuffleNode::KEY) {
                        //target_transfer_scope->mutable_type()->CopyFrom(PbScope::GROUP);
                        channel->set_transfer_type(PbSparkTask::PbShuffleOutput::Channel::KEY);
                    } else if (sn_type == PbShuffleNode::SEQUENCE) {
                        //target_transfer_scope->mutable_type()->CopyFrom(PbScope::BUCKET);
                        channel->set_transfer_type(PbSparkTask::PbShuffleOutput::Channel::SEQUENCE);
                    } else if (sn_type == PbShuffleNode::BROADCAST) {
                        //target_transfer_scope->mutable_type()->CopyFrom(PbScope::GROUP);
                        channel->set_transfer_type(PbSparkTask::PbShuffleOutput::Channel::BROADCAST);
                    }

                    CHECK(channel->has_transfer_type()) << "channel's unit id: "
                                                        << target->identity();
                }

                channel->mutable_encoder()->CopyFrom(target->get<PbTransferEncoder>());

                Unit* target_task = target->task();
                channel->set_task_index(*target_task->get<TaskIndex>());
                channel->set_task_concurrency(*target_task->get<TaskConcurrency>());
                channel->set_task_offset(*target_task->get<TaskOffset>());
                child->set<PbSparkTask::PbShuffleOutput::Channel>(*channel);
            }

            return true;
        }
    };
};

bool BuildTransferExecutorPass::BuildShuffleOutputPass::Run(Plan* plan) {
    return Impl::Run(plan);
}
class BuildTransferExecutorPass::TransferAnalysis::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher clean_phase;
        clean_phase.AddRule(new CleanTags);
        clean_phase.Run(plan);

        TopologicalDispatcher search_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        search_phase.AddRule(new FindShuffleInputChannel);
        search_phase.AddRule(new FindMergeInputChannel);
        search_phase.AddRule(new FindTerminalChannel);
        search_phase.Run(plan);

        DepthFirstDispatcher init_phase(DepthFirstDispatcher::POST_ORDER);
        init_phase.AddRule(new InitTransferEncodingForTerminalChannel);
        init_phase.AddRule(new InitTransferEncodingForExecutor);
        init_phase.AddRule(new InitTransferTag);
        init_phase.Run(plan);

        DepthFirstDispatcher post_analyze_phase(DepthFirstDispatcher::POST_ORDER);
        post_analyze_phase.AddRule(new BuildShuffleDecoderForTerminalChannel);
        post_analyze_phase.AddRule(new BuildShuffleDecoderForShuffleExecutor);
        post_analyze_phase.AddRule(new BuildShuffleDecoderForShuffleInputExecutor);
        post_analyze_phase.Run(plan);

        TopologicalDispatcher reverse_analyze_phase(TopologicalDispatcher::REVERSE_ORDER);
        reverse_analyze_phase.AddRule(new SetTransferScope);
        reverse_analyze_phase.AddRule(new SetTransferPort);
        reverse_analyze_phase.AddRule(new BuildTransferEncoderForShuffleInputChannel);
        reverse_analyze_phase.Run(plan);

        RuleDispatcher fix_empty_group_phase;
        fix_empty_group_phase.AddRule(new BuildCreateEmptyRecordExecutor());
        fix_empty_group_phase.AddRule(new BuildFilterEmptyRecordExecutor());
        fix_empty_group_phase.Run(plan);

        return false;
    }

    typedef TaskSingleton<runtime::spark::ShuffleInputExecutor> ShuffleInputExecutor;
    typedef PbTransferEncoder::TagEncoding TagEncoding;
    struct IsShuffleInputRelated {};
    struct IsTerminal {};

    class CleanTags : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<TransferScope>();
            unit->clear<TransferPort>();
            unit->clear<TransferTag>();
            unit->clear<TagEncoding>();

            unit->clear<PbTransferEncodingField>();
            unit->clear<PbTransferDecoder>();
            unit->clear<PbTransferEncoder>();

            unit->clear<IsShuffleInputRelated>();
            unit->clear<IsTerminal>();

            return false;
        }
    };

    class FindShuffleInputChannel : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::CHANNEL
                    && unit->father()->type() == Unit::EXTERNAL_EXECUTOR
                    && unit->father()->get<ExternalExecutor>() == SHUFFLE_INPUT;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsShuffleInputRelated>();
            unit->father()->set<IsShuffleInputRelated>();
            return false;
        }
    };

    class FindMergeInputChannel : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::CHANNEL
                    || unit->father()->type() != Unit::SHUFFLE_EXECUTOR) {
                return false;
            }

            CHECK_EQ(unit->direct_needs().size(), 1);
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                return need->has<IsShuffleInputRelated>();
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsShuffleInputRelated>();
            unit->father()->set<IsShuffleInputRelated>();
            return false;
        }
    };

    class FindTerminalChannel : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->has<IsShuffleInputRelated>() || unit->type() != Unit::CHANNEL) {
                return false;
            }

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->type() == Unit::CHANNEL
                        && user->father()->type() == Unit::SHUFFLE_EXECUTOR) {
                    return false;
                }
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsTerminal>();
            return false;
        }
    };

    class InitTransferEncodingForTerminalChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<IsTerminal>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbTransferEncodingField* field = &unit->get<PbTransferEncodingField>();
            field->Clear();
            field->set_identity(unit->identity());
            field->set_type(PbTransferEncodingField::RECORD);

            return false;
        }
    };

    class InitTransferEncodingForExecutor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf() && unit->has<IsShuffleInputRelated>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbTransferEncodingField* field = &unit->get<PbTransferEncodingField>();
            field->Clear();
            field->set_identity(unit->identity());

            bool is_tailing_key = true;

            uint32_t record_count = 0;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::SHUFFLE_EXECUTOR
                        && child->has<IsShuffleInputRelated>()) {
                    is_tailing_key = false;
                }

                if (child->has<IsTerminal>()) {
                    if (child->has<OrderScopes>()) {
                        is_tailing_key = false;
                    }

                    ++record_count;
                    if (record_count > 1) {
                        is_tailing_key = false;
                    }
                }
            }

            const PbScope& scope = unit->get<PbScope>();
            if (unit->type() != Unit::SHUFFLE_EXECUTOR) {
                CHECK_EQ(unit->type(), Unit::EXTERNAL_EXECUTOR);
                field->set_type(PbTransferEncodingField::GLOBAL_KEY);
            } else if (unit->has<IsLocalDistribute>()) {
                // key can be get from partition
                field->set_type(PbTransferEncodingField::LOCAL_PARTITION);
            } else if (scope.type() == PbScope::BUCKET) {
                field->set_type(PbTransferEncodingField::FIXED_LENGTH_KEY);
                field->set_length(4);  // sizeof(uint32)
            } else if (is_tailing_key) {
                field->set_type(PbTransferEncodingField::TAILING_KEY);
            } else {
                field->set_type(PbTransferEncodingField::VARIABLE_LENGTH_KEY);
            }

            return false;
        }
    };

    class InitTransferTag : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf() && unit->has<IsShuffleInputRelated>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            const uint64_t kDefaultTerminalPriority =
                    static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1ul;
            const uint64_t kDefaultPassbyPriority =
                    static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 2ul;

            // use multimap for sorting units
            typedef std::pair<uint64_t, Unit*> TransferUnit;
            std::set<TransferUnit> transfer_units;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::CHANNEL && child->has<IsTerminal>()) {
                    uint64_t priority = kDefaultTerminalPriority;
                    if (child->has<PreparePriority>()) {
                        priority = *child->get<PreparePriority>();
                    }
                    transfer_units.insert(std::make_pair(priority, child));
                }

                if (child->type() == Unit::CHANNEL
                        && child->has<IsShuffleInputRelated>()
                        && !child->has<IsTerminal>()) {
                    CHECK_EQ(child->direct_users().size(), 1);
                    Unit* shuffle_executor = child->direct_users().front()->father();

                    uint64_t priority = kDefaultPassbyPriority;
                    if (shuffle_executor->has<PreparePriority>()) {
                        priority = *shuffle_executor->get<PreparePriority>();
                    }
                    transfer_units.insert(std::make_pair(priority, shuffle_executor));
                }
            }

            uint32_t next_tag = 0;
            BOOST_FOREACH(TransferUnit pair, transfer_units) {
                Unit* transfer_unit = pair.second;

                *transfer_unit->get<TransferTag>() = next_tag;
                BOOST_FOREACH(Unit* child, unit->children()) {
                    if (child->type() == Unit::CHANNEL
                            && child->has<IsShuffleInputRelated>()
                            && !child->has<IsTerminal>()) {
                        Unit* shuffle_executor = child->direct_users().front()->father();
                        if (shuffle_executor == transfer_unit) {
                            *child->get<TransferTag>() = next_tag;
                        }
                    }
                }

                std::string label =
                        "Transfer Tag: " + boost::lexical_cast<std::string>(next_tag);
                DrawPlanPass::UpdateLabel(transfer_unit, "40-transfer-tag", label);

                ++next_tag;
            }

            if (next_tag == 1) {
                unit->get<TagEncoding>() = PbTransferEncoder::UNIQUE;
            } else if (next_tag <= 256) {
                unit->get<TagEncoding>() = PbTransferEncoder::UINT8;
            } else {
                CHECK_LT(next_tag, std::numeric_limits<uint16_t>::max());
                unit->get<TagEncoding>() = PbTransferEncoder::UINT16;
            }

            return false;
        }
    };

    class BuildShuffleDecoderForTerminalChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<IsTerminal>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbTransferDecoder* decoder = &unit->get<PbTransferDecoder>();
            *decoder->mutable_field() = unit->get<PbTransferEncodingField>();
            return false;
        }
    };

    class BuildShuffleDecoderForShuffleExecutor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SHUFFLE_EXECUTOR && unit->has<IsShuffleInputRelated>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbTransferDecoder* decoder = &unit->get<PbTransferDecoder>();
            *decoder->mutable_field() = unit->get<PbTransferEncodingField>();

            std::vector<PbTransferDecoder> successors;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (!child->has<PbTransferDecoder>()) {
                    continue;
                }

                uint32_t tag = *child->get<TransferTag>();
                if (tag >= successors.size()) {
                    successors.resize(tag + 1);
                }
                successors[tag] = child->get<PbTransferDecoder>();
            }
            boost::copy(successors, RepeatedFieldBackInserter(decoder->mutable_successor()));

            return false;
        }
    };

    class BuildShuffleDecoderForShuffleInputExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK && unit->has<ShuffleInputExecutor>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = unit->get<ShuffleInputExecutor>();
            PbTransferDecoder* decoder = &executor->get<PbTransferDecoder>();
            *decoder->mutable_field() = executor->get<PbTransferEncodingField>();

            std::vector<PbTransferDecoder> successors;
            BOOST_FOREACH(Unit* child, executor->children()) {
                uint32_t tag = *child->get<TransferTag>();
                if (tag >= successors.size()) {
                    successors.resize(tag + 1);
                }

                if (child->has<IsTerminal>()) {
                    successors[tag] = child->get<PbTransferDecoder>();
                } else {
                    BOOST_FOREACH(Unit* user, child->direct_users()) {
                        successors[tag] = user->father()->get<PbTransferDecoder>();
                    }
                }
            }
            boost::copy(successors, RepeatedFieldBackInserter(decoder->mutable_successor()));

            return false;
        }
    };

    class SetTransferScope : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            Unit::Type unit_type = unit->type();
            Unit::Type unit_father_type = unit->father()->type();
            ExternalExecutor unit_father_external_executor = unit->father()->get<ExternalExecutor>();

            bool ret = unit_type == Unit::CHANNEL
                    && unit_father_type == Unit::EXTERNAL_EXECUTOR
                    && (unit_father_external_executor == SHUFFLE_INPUT);
            return ret;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbScope scope;
            if (unit->has<IsTerminal>()) {
                scope = unit->task()->get<PbScope>();

                CHECK(unit->direct_users().size() >= 1u);

                // FIXME(zhenggonglin): maybe more than one direct_user.
                // each direct_user's shuffle node type should be same.
                std::vector<PbShuffleNode::Type> types;
                BOOST_FOREACH(Unit* user, unit->direct_users()) {
                    if (user->type() == Unit::SHUFFLE_NODE) {
                        const PbLogicalPlanNode& lpn = user->get<PbLogicalPlanNode>();
                        CHECK_EQ(lpn.type(), PbLogicalPlanNode::SHUFFLE_NODE);
                        const PbShuffleNode &sn = lpn.shuffle_node();

                        TransferType tt;
                        (*tt) = sn.type();
                        types.push_back(sn.type());

                        unit->set<TransferType>(tt);
                    }
                }
                for (size_t idx = 1; idx < types.size(); idx++) {
                    CHECK_EQ(types[idx], types[idx - 1]) << "IsTerminal channel "
                        << "has more than one shuffle node and "
                        << "some nodes has different type.";
                }

            } else {
                BOOST_FOREACH(Unit* user, unit->direct_users()) {
                    scope = user->father()->get<PbScope>();
                }
            }
            unit->get<TransferScope>().CopyFrom(scope);

            return false;
        }
    };

    class SetTransferPort : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::CHANNEL && unit->has<IsShuffleInputRelated>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            if (unit->has<IsTerminal>()) {
                *unit->get<TransferPort>() = unit->identity();
            } else {
                BOOST_FOREACH(Unit* user, unit->direct_users()) {
                    CHECK(user->has<TransferPort>());
                    unit->get<TransferPort>() = user->get<TransferPort>();
                }
            }
            return false;
        }
    };

    class BuildTransferEncoderForShuffleInputChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::CHANNEL && unit->has<IsShuffleInputRelated>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbTransferEncoder* encoder = &unit->get<PbTransferEncoder>();
            *encoder->mutable_field() = unit->father()->get<PbTransferEncodingField>();
            encoder->set_tag(*unit->get<TransferTag>());
            encoder->set_tag_encoding(unit->father()->get<TagEncoding>());

            if (unit->has<IsTerminal>()) {
                encoder = encoder->mutable_successor();
                *encoder->mutable_field() = unit->get<PbTransferEncodingField>();
                encoder->set_tag(0);
                encoder->set_tag_encoding(PbTransferEncoder::UNIQUE);
            } else {
                CHECK_EQ(unit->direct_users().size(), 1);
                BOOST_FOREACH(Unit* user, unit->direct_users()) {
                    *encoder->mutable_successor() = user->get<PbTransferEncoder>();
                }
            }

            if (unit->has<IsTerminal>() && unit->has<OrderScopes>()) {
                const OrderScopes& scopes = unit->get<OrderScopes>();
                for (size_t i = 0; i < scopes.size(); ++i) {
                    encoder = encoder->mutable_successor();
                    encoder->set_tag(0);
                    encoder->set_tag_encoding(PbTransferEncoder::UNIQUE);

                    PbTransferEncodingField* field = encoder->mutable_field();
                    field->set_identity(scopes[i].id());
                    if (scopes[i].type() == PbScope::BUCKET) {
                        field->set_type(PbTransferEncodingField::FIXED_LENGTH_KEY);
                        field->set_length(4);
                    } else {
                        field->set_type(PbTransferEncodingField::VARIABLE_LENGTH_KEY);
                    }
                }
            }

            return false;
        }
    };

    class BuildCreateEmptyRecordExecutor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::CREATE_EMPTY_RECORD_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            if (unit->has<PbExecutor>()
                && unit->get<PbExecutor>().type() == PbExecutor::CREATE_EMPTY_RECORD) {
                return false;
            }
            PbExecutor& executor = unit->get<PbExecutor>();
            executor.set_type(PbExecutor::CREATE_EMPTY_RECORD);
            PbCreateEmptyRecordExecutor* msg = executor.mutable_create_empty_record_executor();
            CHECK_EQ(1u, unit->children().size());
            Unit* child = unit->children()[0];
            msg->set_output(child->identity());
            return true;
        }
    };

    class BuildFilterEmptyRecordExecutor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::FILTER_EMPTY_RECORD_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            if (unit->has<PbExecutor>()
                && unit->get<PbExecutor>().type() == PbExecutor::FILTER_EMPTY_RECORD) {
                return false;
            }
            PbExecutor& executor = unit->get<PbExecutor>();
            executor.set_type(PbExecutor::FILTER_EMPTY_RECORD);
            PbFilterEmptyRecordExecutor* msg = executor.mutable_filter_empty_record_executor();
            CHECK_EQ(1u, unit->children().size());
            Unit* child = unit->children()[0];
            msg->set_output(child->identity());
            return true;
        }
    };
};

bool BuildTransferExecutorPass::TransferAnalysis::Run(Plan* plan) {
    return Impl::Run(plan);
}


}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

