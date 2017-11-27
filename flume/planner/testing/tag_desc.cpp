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
// Author: Pan Yuchang(BDG)<bigflow-opensource@baidu.com>
//         Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/planner/testing/tag_desc.h"

#include <algorithm>
#include <flume/planner/common/draw_plan_pass.h>

#include "boost/foreach.hpp"
#include "boost/range/algorithm.hpp"

#include "flume/proto/logical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

class UnitTypeImpl : public TagDesc {
public:
    explicit UnitTypeImpl(Unit::Type type) : m_type(type) {}

    virtual void Set(Unit* unit) {
        unit->set_type(m_type);
    }

    virtual bool Test(Unit* unit) {
        return unit->type() == m_type;
    }

private:
    Unit::Type m_type;
};

TagDescRef UnitType(Unit::Type type) {
    return TagDescRef(new UnitTypeImpl(type));
}

class PbScopeTagImpl : public TagDesc {
public:
    explicit PbScopeTagImpl(PbScope::Type type) {
        InitMessage(type);
    }

    PbScopeTagImpl(PbScope::Type type, int concurrency) {
        InitMessage(type);
        m_message.set_concurrency(concurrency);
    }

    void InitMessage(PbScope::Type type) {
        // only fill required fields
        m_message.set_type(type);
        switch (type) {
            case PbScope::DEFAULT: {
                break;
            }
            case PbScope::INPUT: {
                PbInputScope* input = m_message.mutable_input_scope();
                input->mutable_spliter()->set_name("TestLoader");
                input->mutable_spliter()->set_config("test-loader-config");
                break;
            }
            case PbScope::GROUP: {
                m_message.mutable_group_scope();
                break;
            }
            case PbScope::BUCKET: {
                m_message.mutable_bucket_scope();
                break;
            }
            case PbScope::WINDOW: {
                break;
            }
            // no default here. So if a new Type value is added, compiler will tell us
        }
    }

    void Set(Unit* unit) {
        PbScope& message = unit->get<PbScope>();
        message.MergeFrom(m_message);
        message.set_id(unit->identity());
        std::string expect_message = "Expect: <PbScope>.type == " + PbScope_Type_Name(message.type());
        DrawPlanPass::UpdateLabel(unit, "312-expect-pbscope", expect_message);
    }

    bool Test(Unit* unit) {
        if (!unit->has<PbScope>()) {
            return false;
        }
        PbScope& message = unit->get<PbScope>();

        if (message.type() != m_message.type()) {
            return false;
        }

        return true;
    }

private:
    PbScope m_message;
};

TagDescRef PbScopeTag(PbScope::Type type) {
    return TagDescRef(new PbScopeTagImpl(type));
}

TagDescRef PbScopeTag(PbScope::Type type, int concurrency) {
    return TagDescRef(new PbScopeTagImpl(type, concurrency));
}

class PbScopeSortedTagImpl : public TagDesc {
public:
    explicit PbScopeSortedTagImpl(bool is_sorted) : m_is_sorted(is_sorted) {}

    void Set(Unit* unit) {
        PbScope& message = unit->get<PbScope>();
        message.set_is_sorted(m_is_sorted);
    }

    bool Test(Unit* unit) {
        return true;
    }

private:
    int m_is_sorted;
};


TagDescRef PbScopeSortedTag(bool is_sorted) {
    return TagDescRef(new PbScopeSortedTagImpl(is_sorted));
}

class PbScopeStreamTagImpl : public TagDesc {
public:
    explicit PbScopeStreamTagImpl(bool is_stream) : _is_stream(is_stream) {}

    void Set(Unit* unit) {
        PbScope& message = unit->get<PbScope>();
        message.set_is_stream(_is_stream);
    }

    bool Test(Unit* unit) {
        return true;
    }

private:
    bool _is_stream;
};

TagDescRef PbScopeStreamTag(bool is_stream) {
    return TagDescRef(new PbScopeStreamTagImpl(is_stream));
}

class PbNodeTagImpl : public TagDesc {
public:
    explicit PbNodeTagImpl(PbLogicalPlanNode::Type type) {
        // only fill required fields
        m_message.set_type(type);
        m_message.mutable_objector()->set_name("TestObjector");
        m_message.mutable_objector()->set_config("test-objector-config");
        switch (type) {
            case PbLogicalPlanNode::UNION_NODE: {
                m_message.mutable_union_node();
                break;
            }
            case PbLogicalPlanNode::LOAD_NODE: {
                PbEntity* loader = m_message.mutable_load_node()->mutable_loader();
                loader->set_name("TestLoader");
                loader->set_config("test-loader-config");
                break;
            }
            case PbLogicalPlanNode::SINK_NODE: {
                PbEntity* sinker = m_message.mutable_sink_node()->mutable_sinker();
                sinker->set_name("TestSinker");
                sinker->set_config("test-sinker-config");
                break;
            }
            case PbLogicalPlanNode::PROCESS_NODE: {
                PbEntity* processor = m_message.mutable_process_node()->mutable_processor();
                processor->set_name("TestProcessor");
                processor->set_config("test-processor-config");
                break;
            }
            case PbLogicalPlanNode::SHUFFLE_NODE: {
                m_message.mutable_shuffle_node()->set_type(PbShuffleNode::BROADCAST);
                break;
            }
            // no default here. So if a new Type value is added, compiler will tell us
        }
    }

    virtual void Set(Unit* unit) {
        std::set<std::string> froms;
        BOOST_FOREACH(Unit* need, unit->direct_needs()) {
            froms.insert(need->identity());
        }

        PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
        message.MergeFrom(m_message);
        message.set_id(unit->identity());
        switch (message.type()) {
            case PbLogicalPlanNode::UNION_NODE: {
                using google::protobuf::RepeatedFieldBackInserter;
                std::copy(froms.begin(), froms.end(),
                          RepeatedFieldBackInserter(message.mutable_union_node()->mutable_from()));
                break;
            }
            case PbLogicalPlanNode::LOAD_NODE: {
                CHECK_EQ(froms.size(), 0) << "LOAD_NODE should has no from";
                break;
            }
            case PbLogicalPlanNode::SINK_NODE: {
                CHECK_EQ(froms.size(), 1) << "SINK_NODE should has only one from";
                message.mutable_sink_node()->set_from(*froms.begin());
                break;
            }
            case PbLogicalPlanNode::PROCESS_NODE: {
                PbProcessNode* node = message.mutable_process_node();
                BOOST_FOREACH(std::string from, froms) {
                    node->add_input()->set_from(from);
                }
                break;
            }
            case PbLogicalPlanNode::SHUFFLE_NODE: {
                CHECK_EQ(froms.size(), 1) << "SHUFFLE_NODE should has only one from";
                message.mutable_shuffle_node()->set_from(*froms.begin());
                break;
            }
        }
    }

    virtual bool Test(Unit* unit) {
        if (!unit->has<PbLogicalPlanNode>()) {
            return false;
        }

        const PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
        if (message.type() != m_message.type()) {
            return false;
        }

        std::set<std::string> froms, expected_froms;
        BOOST_FOREACH(Unit* need, unit->direct_needs()) {
            froms.insert(need->identity());
        }

        switch (message.type()) {
            case PbLogicalPlanNode::UNION_NODE: {
                if (!message.has_union_node()) {
                    return false;
                }
                const PbUnionNode& node = message.union_node();
                expected_froms.insert(node.from().begin(), node.from().end());
                break;
            }
            case PbLogicalPlanNode::LOAD_NODE: {
                if (!message.has_load_node()) {
                    return false;
                }
                break;
            }
            case PbLogicalPlanNode::SINK_NODE: {
                if (!message.has_sink_node()) {
                    return false;
                }
                expected_froms.insert(message.sink_node().from());
                break;
            }
            case PbLogicalPlanNode::PROCESS_NODE: {
                if (!message.has_process_node()) {
                    return false;
                }
                const PbProcessNode& node = unit->get<PbLogicalPlanNode>().process_node();
                for (int i = 0; i < node.input_size(); ++i) {
                    expected_froms.insert(node.input(i).from());
                }
                break;
            }
            case PbLogicalPlanNode::SHUFFLE_NODE: {
                if (!message.has_shuffle_node()) {
                    return false;
                }
                expected_froms.insert(message.shuffle_node().from());
                break;
            }
        }

        return boost::includes(expected_froms, froms);
    }

private:
    PbLogicalPlanNode m_message;
};

TagDescRef PbNodeTag(PbLogicalPlanNode::Type type) {
    return TagDescRef(new PbNodeTagImpl(type));
}

class PbShuffleNodeTagImpl : public TagDesc {
public:
    explicit PbShuffleNodeTagImpl(PbShuffleNode::Type type) {
        m_message.set_type(type);
        if (type == PbShuffleNode::KEY) {
            PbEntity* key_reader = m_message.mutable_key_reader();
            key_reader->set_name("TestKeyReader");
            key_reader->set_config("test-key-reader-config");
        }
    }

    virtual void Set(Unit* unit) {
        CHECK(unit->has<PbLogicalPlanNode>());
        PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
        CHECK_EQ(message.type(), PbLogicalPlanNode::SHUFFLE_NODE);
        message.mutable_shuffle_node()->MergeFrom(m_message);
    }

    virtual bool Test(Unit* unit) {
        if (!unit->has<PbLogicalPlanNode>()) {
            return false;
        }

        const PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
        if (message.type() != PbLogicalPlanNode::SHUFFLE_NODE) {
            return false;
        }

        const PbShuffleNode& sub_message = message.shuffle_node();
        return sub_message.type() == m_message.type();
    }

private:
    PbShuffleNode m_message;
};

TagDescRef PbShuffleNodeTag(PbShuffleNode::Type type) {
    return TagDescRef(new PbShuffleNodeTagImpl(type));
}

class EffectiveKeyNumTagImpl : public TagDesc {
public:
    explicit EffectiveKeyNumTagImpl(int n) : _n(n){
    }

    virtual void Set(Unit* unit) {
        CHECK(unit->has<PbLogicalPlanNode>());
        PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
        CHECK_EQ(message.type(), PbLogicalPlanNode::PROCESS_NODE);
        message.mutable_process_node()->set_effective_key_num(_n);
    }

    virtual bool Test(Unit* unit) {
        if (!unit->has<PbLogicalPlanNode>()) {
            return false;
        }

        const PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
        if (message.type() != PbLogicalPlanNode::PROCESS_NODE) {
            return false;
        }

        return message.process_node().effective_key_num() == _n;
    }

private:
    int _n;
};


TagDescRef EffectiveKeyNum(int n) {
    return TagDescRef(new EffectiveKeyNumTagImpl(n));
}


class ScopeLevelTagImpl : public TagDesc {
public:
    explicit ScopeLevelTagImpl(int n) : _n(n){
    }

    virtual void Set(Unit* unit) {
        unit->get<ScopeLevel>().level = _n;
    }

    virtual bool Test(Unit* unit) {
        return unit->has<ScopeLevel>() && unit->get<ScopeLevel>().level == _n;
    }

private:
    int _n;
};


TagDescRef ScopeLevelTag(int n) {
    return TagDescRef(new ScopeLevelTagImpl(n));
}

class StageTagImpl : public TagDesc {
public:
    explicit StageTagImpl(OptimizingStage stage) : _stage(stage){
    }

    virtual void Set(Unit* unit) {
        unit->plan()->Root()->get<OptimizingStage>() = _stage;
    }

    virtual bool Test(Unit* unit) {
        Unit* root = unit->plan()->Root();
        return root->has<OptimizingStage>() && root->get<OptimizingStage>() == _stage;
    }

private:
    OptimizingStage _stage;
};


TagDescRef Stage(OptimizingStage stage) {
    return TagDescRef(new StageTagImpl(stage));
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

