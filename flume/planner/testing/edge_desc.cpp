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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/testing/edge_desc.h"

#include "flume/proto/logical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

class ProcessInputEdgeDesc : public EdgeDesc {
protected:
    PbProcessNode::Input* SelectProcessInput(Unit* from, Unit* to) {
        if (!to->has<PbLogicalPlanNode>()) {
            return NULL;
        }

        PbLogicalPlanNode& message = to->get<PbLogicalPlanNode>();
        if (message.type() != PbLogicalPlanNode::PROCESS_NODE) {
            return NULL;
        }

        PbProcessNode* node = message.mutable_process_node();
        for (int i = 0; i < node->input_size(); ++i) {
            PbProcessNode::Input* input = node->mutable_input(i);
            if (input->from() == from->identity()) {
                return input;
            }
        }

        return NULL;
    }
};

class PreparedEdgeImpl : public ProcessInputEdgeDesc {
public:
    virtual void Set(Unit* from, Unit* to) {
        PbProcessNode::Input* input = SelectProcessInput(from, to);
        CHECK_NOTNULL(input);

        input->set_is_prepared(true);
    }

    virtual bool Test(Unit* from, Unit* to) {
        PbProcessNode::Input* input = SelectProcessInput(from, to);
        if (input == NULL) {
            return false;
        }

        return input->is_prepared();
    }
};

EdgeDescRef PreparedEdge() {
    return EdgeDescRef(new PreparedEdgeImpl());
}

class PartialEdgeImpl : public ProcessInputEdgeDesc {
public:
    virtual void Set(Unit* from, Unit* to) {
        PbProcessNode::Input* input = SelectProcessInput(from, to);
        CHECK_NOTNULL(input);

        input->set_is_partial(true);
    }

    virtual bool Test(Unit* from, Unit* to) {
        PbProcessNode::Input* input = SelectProcessInput(from, to);
        if (input == NULL) {
            return false;
        }

        return input->is_partial();
    }
};

EdgeDescRef PartialEdge() {
    return EdgeDescRef(new PartialEdgeImpl());
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

