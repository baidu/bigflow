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

#ifndef FLUME_PLANNER_TESTING_EDGE_DESC_H_
#define FLUME_PLANNER_TESTING_EDGE_DESC_H_

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "boost/make_shared.hpp"
#include "boost/shared_ptr.hpp"
#include "glog/logging.h"

#include "flume/planner/pass.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

class EdgeDesc {
public:
    virtual ~EdgeDesc() {}
    virtual void Set(Unit* from, Unit* to) = 0;
    virtual bool Test(Unit* from, Unit* to) = 0;
};

typedef boost::shared_ptr<EdgeDesc> EdgeDescRef;

EdgeDescRef PreparedEdge();
EdgeDescRef PartialEdge();

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_TESTING_EDGE_DESC_H_
