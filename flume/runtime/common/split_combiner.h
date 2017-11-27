/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//

#ifndef FLUME_RUNTIME_COMMON_SPLIT_COMBINER_H_
#define FLUME_RUNTIME_COMMON_SPLIT_COMBINER_H_

#include <queue>
#include <vector>

#include "boost/range/algorithm.hpp"

#include "flume/proto/split.pb.h"

namespace baidu {
namespace flume {
namespace runtime {

class SplitCombiner {
public:
    void add_split(const PbSplit& split);

    void combine(uint32_t split_number, std::vector<PbSplit>* result);

private:
    struct CompareByLength;

    void merge_into(const PbSplit& src, PbSplit* dst);

    std::vector<PbSplit> _splits;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_COMMON_SPLIT_COMBINER_H_
