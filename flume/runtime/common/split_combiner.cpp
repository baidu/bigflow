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

#include <algorithm>

#include "flume/runtime/common/split_combiner.h"

#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace runtime {

void SplitCombiner::add_split(const PbSplit& split) {
    _splits.push_back(split);
}

struct SplitCombiner::CompareByLength {
    bool operator()(const PbSplit* s0, const PbSplit* s1) {
        return s0->length() > s1->length();
    }
};

void SplitCombiner::combine(uint32_t split_number, std::vector<PbSplit>* result) {
    std::priority_queue<
            PbSplit*, std::vector<PbSplit*>, CompareByLength
    > length_queue;

    result->resize(split_number);
    for (size_t i = 0; i < result->size(); ++i) {
        PbSplit* split = &result->at(i);

        split->Clear();
        split->set_magic(PbSplit::FLUME);
        split->MutableExtension(PbCombineSplit::combine)->clear_split();

        length_queue.push(split);
    }

    std::vector<PbSplit*> sorted_splits;
    sorted_splits.reserve(_splits.size());
    for (size_t i = 0; i < _splits.size(); ++i) {
        sorted_splits.push_back(&_splits[i]);
    }
    std::sort(sorted_splits.begin(), sorted_splits.end(), CompareByLength());

    // add to queue from large to small
    typedef std::vector<PbSplit*>::reverse_iterator Iterator;
    for (size_t i = 0; i < sorted_splits.size(); ++i) {
        const PbSplit& split = *sorted_splits[i];

        if (split.has_length()) {
            PbSplit* target = length_queue.top();
            length_queue.pop();

            merge_into(split, target);
            length_queue.push(target);
        } else {
            merge_into(split, &result->at(i % result->size()));
        }
    }
}

void SplitCombiner::merge_into(const PbSplit& src, PbSplit* dst) {
    using google::protobuf::RepeatedFieldBackInserter;

    if (src.has_length()) {
        dst->set_length(dst->length() + src.length());
    }
    boost::copy(src.location(), RepeatedFieldBackInserter(dst->mutable_location()));

    PbCombineSplit* combine = dst->MutableExtension(PbCombineSplit::combine);
    if (src.HasExtension(PbCombineSplit::combine)) {
        boost::copy(src.GetExtension(PbCombineSplit::combine).split(),
                    RepeatedFieldBackInserter(combine->mutable_split()));
    } else {
        *combine->add_split() = src;
    }
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
