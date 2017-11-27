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

#include "flume/runtime/common/split_combiner.h"

#include <list>
#include <string>
#include <map>

#include "boost/assign.hpp"
#include "boost/foreach.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/range/algorithm.hpp"
#include "boost/scoped_ptr.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/core/partitioner.h"

namespace baidu {
namespace flume {
namespace runtime {

using namespace ::boost::assign;
using namespace ::testing;

PbCombineSplit get_combine(const PbSplit& split) {
    CHECK(split.HasExtension(PbCombineSplit::combine));
    return split.GetExtension(PbCombineSplit::combine);
}

PbSplit split_of(const std::list<std::string>& locations, uint64_t length = 0) {
    using google::protobuf::RepeatedFieldBackInserter;

    PbSplit split;
    split.set_magic(PbSplit::FLUME);
    if (length != 0) {
        split.set_length(length);
    }
    boost::copy(locations, RepeatedFieldBackInserter(split.mutable_location()));

    return split;
}

TEST(SplitCombiner, Empty) {
    SplitCombiner combiner;

    std::vector<PbSplit> splits;
    combiner.combine(10, &splits);

    EXPECT_THAT(splits, AllOf(
        SizeIs(10),
        Each(AllOf(
            Property(&PbSplit::magic, PbSplit::FLUME),
            Property(&PbSplit::location_size, 0),
            Property(&PbSplit::has_length, 0),
            ResultOf(get_combine,
                Property(&PbCombineSplit::split_size, 0)
            )
        ))
    ));
}

TEST(SplitCombiner, CombineSplitsWithLength) {
    SplitCombiner combiner;
    combiner.add_split(split_of(list_of("c"), 3));
    combiner.add_split(split_of(list_of("b"), 2));
    combiner.add_split(split_of(list_of("a"), 1));
    combiner.add_split(split_of(list_of("d"), 4));

    std::vector<PbSplit> splits;
    combiner.combine(2, &splits);

    EXPECT_THAT(splits, UnorderedElementsAre(
        AllOf(
            Property(&PbSplit::location, ElementsAre("d", "a")),
            Property(&PbSplit::length, 5),
            ResultOf(get_combine,
                Property(&PbCombineSplit::split, ElementsAre(
                    Property(&PbSplit::length, 4),
                    Property(&PbSplit::length, 1)
                ))
            )
        ),
        AllOf(
            Property(&PbSplit::location, ElementsAre("c", "b")),
            Property(&PbSplit::length, 5),
            ResultOf(get_combine,
                Property(&PbCombineSplit::split, ElementsAre(
                    Property(&PbSplit::length, 3),
                    Property(&PbSplit::length, 2)
                ))
            )
        )
    ));
}

TEST(SplitCombiner, CombineSplitsWithoutLength) {
    SplitCombiner combiner;
    combiner.add_split(split_of(list_of("a")));
    combiner.add_split(split_of(list_of("b")));
    combiner.add_split(split_of(list_of("c")));
    combiner.add_split(split_of(list_of("d")));

    std::vector<PbSplit> splits;
    combiner.combine(2, &splits);

    EXPECT_THAT(splits, Each(
        AllOf(
            Property(&PbSplit::location_size, 2),
            Property(&PbSplit::has_length, false),
            ResultOf(get_combine,
                Property(&PbCombineSplit::split_size, 2)
            )
        )
    ));
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
