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
// Author: Wen Xiang <wenxiang@baidu.com>

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/core/loader.h"
#include "flume/proto/split.pb.h"  // flume/proto/split.proto
#include "flume/proto/custom_split.pb.h"

namespace baidu {
namespace flume {
namespace core {

TEST(SplitTest, EncodeSplit) {
    PbSplit message = Loader::EncodeSplit("raw_split");
    message.CheckInitialized();
    EXPECT_EQ(0, message.location_size());
    EXPECT_EQ(false, message.has_length());
    EXPECT_EQ("raw_split", message.raw_split());
}

TEST(SplitTest, DecodeRawSplit) {
    PbSplit message = Loader::DecodeSplit("raw_split");
    message.CheckInitialized();
    EXPECT_EQ(0, message.location_size());
    EXPECT_EQ(false, message.has_length());
    EXPECT_EQ("raw_split", message.raw_split());
}

TEST(SplitTest, DecodeMessage) {
    PbCustomSplit custom_split;
    custom_split.set_uri("file://test.txt");
    custom_split.set_offset(1024);
    PbSplit split;
    split.set_magic(PbSplit::FLUME);
    split.add_location("127.0.0.1");
    split.set_length(4096);
    *split.MutableExtension(PbCustomSplit::ext) = custom_split;

    PbSplit message = Loader::DecodeSplit(split.SerializeAsString());
    EXPECT_EQ(1, message.location_size());
    EXPECT_EQ("127.0.0.1", message.location(0));
    EXPECT_EQ(4096u, message.length());

    PbCustomSplit custom_message = message.GetExtension(PbCustomSplit::ext);
    EXPECT_EQ("file://test.txt", custom_message.uri());
    EXPECT_EQ(1024u, custom_message.offset());
}

}  // namespace core
}  // namespace flume
}  // namespace baidu
