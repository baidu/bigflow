/************************************************************************
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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/core/testing/mock_loader.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace flume {

using core::Entity;
using core::Loader;

using ::testing::_;
using ::testing::ElementsAreArray;
using ::testing::Return;
using ::testing::SetArgPointee;

TEST(MockObjectTest, Basic) {
    const char kConfig[] = "test";
    const char kUri[] = "file";
    const char* kSplits[] = {"split1", "split2"};

    MockEmitter<> emitter;
    MockLoader& mock = MockLoader::Mock(kConfig);

    EXPECT_CALL(mock, Split(kUri, _))
        .WillOnce(SetArgPointee<1>(std::vector<std::string>(kSplits,
                                                            kSplits + TOFT_ARRAY_SIZE(kSplits))));
    EXPECT_CALL(mock, Load(kSplits[0]));


    Entity<Loader> entity = Entity<Loader>::Of<MockLoader>(kConfig);
    toft::scoped_ptr<Loader> loader(entity.CreateAndSetup());

    std::vector<std::string> results;
    loader->Split(kUri, &results);
    EXPECT_THAT(results, ElementsAreArray(kSplits));
    loader->Load(results[0], &emitter);
}

}  // namespace flume
}  // namespace baidu
