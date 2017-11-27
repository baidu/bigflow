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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/core/testing/mock_key_reader.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"

namespace baidu {
namespace flume {

using core::Entity;
using core::KeyReader;

using ::testing::Return;

TEST(MockKeyReaderTest, Basic) {
    const char kConfig[] = "test";

    char buffer[16];
    size_t buffer_size = sizeof(buffer);

    int object = 24;
    std::string key = "24";

    MockKeyReader& mock = MockKeyReader::Mock(kConfig);
    mock.KeyOf(&object) = "24";

    Entity<KeyReader> entity = Entity<KeyReader>::Of<MockKeyReader>(kConfig);
    toft::scoped_ptr<KeyReader> key_reader(entity.CreateAndSetup());
    ASSERT_EQ(key.size(), key_reader->ReadKey(&object, buffer, buffer_size));
    ASSERT_EQ(key, std::string(buffer, key.size()));
}

}  // namespace flume
}  // namespace baidu
