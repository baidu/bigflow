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
// Author: Wen Xiang <wenxiang@baidu.com>
//

#ifndef FLUME_RUNTIME_TESTING_MOCK_EXECUTOR_BASE_H_
#define FLUME_RUNTIME_TESTING_MOCK_EXECUTOR_BASE_H_

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/string/string_piece.h"

#include "flume/runtime/common/executor_base.h"

namespace baidu {
namespace flume {
namespace runtime {

class MockExecutorBase : public ExecutorBase {
public:
    MOCK_METHOD1(GetInput, Source* (const std::string&));
    MOCK_METHOD1(GetOutput, Dispatcher* (const std::string&));


    MOCK_METHOD1(BeginSubGroup, void (const std::string&));
    MOCK_METHOD0(EndSubGroup, void ());

private:
    virtual void BeginSubGroup(const toft::StringPiece& key) {
        this->BeginSubGroup(key.as_string());
    }
};

std::vector<toft::StringPiece> MakeKeys(const char* k0 = NULL, const char* k1 = NULL,
                                        const char* k2 = NULL, const char* k3 = NULL) {
    std::vector<toft::StringPiece> results;

    const char* keys[] = {k0, k1, k2, k3};
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(keys); ++i) {
        if (keys[i] != NULL) {
            results.push_back(keys[i]);
        }
    }

    return results;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_MOCK_EXECUTOR_BASE_H_
