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

#ifndef FLUME_RUNTIME_TESTING_MOCK_EXECUTOR_CONTEXT_H_
#define FLUME_RUNTIME_TESTING_MOCK_EXECUTOR_CONTEXT_H_

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/string/string_piece.h"

#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

class MockExecutorContext : public ExecutorContext {
public:
    MOCK_METHOD1(input, Source* (const std::string&));
    MOCK_METHOD1(output, Dispatcher* (const std::string&));

    MOCK_METHOD1(close_prior_outputs, void (int priority));
    MOCK_METHOD1(begin_sub_group, void (const std::string&));
    MOCK_METHOD0(end_sub_group, void ());

    virtual PriorityDispatcher priority_output(const std::string& identity) {
        return PriorityDispatcher();
    }

private:
    virtual void begin_sub_group(const toft::StringPiece& key) {
        this->begin_sub_group(key.as_string());
    }
};

std::vector<toft::StringPiece> as_keys(const char* k0 = NULL, const char* k1 = NULL,
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

std::vector<toft::StringPiece> as_keys(const toft::StringPiece& k0,
                                       const toft::StringPiece& k1 = toft::StringPiece(),
                                       const toft::StringPiece& k2 = toft::StringPiece(),
                                       const toft::StringPiece& k3 = toft::StringPiece()) {
    std::vector<toft::StringPiece> results;

    const toft::StringPiece keys[] = {k0, k1, k2, k3};
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(keys); ++i) {
        if (keys[i].data() != NULL) {
            results.push_back(keys[i]);
        }
    }

    return results;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_MOCK_EXECUTOR_CONTEXT_H_
