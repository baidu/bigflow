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

#ifndef FLUME_RUNTIME_TESTING_MOCK_DISPATCHER_H_
#define FLUME_RUNTIME_TESTING_MOCK_DISPATCHER_H_

#include <list>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/runtime/dataset.h"
#include "flume/runtime/dispatcher.h"

namespace baidu {
namespace flume {
namespace runtime {

class MockDispatcher : public Dispatcher {
public:
    MockDispatcher() : m_is_accept_more(true) {
        using ::testing::_;
        using ::testing::AnyNumber;
        using ::testing::Return;
        using ::testing::InvokeWithoutArgs;

        EXPECT_CALL(*this, SetObjector(_)).Times(AnyNumber());

        ON_CALL(*this, IsAcceptMore())
            .WillByDefault(InvokeWithoutArgs(this, &MockDispatcher::DelegateIsAcceptMore));
        EXPECT_CALL(*this, IsAcceptMore()).Times(AnyNumber());

        ON_CALL(*this, EmitObject(_))
            .WillByDefault(InvokeWithoutArgs(this, &MockDispatcher::DelegateIsAcceptMore));
        ON_CALL(*this, EmitBinary(_))
            .WillByDefault(InvokeWithoutArgs(this, &MockDispatcher::DelegateIsAcceptMore));

        ON_CALL(*this, EmitEmpty())
            .WillByDefault(InvokeWithoutArgs(this, &MockDispatcher::DelegateIsAcceptMore));
    }

    MOCK_METHOD1(SetDatasetManager, void(DatasetManager*));
    MOCK_METHOD1(SetObjector, void(const PbEntity&));
    MOCK_METHOD1(GetSource, Source*(uint32_t));

    MOCK_METHOD1(BeginGroup, void(const toft::StringPiece&));
    MOCK_METHOD2(BeginGroup, void(const toft::StringPiece&, Dataset*));
    MOCK_METHOD0(FinishGroup, void());

    MOCK_METHOD0(GetScopeLevel, uint32_t());
    MOCK_METHOD1(GetDataset, Dataset*(uint32_t));
    MOCK_METHOD0(IsAcceptMore, bool());

    MOCK_METHOD1(EmitObject, bool(void*));
    MOCK_METHOD2(EmitObject, bool(const std::vector<toft::StringPiece>&, void*));

    MOCK_METHOD1(EmitBinary, bool(const std::string&));
    MOCK_METHOD2(EmitBinary, bool(const std::vector<toft::StringPiece>&, const std::string&));

    MOCK_METHOD0(Done, void ());  // NOLINT

    MOCK_METHOD0(EmitEmpty, bool ()); // NOLINT

    bool AcceptMore() {
        m_is_accept_more = true;
        return true;
    }

    bool DoNotAcceptMore() {
        m_is_accept_more = false;
        return false;
    }

private:
    virtual bool EmitBinary(const toft::StringPiece& binary) {
        if (binary.data() == NULL) {
            return EmitEmpty();
        }
        return EmitBinary(binary.as_string());
    }

    virtual bool EmitBinary(const std::vector<toft::StringPiece>& keys,
                            const toft::StringPiece& binary) {
        return EmitBinary(keys, binary.as_string());
    }

    bool DelegateIsAcceptMore() {
        return m_is_accept_more;
    }

private:
    bool m_is_accept_more;
};

std::list<std::string> ToList(Dataset* dataset) {
    Dataset::Iterator* iterator = dataset->NewIterator();

    std::list<std::string> result;
    while (iterator->HasNext()) {
        result.push_back(iterator->NextValue().as_string());
    }

    iterator->Done();
    dataset->Release();

    return result;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_MOCK_DISPATCHER_H_
