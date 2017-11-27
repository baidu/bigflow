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

#ifndef FLUME_CORE_TESTING_MOCK_LOADER_H_
#define FLUME_CORE_TESTING_MOCK_LOADER_H_

#include <string>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "gmock/gmock.h"

#include "flume/core/loader.h"
#include "flume/core/testing/mock_base.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace flume {

class MockLoader : public MockBase< ::baidu::flume::core::Loader, MockLoader > {
public:
    MOCK_METHOD2(Split, void(const std::string& uri, std::vector<std::string>*));
    MOCK_METHOD2(Load, void(const std::string& split, ::baidu::flume::core::Emitter*));
    MOCK_METHOD1(Load, void(const std::string& split));

    void Delegate(MockLoader* mock) {
        using ::testing::_;
        using ::testing::AnyNumber;
        using ::testing::Invoke;

        DELEGATE_METHOD2(mock, MockLoader, Split);

        ON_CALL(*this, Load(_, _))
            .WillByDefault(Invoke(mock, &MockLoader::DelegateLoad));
        EXPECT_CALL(*this, Load(_, _)).Times(AnyNumber());
    }

    bool Emit(void *object) {
        return m_emitter->Emit(object);
    }

    void Done() {
        m_emitter->Done();
    }

private:
    void DelegateLoad(const std::string& split, ::baidu::flume::core::Emitter* emitter) {
        m_emitter = emitter;
        Load(split);
    }

    ::baidu::flume::core::Emitter* m_emitter;
};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_LOADER_H_
