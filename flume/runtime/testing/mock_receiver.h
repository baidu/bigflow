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
// Author: Wang Song <wangsong06@baidu.com>
//

#ifndef FLUME_RUNTIME_TESTING_MOCK_RECEIVER_H_
#define FLUME_RUNTIME_TESTING_MOCK_RECEIVER_H_

#include <map>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/iterator.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

template<typename T>
class MockReceiver : public Executor {
public:
    MockReceiver() {}

    MOCK_METHOD2(GetSource, Source* (const std::string&, unsigned));    // NOLINT
    MOCK_METHOD1(Begin, void (const std::string& key));                 // NOLINT
    MOCK_METHOD0(Finish, void ());                                      // NOLINT
    MOCK_METHOD2_T(Got, void (int, const T&));                          // NOLINT
    MOCK_METHOD1(GotDone, void (int));                                  // NOLINT

    MockReceiver& Listen(const std::string& source) {
        m_sources.push_back(source);
        return *this;
    }

    virtual void Setup(const std::map<std::string, Source*>& sources) {
        for (size_t i = 0; i < m_sources.size(); ++i) {
            std::map<std::string, Source*>::const_iterator ptr = sources.find(m_sources[i]);
            ASSERT_TRUE(ptr != sources.end());
            ptr->second->RequireStream(Source::REQUIRE_OBJECT,
                    toft::NewPermanentClosure(this, &MockReceiver::ReceiveObject, i),
                    toft::NewPermanentClosure(this, &MockReceiver::GotDone, i)
            );
        }
    }

    virtual void BeginGroup(const toft::StringPiece& key) {
        Begin(key.as_string());
    }

    virtual void FinishGroup() {
        Finish();
    }

    std::vector<T> GetResults() {
        return m_results;
    }

private:
    void ReceiveObject(int index, const std::vector<toft::StringPiece>& keys,
                       void* object, const toft::StringPiece& binary) {
        Got(index, *static_cast<T*>(object));
        m_results.push_back(*static_cast<T*>(object));
    }

private:
    std::vector<std::string> m_sources;
    std::vector<T> m_results;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu


#endif // FLUME_RUNTIME_TESTING_MOCK_RECEIVER_H_
