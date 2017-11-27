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
// Author: Xue Kang <xuekang@baidu.com>

#ifndef FLUME_RUNTIME_TESTING_STRING_LISTENER_H_
#define FLUME_RUNTIME_TESTING_STRING_LISTENER_H_

#include <cstring>
#include <list>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/iterator.h"
#include "flume/runtime/dispatcher.h"

namespace baidu {
namespace flume {
namespace runtime {

class StringListener {
public:
    StringListener() {
        using ::testing::AnyNumber;
        using ::testing::Invoke;

        ON_CALL(*this, NewDoneCallback())
            .WillByDefault(Invoke(this, &StringListener::NewDefaultDoneCallback));
        EXPECT_CALL(*this, NewDoneCallback()).Times(AnyNumber());
    }

    MOCK_METHOD1(GotObject, void(std::string object));
    MOCK_METHOD1(GotBinary, void(const std::string&));
    MOCK_METHOD2(GotObjectAndBinary, void(std::string, const std::string&));
    MOCK_METHOD2(GotKeyValue, void(const std::vector<std::string>&, const std::string&));
    MOCK_METHOD0(GotDone, void());

    MOCK_METHOD1(GotIterator, void(const std::list<std::string>&));
    MOCK_METHOD0(NewDoneCallback, Source::DoneCallback*());

    Source::Handle* RequireObject(Source* source) {
        return source->RequireStream(Source::REQUIRE_OBJECT,
                toft::NewPermanentClosure(this, &StringListener::ReceiveObject),
                NewDoneCallback()
        );
    }

    Source::Handle* RequireBinary(Source* source) {
        return source->RequireStream(Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(this, &StringListener::ReceiveBinary),
                NewDoneCallback()
        );
    }

    Source::Handle* RequireObjectAndBinary(Source* source) {
        return source->RequireStream(Source::REQUIRE_OBJECT | Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(this, &StringListener::ReceiveObjectAndBinary),
                NewDoneCallback()
        );
    }

    Source::Handle* RequireKeyValue(Source* source) {
        return source->RequireStream(Source::REQUIRE_KEY | Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(this, &StringListener::ReceiveKeyValue),
                NewDoneCallback()
        );
    }

    Source::Handle* RequireIterator(Source* source) {
        return source->RequireIterator(
                toft::NewPermanentClosure(this, &StringListener::ReceiveIterator),
                NewDoneCallback()
        );
    }

private:
    static std::string& ToString(void* object) {
        return *static_cast<std::string*>(object);
    }

    static std::string ToString(const toft::StringPiece& binary) {
        return binary.as_string();
    }

    static std::vector<std::string> ToString(const std::vector<toft::StringPiece>& keys) {
        std::vector<std::string> results;
        for (size_t i = 0; i < keys.size(); ++i) {
            results.push_back(keys[i].as_string());
        }
        return results;
    }

    void ReceiveObject(const std::vector<toft::StringPiece>& keys,
                       void* object, const toft::StringPiece& binary) {
        GotObject(ToString(object));
    }

    void ReceiveBinary(const std::vector<toft::StringPiece>& keys,
                       void* object, const toft::StringPiece& binary) {
        GotBinary(ToString(binary));
    }

    void ReceiveObjectAndBinary(const std::vector<toft::StringPiece>& keys,
                                void* object, const toft::StringPiece& binary) {
        GotObjectAndBinary(ToString(object), ToString(binary));
    }

    void ReceiveKeyValue(const std::vector<toft::StringPiece>& keys,
                         void* object, const toft::StringPiece& binary) {
        GotKeyValue(ToString(keys), ToString(binary));
    }

    void ReceiveIterator(core::Iterator* iterator) {
        std::list<std::string> list;
        while (iterator->HasNext()) {
            list.push_back(ToString(iterator->NextValue()));
        }
        iterator->Done();

        GotIterator(list);
    }

    Source::DoneCallback* NewDefaultDoneCallback() {
        return toft::NewPermanentClosure(this, &StringListener::GotDone);
    }

};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_STRING_LISTENER_H_
