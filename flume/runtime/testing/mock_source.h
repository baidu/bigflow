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
//
// Interface.

#ifndef FLUME_RUNTIME_TESTING_MOCK_SOURCE_H_
#define FLUME_RUNTIME_TESTING_MOCK_SOURCE_H_

#include <string>
#include <vector>

#include "boost/scoped_array.hpp"
#include "boost/scoped_ptr.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/runtime/dispatcher.h"

namespace baidu {
namespace flume {
namespace runtime {

class MockIterator : public core::Iterator {
public:
    int begin() { return m_begin; }
    int end() { return m_end; }

    void SetRange(int begin, int end) {
        m_next = m_begin = begin;
        m_end = end;
    }

    virtual bool HasNext() const {
        return m_next <= m_end;
    }

    virtual void* NextValue() {
        return reinterpret_cast<void*>(m_next++);
    }

    virtual void Reset() {
        m_next = m_begin;
    }

    MOCK_METHOD0(Done, void ());  // NOLINT

private:
    int m_begin;
    int m_end;
    int m_next;
};

class MockSource : public Source, public Source::Handle {
public:
    explicit MockSource(core::Objector* objector = NULL) :
            kEmptyObject(NULL), m_flag(0), m_objector(objector) {
        using ::testing::_;
        using ::testing::AtLeast;
        using ::testing::Invoke;

        ON_CALL(*this, RequireStream(_, _, _))
            .WillByDefault(Invoke(this, &MockSource::DelegateRequireStream));
        EXPECT_CALL(*this, RequireStream(_, _, _)).Times(AtLeast(0));

        ON_CALL(*this, RequireIterator(_, _))
            .WillByDefault(Invoke(this, &MockSource::DelegateRequireIterator));

        ON_CALL(m_iterator, Done())
            .WillByDefault(Invoke(this, &MockSource::IteratorDone));
        EXPECT_CALL(m_iterator, Done()).Times(AtLeast(0));
    }

    void SetObjector(core::Objector* objector) {
        m_objector.reset(objector);
    }

    // for Source
    MOCK_METHOD3(RequireStream,
                 Source::Handle*(uint32_t, StreamCallback* callback, DoneCallback* done));

    MOCK_METHOD2(RequireIterator,
                 Source::Handle*(IteratorCallback* iterator_callback,
                                 DoneCallback* done));

    MOCK_METHOD2(RequireObject, void(StreamCallback* callback, DoneCallback* done));

    MOCK_METHOD2(RequireBinary, void(StreamCallback* callback, DoneCallback* done));

    MOCK_METHOD2(RequireObjectAndBinary, void(StreamCallback* callback, DoneCallback* done));

    // for Handle
    MOCK_METHOD0(Done, void());

    // for dispatched iterator
    MOCK_METHOD0(IteratorDone, void());  // NOLINT

    // dispatch methods

    template<typename T>
    void Dispatch(const T& value) {
        ASSERT_TRUE(m_stream_callback && m_done_callback);

        void* object = const_cast<T*>(&value);

        toft::StringPiece binary;
        boost::scoped_array<char> buffer;
        if (m_flag & Source::REQUIRE_BINARY) {
            size_t buffer_size = m_objector->Serialize(object, NULL, 0);
            buffer.reset(new char[buffer_size]);
            binary.set(buffer.get(), m_objector->Serialize(object, buffer.get(), buffer_size));
        }

        DispatchStream(kEmptyKeys, object, binary);
    }

    void DispatchStream(const std::vector<toft::StringPiece>& keys,
                        void* object, const toft::StringPiece& binary) {
        ASSERT_TRUE(m_stream_callback.get() != NULL);
        m_stream_callback->Run(keys, object, binary);
    }

    void DispatchObject(const void* object) {
        ASSERT_TRUE(m_stream_callback != NULL);
        m_stream_callback->Run(kEmptyKeys, const_cast<void*>(object), kEmptyBinary);
    }

    void DispatchBinary(const toft::StringPiece& binary) {
        ASSERT_TRUE(m_stream_callback != NULL);
        m_stream_callback->Run(kEmptyKeys, kEmptyObject, binary);
    }

    void DispatchObjectAndBinary(const void* object, const toft::StringPiece& binary) {
        ASSERT_TRUE(m_stream_callback != NULL);
        m_stream_callback->Run(kEmptyKeys, const_cast<void*>(object), binary);
    }

    void DispatchIterator(int begin, int end) {
        ASSERT_TRUE(m_iterator_callback != NULL);
        m_iterator.SetRange(begin, end);
        m_iterator_callback->Run(&m_iterator);
    }

    void DispatchIterator(core::Iterator* iterator) {
        ASSERT_TRUE(m_iterator_callback != NULL);
        m_iterator_callback->Run(iterator);
    }

    void DispatchDone() {
        if (m_done_callback != NULL) {
            m_done_callback->Run();
        }
    }

private:
    Source::Handle* DelegateRequireStream(uint32_t flag,
                                          StreamCallback* callback, DoneCallback* done) {
        m_flag = flag;
        m_stream_callback.reset(callback);
        m_done_callback.reset(done);

        if (m_flag & Source::REQUIRE_KEY) {
        } else if ((m_flag & Source::REQUIRE_OBJECT) && (m_flag & Source::REQUIRE_BINARY)) {
            RequireObjectAndBinary(callback, done);
        } else if (m_flag & Source::REQUIRE_OBJECT) {
            RequireObject(callback, done);
        } else if (m_flag & Source::REQUIRE_BINARY) {
            RequireBinary(callback, done);
        }

        return this;
    }

    Source::Handle* DelegateRequireIterator(IteratorCallback* iterator_callback,
                                            DoneCallback* done) {
        m_flag = 0;
        m_iterator_callback.reset(iterator_callback);
        m_done_callback.reset(done);
        return this;
    }

private:
    const std::vector<toft::StringPiece> kEmptyKeys;
    void* const kEmptyObject;
    const toft::StringPiece kEmptyBinary;

    uint32_t m_flag;
    MockIterator m_iterator;

    boost::scoped_ptr<core::Objector> m_objector;
    boost::scoped_ptr<StreamCallback> m_stream_callback;
    boost::scoped_ptr<DoneCallback> m_done_callback;
    boost::scoped_ptr<IteratorCallback> m_iterator_callback;
};


MATCHER_P2(IterateOver, begin, end, "") {
    MockIterator* iterator = dynamic_cast<MockIterator*>(arg);  // NOLINT(runtime/rtti)
    if (iterator == NULL) {
        return false;
    }
    return iterator->begin() == begin && iterator->end() == end;
}

MATCHER(IsEmptyIterator, "") {
    core::Iterator* iterator = arg;
    return iterator->HasNext() == false;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_MOCK_SOURCE_H_
