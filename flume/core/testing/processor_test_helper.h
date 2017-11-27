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
// This file declares ProcessorTestHelper, a testing facility to test custom Processor.

#ifndef FLUME_CORE_TESTING_PROCESSOR_TEST_HELPER_H_
#define FLUME_CORE_TESTING_PROCESSOR_TEST_HELPER_H_

#include <list>
#include <map>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"
#include "flume/core/iterator.h"
#include "flume/core/processor.h"

namespace baidu {
namespace flume {
namespace core {

class ProcessorTestHelper {
public:
    ProcessorTestHelper(const Entity<Processor>& processor, Emitter* emitter);
    ProcessorTestHelper(Processor* processor, Emitter* emitter);

    // setup parameters for Processor::BeginGroup. After calling EndGroup method, all
    // these paramters will be reset.
    void AddKey(const std::string& key);
    void AddPreparedInput(uint32_t index, void* object);

    void BeginGroup(uint32_t inputs_number);
    void Process(uint32_t index, void* object);
    void EndGroup();

private:
    class IteratorImpl : public core::Iterator {
    public:
        typedef std::vector<void*>::iterator InputIterator;

        IteratorImpl(InputIterator begin, InputIterator end)
                : m_begin(begin), m_end(end) {
            Reset();
        }

        virtual bool HasNext() const {
            return m_ptr != m_end;
        }

        virtual void* NextValue() {
            return *(m_ptr++);
        }

        virtual void Reset() {
            m_ptr = m_begin;
        }

        virtual void Done() {}

    private:
        InputIterator m_begin;
        InputIterator m_end;
        InputIterator m_ptr;
    };

private:
    toft::scoped_ptr<Processor> m_processor_deleter;

    Processor* m_processor;
    Emitter* m_emitter;

    std::vector<std::string> m_keys;
    std::map<uint32_t, std::vector<void*> > m_inputs;
    boost::ptr_vector<IteratorImpl> m_inputs_deleter;
};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_PROCESSOR_TEST_HELPER_H_
