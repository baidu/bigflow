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

#include "flume/runtime/counter.h"

#include <string>

#include "boost/atomic.hpp"

namespace baidu {
namespace flume {
namespace runtime {

class CounterSession::CounterImpl : public Counter {
public:
    CounterImpl() : m_value(0) {}
    virtual ~CounterImpl() {}

    virtual uint64_t GetValue() const {
        return m_value;
    }

    virtual void Update(uint64_t increments) {
        m_value += increments;
    }

    virtual void Reset() {
        m_value = 0;
    }

private:
    boost::atomic<uint64_t> m_value;
};

const std::string CounterSession::COUNTER_DEFAULT_PREFIX = "Flume";

CounterSession::CounterSession() {
}

CounterSession::~CounterSession() {
}

Counter* CounterSession::GetCounter(const std::string& name) {
    size_t pos = name.find("|");
    CHECK_NE(pos, std::string::npos) << "No group found in the counter_key: " << name;
    toft::MutexLocker lock(&m_mutex);
    return &m_counters[name];
}

Counter* CounterSession::GetCounter(const std::string& prefix, const std::string& name) {
    toft::MutexLocker lock(&m_mutex);
    return &m_counters[GenerateCounterKey(prefix, name)];
}

std::map<std::string, const Counter*> CounterSession::GetAllCounters() const {
    typedef boost::ptr_map<std::string, CounterImpl>::const_iterator Iterator;

    toft::MutexLocker lock(&m_mutex);

    std::map<std::string, const Counter*> result;
    for (Iterator ptr = m_counters.begin(); ptr != m_counters.end(); ++ptr) {
        result[ptr->first] = ptr->second;
    }
    return result;
}

void CounterSession::ResetAllCounters() {
    toft::MutexLocker lock(&m_mutex);

    typedef boost::ptr_map<std::string, CounterImpl>::iterator Iterator;
    for (Iterator ptr = m_counters.begin(); ptr != m_counters.end(); ++ptr) {
        ptr->second->Reset();
    }
}

void CounterSession::Merge(const CounterSession& other) {
    std::map<std::string, const Counter*> counters = other.GetAllCounters();

    typedef std::map<std::string, const Counter*>::const_iterator Iterator;
    for (Iterator ptr = counters.begin(); ptr != counters.end(); ++ptr) {
        uint64_t value = ptr->second->GetValue();
        if (value != 0) {
            toft::MutexLocker lock(&m_mutex);
            m_counters[ptr->first].Update(value);
        }
    }
}

CounterSession* CounterSession::s_global_counter_session = NULL;

CounterSession* CounterSession::GlobalCounterSession() {
    if (s_global_counter_session == NULL) {
        s_global_counter_session = new CounterSession();
    }
    return s_global_counter_session;
}

std::string CounterSession::GenerateCounterKey(const std::string& prefix,
                                               const std::string& name) {
    return prefix + "|" + name;
}

void CounterSession::GetPrefixFromCounterKey(const std::string& counter_name,
                                             std::string* prefix, std::string* name) {
    size_t pos = counter_name.find("|");
    if (pos == std::string::npos) {
        *prefix = COUNTER_DEFAULT_PREFIX;
        *name = counter_name;
        return;
    }
    *prefix = counter_name.substr(0, pos);
    *name = counter_name.substr(pos + 1);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
