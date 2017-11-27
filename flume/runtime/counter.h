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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//
// Interface for counter.

#ifndef FLUME_RUNTIME_COUNTER_H_
#define FLUME_RUNTIME_COUNTER_H_

#include <stdint.h>

#include <map>
#include <ostream>  // NOLINT(readability/streams)
#include <string>

#include "boost/ptr_container/ptr_map.hpp"
#include "boost/type_traits/is_integral.hpp"
#include "boost/utility/enable_if.hpp"
#include "glog/logging.h"
#include "toft/base/uncopyable.h"
#include "toft/system/threading/mutex.h"

#include "flume/util/reflection.h"

namespace baidu {
namespace flume {
namespace runtime {

class Counter {
public:
    virtual ~Counter() {}

    virtual uint64_t GetValue() const = 0;

    virtual void Update(uint64_t increments) = 0;

    virtual void Reset() = 0;
};

class CounterSession {
    TOFT_DECLARE_UNCOPYABLE(CounterSession);

public:
    static CounterSession* GlobalCounterSession();

    static std::string GenerateCounterKey(const std::string& prefix,
                                          const std::string& name);

    static void GetPrefixFromCounterKey(const std::string& counter_name,
                                        std::string* prefix, std::string* name);

    const static std::string COUNTER_DEFAULT_PREFIX;

public:
    CounterSession();
    virtual ~CounterSession();

    /// GetCounter requires name must be the form of `prefix|name` to get a unified semantic.
    /// Note: prefix can be empty, counters defined in the flume runtime usually get prefix: `Flume`
    /// \param name: counter name, must be prefix|counter_name
    /// \return Counter
    virtual Counter* GetCounter(const std::string& name);

    virtual Counter* GetCounter(const std::string& prefix, const std::string& name);

    virtual std::map<std::string, const Counter*> GetAllCounters() const;

    virtual void ResetAllCounters();

    virtual void Merge(const CounterSession& other);

private:
    static CounterSession* s_global_counter_session;

    class CounterImpl;
    boost::ptr_map<std::string, CounterImpl> m_counters;
    mutable toft::Mutex m_mutex;
};

#define DEFINE_COUNTER(name) \
    namespace name##_cNs { \
        struct Marker {}; \
        ::baidu::flume::runtime::GlobalCounterVariable \
        COUNTER_##name(::baidu::flume::PrettyTypeName<Marker>()); \
    } \
    using name##_cNs::COUNTER_##name

#define DECLARE_COUNTER(name) \
    namespace name##_cNs { \
        extern ::baidu::flume::runtime::GlobalCounterVariable COUNTER_##name; \
    } \
    using name##_cNs::COUNTER_##name


// Do NOT use this class directly, use DEFINE_COUNT and DECLARE_COUNTER instead.
// Provide basic operator overloading such as +=, ++, and <<;
class GlobalCounterVariable {
public:
    explicit GlobalCounterVariable(const std::string& marker) {
        const std::string kMarkerSubfix = "_cNs::Marker";
        m_name = marker.substr(0, marker.find(kMarkerSubfix));
        // Make sure we add the default prefix.
        m_name = CounterSession::GenerateCounterKey(CounterSession::COUNTER_DEFAULT_PREFIX, m_name);
        m_counter = CounterSession::GlobalCounterSession()->GetCounter(m_name);
    }

    operator uint64_t() {
        return m_counter->GetValue();
    }

    template<typename T>
    typename boost::enable_if< boost::is_integral<T>, GlobalCounterVariable&>::type
    operator+=(T increments) {
        if (increments >= 0) {
            m_counter->Update(increments);
        } else {
            LOG_FIRST_N(WARNING, 10) << "Try to add negative value to counter, ignore it!";
        }
        return *this;
    }

    GlobalCounterVariable& operator++() {
        m_counter->Update(1);
        return *this;
    }

    uint64_t operator++(int postfix) {
        uint64_t ret = m_counter->GetValue();
        m_counter->Update(1);
        return ret;
    }

    friend std::ostream& operator<<(std::ostream& stream, const GlobalCounterVariable& var) {
        stream << var.m_counter->GetValue();
        return stream;
    }


private:
    std::string m_name;
    Counter* m_counter;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COUNTER_H_
