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
// Author: Liu Cheng <liucheng02@baidu.com>
//
// Converts cpp typed values to a lexicographically ordered string, such that the string sorted in
// the same order as the original value. Some of the following codes are refactored from supersonic.
//
// The following implemention assumes interger is represent as little-endian and float is ieee
// format

#ifndef  FLUME_UTIL_COMPARABLE_H_
#define  FLUME_UTIL_COMPARABLE_H_

#include <limits.h>
#include <string>
#include "toft/base/byte_order.h"
#include "toft/base/static_assert.h"

namespace baidu {
namespace flume {

// unsigned integral
template <typename T>
inline void AppendOrdered(T key, std::string* s) {
    // refuse to specialize unsupport type
    TOFT_STATIC_ASSERT(sizeof(typename T::unsupported_type_for_AppendOrder) == INT_MAX);
}

template <typename T>
inline uint32_t AppendOrdered(T key, char* buffer, uint32_t buffer_size) {
    // refuse to specialize unsupport type
    TOFT_STATIC_ASSERT(sizeof(typename T::unsupported_type_for_AppendOrder) == INT_MAX);
    return 0;
}

template <typename T>
inline void AppendReverseOrdered(T key, std::string* s) {
    // refuse to specialize unsupport type
    TOFT_STATIC_ASSERT(sizeof(typename T::unsupported_type_for_AppendReverseOrder) == INT_MAX);
}

template <typename T>
inline uint32_t AppendReverseOrdered(T key, char* buffer, uint32_t buffer_size) {
    TOFT_STATIC_ASSERT(sizeof(typename T::unsupported_type_for_AppendReverseOrder) == INT_MAX);
    return 0;
}

template <>
inline void AppendOrdered(bool key, std::string* s) {
    s->append(reinterpret_cast<char*>(&key), sizeof(key));
}

template <>
inline void AppendOrdered(uint8_t key, std::string* s) {
    s->append(reinterpret_cast<char*>(&key), sizeof(key));
}

template <>
inline void AppendOrdered(uint16_t key, std::string* s) {
    key = toft::ByteOrder::ToNet<uint16_t>(key);
    s->append(reinterpret_cast<char*>(&key), sizeof(key));
}

template <>
inline void AppendOrdered(uint32_t key, std::string* s) {
    key = toft::ByteOrder::ToNet<uint32_t>(key);
    s->append(reinterpret_cast<char*>(&key), sizeof(key));
}

template <>
inline void AppendOrdered(uint64_t key, std::string* s) {
    key = toft::ByteOrder::ToNet<uint64_t>(key);
    s->append(reinterpret_cast<char*>(&key), sizeof(key));
}

template <>
inline uint32_t AppendOrdered(bool key, char* buffer, uint32_t buffer_size) {
    if (buffer_size >= sizeof(key)) {
        memcpy(buffer, reinterpret_cast<char*>(&key), sizeof(key));
    }
    return sizeof(key);
}

template <>
inline uint32_t AppendOrdered(uint8_t key, char* buffer, uint32_t buffer_size) {
    if (buffer_size >= sizeof(key)) {
        memcpy(buffer, reinterpret_cast<char*>(&key), sizeof(key));
    }
    return sizeof(key);
}

template <>
inline uint32_t AppendOrdered(uint32_t key, char* buffer, uint32_t buffer_size) {
    key = toft::ByteOrder::ToNet<uint32_t>(key);
    if (buffer_size >= sizeof(key)) {
        memcpy(buffer, reinterpret_cast<char*>(&key), sizeof(key));
    }
    return sizeof(key);
}

template <>
inline uint32_t AppendOrdered(uint64_t key, char* buffer, uint32_t buffer_size) {
    key = toft::ByteOrder::ToNet<uint64_t>(key);
    if (buffer_size >= sizeof(key)) {
        memcpy(buffer, reinterpret_cast<char*>(&key), sizeof(key));
    }
    return sizeof(key);
}

// signed integral
template <>
inline void AppendOrdered(int8_t key, std::string* s) {
    static const uint8_t n = (1U << 7);
    uint8_t ukey = (static_cast<uint8_t>(key) ^ n);
    AppendOrdered(ukey, s);
}

template <>
inline void AppendOrdered(int16_t key, std::string* s) {
    static const uint16_t n = (1U << 15);
    uint16_t ukey = (static_cast<uint16_t>(key) ^ n);
    AppendOrdered(ukey, s);
}

template <>
inline void AppendOrdered(int32_t key, std::string* s) {
    static const uint32_t n = (1U << 31);
    uint32_t ukey = (static_cast<uint32_t>(key) ^ n);
    AppendOrdered(ukey, s);
}

template <>
inline void AppendOrdered(int64_t key, std::string* s) {
    static const uint64_t n = (1LLU << 63);
    uint64_t ukey = (static_cast<uint64_t>(key) ^ n);
    AppendOrdered(ukey, s);
}

template <>
inline uint32_t AppendOrdered(int8_t key, char* buffer, uint32_t buffer_size) {
    static const uint8_t n = (1U << 7);
    uint8_t ukey = (static_cast<uint8_t>(key) ^ n);
    return AppendOrdered(ukey, buffer, buffer_size);
}

template <>
inline uint32_t AppendOrdered(int32_t key, char* buffer, uint32_t buffer_size) {
    static const uint32_t n = (1U << 31);
    uint32_t ukey = (static_cast<uint32_t>(key) ^ n);
    return AppendOrdered(ukey, buffer, buffer_size);
}

template <>
inline uint32_t AppendOrdered(int64_t key, char* buffer, uint32_t buffer_size) {
    static const uint64_t n = (1LLU << 63);
    uint64_t ukey = (static_cast<uint64_t>(key) ^ n);
    return AppendOrdered(ukey, buffer, buffer_size);
}



// floating numbers
// The IEEE float and double formats were designed so that the numbers are "lexicographically
// ordered", which - in the words of IEEE architect William Kahan mean "if two
// floating-point numbers in the same format are ordered ( say x < y ), then they are ordered
// the same way when their bits are reinterpreted as Sign-Magnitude integers."
//
// Kahan says that we can compare them if we interpret them as sign-magnitude integers. That's
// unfortunate because most processors these days use twos-complement integers. Effectively this
// means that the comparison only works if one or more of the floats is positive. If both floats are
// negative then the sense of the comparison is reversed.
// Refer to: http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm
inline uint32_t ToOrdered(float key) {
    uint32_t n = 0;
    memcpy(&n, &key, sizeof(key));
    const uint32_t sign_bit = (1U << 31);
    if ((n & sign_bit) == 0) {
        n += sign_bit;
    } else {
        // Convert twos-complement int to Sign-Magnitude int, with signed bit changed to 0
        n = -n;
    }
    return n;
}

template <>
inline void AppendOrdered(float key, std::string* s) {
    AppendOrdered(ToOrdered(key), s);
}

inline uint64_t ToOrdered(double key) {
    uint64_t n = 0;
    memcpy(&n, &key, sizeof(key));
    const uint64_t sign_bit = (1LLU << 63);
    if ((n & sign_bit) == 0) {
        n += sign_bit;
    } else {
        // Convert twos-complement int to Sign-Magnitude int, with signed bit changed to 0
        n = -n;
    }
    return n;
}

template <>
inline void AppendOrdered(double key, std::string* s) {
    AppendOrdered(ToOrdered(key), s);
}

template <>
inline uint32_t AppendOrdered(double key, char* buffer, uint32_t buffer_size) {
    return AppendOrdered(ToOrdered(key), buffer, buffer_size);
}

// string
// when key is string type, use the Null('\0') character as end of string
// if there exist Null characters in key, they should be escaped by Escape('\1')
// if there exist Escape characters in key, escape them
inline void AppendOrdered(const char* key, size_t len, std::string* s) {
    // Assume the following transform as F(), we have:
    // if s1 == s2, apparently,
    // we have: Comparable(s1) == Comparable(s2)
    //
    // if s1 + s2 == s3 (s2.size > 0), then
    //     Comparable(s1) = F(s1) + '\0',
    //     Comparable(s3) = F(s1) + F(s2) + '\0', with escape '\0', F(s2)[0] >= '\1',
    // we have: Comparable(s1) < Comparable(s3)
    //
    // if s1 < s2,
    //     let s1 = s0 + s1', s2 = s0 + s2' (s1'[0] < s2'[0])  then
    //     Comparable(s1) = F(s0) + F(s1') + '\0',
    //     Comparable(s2) = F(s0) + F(s2') + '\0',
    //     if s1'[0] == '\0' and s2'[0] == '\1', then F(s1')[1] is '\0', F(s2')[1] is '\1',
    //     we have: Comparable(s1) < Comparable(s2)
    //     if s1'[0] == '\0' and s2'[0] != '\1', then F(s1')[0] is '\1', F(s2')[0] > '\1',
    //     we have Comparable(s1) < Comparable(s2)
    //     if s1'[0] != '\0', then F(s1'[0]) is s1'[0], F(s2'[0]) is s2'[0],
    //     we have: Comparable(s1) < Comparable(s2)
    // all in all, we have: Comparable(s1) < Comparable(s2)

    static const unsigned char kNull = '\0';
    static const unsigned char kEscape = '\1';
    s->reserve(s->size() + len + 1);

    for (size_t i = 0; i < len; ++i) {
        if (key[i] == kNull || key[i] == kEscape) {
            s->append(1, kEscape);
        }
        s->append(1, key[i]);
    }

    // Append a '\0' to indicate end of string
    s->append(1, kNull);
}

inline uint32_t AppendOrdered(const char* key, size_t len, char* buffer, uint32_t buffer_size) {

    static const unsigned char kNull = '\0';
    static const unsigned char kEscape = '\1';

    uint32_t ret = 0;

    for (size_t i = 0; i < len; ++i) {
        if (key[i] == kNull || key[i] == kEscape) {
            if (ret < buffer_size) {
                buffer[ret] = kEscape;
            }
            ++ret;
        }
        if (ret < buffer_size) {
            buffer[ret] = key[i];
        }
        ++ret;
    }
    if (ret < buffer_size) {
        buffer[ret] = kNull;
    }
    ++ret;
    return ret;
}

template <>
inline void AppendOrdered(const std::string& key, std::string* s) {
    AppendOrdered(key.c_str(), key.size(), s);
}

template <>
inline uint32_t AppendOrdered(const std::string& key, char* buffer, uint32_t buffer_size) {
    return AppendOrdered(key.c_str(), key.size(), buffer, buffer_size);
}

template <>
inline void AppendReverseOrdered(bool key, std::string* s) {
    AppendOrdered(!key, s);
}

template <>
inline void AppendReverseOrdered(uint8_t key, std::string* s) {
    AppendOrdered(static_cast<uint8_t>(~key), s);
}

template <>
inline void AppendReverseOrdered(uint16_t key, std::string* s) {
    AppendOrdered(static_cast<uint16_t>(~key), s);
}

template <>
inline void AppendReverseOrdered(uint32_t key, std::string* s) {
    AppendOrdered(static_cast<uint32_t>(~key), s);
}

template <>
inline void AppendReverseOrdered(uint64_t key, std::string* s) {
    AppendOrdered(static_cast<uint64_t>(~key), s);
}

template <>
inline uint32_t AppendReverseOrdered(bool key, char* buffer, uint32_t buffer_size) {
    return AppendOrdered(!key, buffer, buffer_size);
}

template <>
inline uint32_t AppendReverseOrdered(uint64_t key, char* buffer, uint32_t buffer_size) {
    return AppendOrdered(static_cast<uint64_t>(~key), buffer, buffer_size);
}

template <>
inline void AppendReverseOrdered(int8_t key, std::string* s) {
    AppendOrdered(static_cast<int8_t>(~key), s);
}

template <>
inline void AppendReverseOrdered(int16_t key, std::string* s) {
    AppendOrdered(static_cast<int16_t>(~key), s);
}

template <>
inline void AppendReverseOrdered(int32_t key, std::string* s) {
    AppendOrdered(static_cast<int32_t>(~key), s);
}

template <>
inline void AppendReverseOrdered(int64_t key, std::string* s) {
    AppendOrdered(static_cast<int64_t>(~key), s);
}

template <>
inline uint32_t AppendReverseOrdered(uint32_t key, char* buffer, uint32_t buffer_size) {
    return AppendOrdered(static_cast<uint32_t>(~key), buffer, buffer_size);
}

template <>
inline uint32_t AppendReverseOrdered(int32_t key, char* buffer, uint32_t buffer_size) {
    return AppendOrdered(static_cast<int32_t>(~key), buffer, buffer_size);
}

template <>
inline uint32_t AppendReverseOrdered(int64_t key, char* buffer, uint32_t buffer_size) {
    return AppendOrdered(static_cast<int64_t>(~key), buffer, buffer_size);
}

template <>
inline void AppendReverseOrdered(float key, std::string* s) {
    AppendReverseOrdered(ToOrdered(key), s);
}

template <>
inline void AppendReverseOrdered(double key, std::string* s) {
    AppendReverseOrdered(ToOrdered(key), s);
}

template <>
inline uint32_t AppendReverseOrdered(double key, char* buffer, uint32_t buffer_size) {
    return AppendReverseOrdered(ToOrdered(key), buffer, buffer_size);
}

// reverse all append bytes to make a reverse ordered string
inline void AppendReverseOrdered(const char* key, size_t len, std::string* s) {
    static const unsigned char kNull = '\0';
    static const unsigned char kEscape = '\1';
    s->reserve(s->size() + len + 1);

    for (size_t i = 0; i < len; ++i) {
        if (key[i] == kNull || key[i] == kEscape) {
            s->append(1, static_cast<unsigned char>(~kEscape));
        }
        s->append(1, static_cast<unsigned char>(~key[i]));
    }

    // Append a '\FF' to indicate end of string, such that shorter string is bigger
    s->append(1, static_cast<unsigned char>(~kNull));
}

inline uint32_t AppendReverseOrdered(const char* key, size_t len, char* buffer, \
    uint32_t buffer_size) {
    static const unsigned char kNull = '\0';
    static const unsigned char kEscape = '\1';
    uint32_t ret = 0;

    for (size_t i = 0; i < len; ++i) {
        if (key[i] == kNull || key[i] == kEscape) {
            if (ret < buffer_size) {
                buffer[ret] = ~kEscape;
            }
            ++ret;
        }
        if (ret < buffer_size) {
            buffer[ret] = ~key[i];
        }
        ++ret;
    }

    // Append a '\FF' to indicate end of string, such that shorter string is bigger
    if (ret < buffer_size) {
        buffer[ret] = ~kNull;
    }
    ++ret;
    return ret;
}

template <>
inline void AppendReverseOrdered(const std::string& key, std::string* s) {
    AppendReverseOrdered(key.c_str(), key.size(), s);
}

template <>
inline uint32_t AppendReverseOrdered(const std::string& key, char* buffer, uint32_t buffer_size) {
    return AppendReverseOrdered(key.c_str(), key.size(), buffer, buffer_size);
}

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_UTIL_COMPARABLE_H_
