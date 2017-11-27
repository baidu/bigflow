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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//
// Description: A copy-friendly dynamic_bitset.

#ifndef FLUME_UTIL_BITSET_H
#define FLUME_UTIL_BITSET_H

#include <stdint.h>

#include <bitset>
#include <string>

#include "boost/dynamic_bitset.hpp"
#include "boost/lexical_cast.hpp"
#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace util {

// Has same interface as boost::dynamic_bitset<>
class Bitset {
public:
    typedef std::size_t size_type;
    static const size_type npos = static_cast<size_type>(-1);

    class Reference {
    public:
        friend class Bitset;

        operator bool() const {
            return _bitset->test(_pos);
        }

        bool operator~() const {
            return !_bitset->test(_pos);
        }

        Reference& flip() {
            _bitset->flip(_pos);
            return *this;
        }

        Reference& operator=(bool x) { // for b[i] = x
            _bitset->set(_pos, x);
            return *this;
        }

        Reference& operator=(const Reference& rhs) { // for b[i] = b[j]
            _bitset->set(_pos, rhs);
            return *this;
        }

        Reference& operator|=(bool x) {
            if (x) {
                _bitset->set(_pos);
            }
            return *this;
        }

        Reference& operator&=(bool x) {
            if (!x) {
                _bitset->reset(_pos);
            }
            return *this;
        }

        Reference& operator^=(bool x) {
            if (x) {
                _bitset->flip(_pos);
            }
            return *this;
        }

    private:
        Reference(Bitset* bitset, int pos) : _bitset(bitset), _pos(pos) {}

        void operator&();  // left undefined. NOLINT(runtime/operator)

        Bitset* _bitset;
        size_type _pos;
    };

    Bitset() : _static_size(0), _dynamic_bitset(NULL) {}

    explicit Bitset(const std::string& str) :
            _static_size(str.size() <= kMaxStaticSize ? str.size() : str.size() - kMaxStaticSize),
            _static_bitset(str, str.size() - _static_size),
            _dynamic_bitset(NULL) {
        if (str.size() > _static_size) {
            _dynamic_bitset = new DynamicBitset(str, 0, str.size() - _static_size);
        }
    }

    explicit Bitset(size_type num_bits) :
            _static_size(num_bits <= kMaxStaticSize ? num_bits : kMaxStaticSize),
            _dynamic_bitset(NULL) {
        if (num_bits > _static_size) {
            _dynamic_bitset = new DynamicBitset(num_bits - _static_size);
        }
    }

    explicit Bitset(size_type num_bits, unsigned long value) :  // NOLINT(runtime/int)
            _static_size(num_bits <= kMaxStaticSize ? num_bits : kMaxStaticSize),
            _static_bitset(value), _dynamic_bitset(NULL) {
        _static_bitset &= static_mask();
        if (num_bits > _static_size) {
            _dynamic_bitset = new DynamicBitset(num_bits - _static_size);
        }
    }

    Bitset(const Bitset& b) :
            _static_size(b._static_size),
            _static_bitset(b._static_bitset),
            _dynamic_bitset(NULL) {
        if (b._dynamic_bitset != NULL) {
            _dynamic_bitset = new DynamicBitset(*b._dynamic_bitset);
        }
    }

    ~Bitset() {
        delete _dynamic_bitset;
    }

    Bitset& operator=(const Bitset& b) {
        _static_size = b._static_size;
        _static_bitset = b._static_bitset;
        if (b._dynamic_bitset != NULL) {
            _dynamic_bitset = new DynamicBitset(*b._dynamic_bitset);
        }
        return *this;
    }

    void clear() {
        _static_size = 0;
        _static_bitset.reset();
        if (_dynamic_bitset != NULL) {
            _dynamic_bitset->clear();
        }
    }

    void push_back(bool bit) {
        if (_static_size < kMaxStaticSize) {
            _static_bitset[_static_size++] = bit;
            return;
        }

        if (_dynamic_bitset == NULL) {
            _dynamic_bitset = new DynamicBitset;
        }
        _dynamic_bitset->push_back(bit);
    }

    void resize(size_type num_bits) {
        resize(num_bits, false);
    }

    void resize(size_type num_bits, bool value) {
        if (size() <= num_bits) {
            for (size_type i = size(); i < num_bits; ++i) {
                push_back(value);
            }
        } else {
            if (num_bits >= kMaxStaticSize) {
                _dynamic_bitset->resize(num_bits - kMaxStaticSize, value);
            } else {
                _static_size = num_bits;
                _static_bitset &= static_mask();
                _dynamic_bitset->resize(0);
            }
        }
    }

    size_type size() const {
        if (_dynamic_bitset == NULL) {
            return _static_size;
        } else {
            return _static_size + _dynamic_bitset->size();
        }
    }

    size_type count() const {
        if (_dynamic_bitset == NULL) {
            return _static_bitset.count();
        } else {
            return _static_bitset.count() + _dynamic_bitset->count();
        }
    }

    bool test(size_type n) const {
        DCHECK_LT(n, size());
        return n < _static_size ? _static_bitset.test(n) : _dynamic_bitset->test(n - _static_size);
    }

    bool any() const {
        return _static_bitset.any() || (_dynamic_bitset != NULL && _dynamic_bitset->any());
    }

    bool none() const {
        return _static_bitset.none() && (_dynamic_bitset == NULL || _dynamic_bitset->none());
    }

    bool all() const {
        if (_static_bitset != static_mask()) {
            return false;
        }
        return _dynamic_bitset == NULL || _dynamic_bitset->all();
    }

    Reference operator[](size_type pos) {
        return Reference(this, pos);
    }

    bool operator[](size_type pos) const { return test(pos); }

    Bitset& set() {
        _static_bitset = static_mask();
        if (_dynamic_bitset != NULL) {
            _dynamic_bitset->set();
        }
        return *this;
    }

    Bitset& set(size_type n) {
        return set(n, true);
    }

    Bitset& set(size_type n, bool val) {
        DCHECK_LT(n, size());
        if (n < _static_size) {
            _static_bitset.set(n, val);
        } else {
            _dynamic_bitset->set(n - _static_size, val);
        }
        return *this;
    }

    Bitset& reset() {
        _static_bitset.reset();
        if (_dynamic_bitset != NULL) {
            _dynamic_bitset->reset();
        }
        return *this;
    }

    Bitset& reset(size_type n) {
        DCHECK_LT(n, size());
        if (n < _static_size) {
            _static_bitset.reset(n);
        } else {
            _dynamic_bitset->reset(n - _static_size);
        }
        return *this;
    }

    Bitset& flip() {
        _static_bitset = ~_static_bitset & static_mask();
        if (_dynamic_bitset != NULL) {
            _dynamic_bitset->flip();
        }
        return *this;
    }

    Bitset& flip(size_type n) {
        DCHECK_LT(n, size());
        if (n < _static_size) {
            _static_bitset.flip(n);
        } else {
            _dynamic_bitset->flip(n - _static_size);
        }
        return *this;
    }

    Bitset& operator&=(const Bitset& b) {
        DCHECK_EQ(size(), b.size());
        _static_bitset &= b._static_bitset;
        if (_dynamic_bitset != NULL && b._dynamic_bitset != NULL) {
            *_dynamic_bitset &= *b._dynamic_bitset;
        }
        return *this;
    }

    Bitset operator&(const Bitset& b) const {
        Bitset a = *this;
        return a &= b;
    }

    Bitset& operator|=(const Bitset& b) {
        DCHECK_EQ(size(), b.size());
        _static_bitset |= b._static_bitset;
        if (_dynamic_bitset != NULL && b._dynamic_bitset != NULL) {
           * _dynamic_bitset |= *b._dynamic_bitset;
        }
        return *this;
    }

    Bitset operator|(const Bitset& b) const {
        Bitset a = *this;
        return a |= b;
    }

    Bitset& operator^=(const Bitset& b) {
        DCHECK_EQ(size(), b.size());
        _static_bitset = (_static_bitset ^ b._static_bitset) & static_mask();
        if (_dynamic_bitset != NULL && b._dynamic_bitset != NULL) {
            *_dynamic_bitset ^= *b._dynamic_bitset;
        }
        return *this;
    }

    Bitset operator^(const Bitset& b) const {
        Bitset a = *this;
        return a ^= b;
    }

    Bitset operator~() const {
        Bitset b = *this;
        return b.flip();
    }

    size_type find_first() const {
        int static_index = __builtin_ffsl(_static_bitset.to_ulong());
        if (static_index > 0 || _dynamic_bitset == NULL) {
            return static_index - 1;  // 0 - 1 == npos
        }

        size_type dynamic_index = _dynamic_bitset->find_first();
        return dynamic_index == DynamicBitset::npos ? npos : dynamic_index + _static_size;
    }

    size_type find_next(size_type pos) const {
        int static_index = __builtin_ffsl((_static_bitset & ~static_mask(pos + 1)).to_ulong());
        if (static_index > 0 || _dynamic_bitset == NULL) {
            return static_index - 1;  // 0 - 1 == npos
        }

        size_type dynamic_index = _dynamic_bitset->find_next(pos - _static_size);
        return dynamic_index == DynamicBitset::npos ? npos : dynamic_index + _static_size;
    }

    unsigned long to_ulong() const {  // NOLINT(runtime/int)
        DCHECK(_dynamic_bitset == NULL);
        return _static_bitset.to_ulong();
    }

    std::string to_string() const {

        std::string to_str = boost::lexical_cast<std::string>(_static_bitset);
        std::string static_str = to_str.substr(kMaxStaticSize - _static_size);

        std::string dynamic_str;
        if (_dynamic_bitset != NULL) {
            boost::to_string(*_dynamic_bitset, dynamic_str);
        }

        return dynamic_str + static_str;
    }

    friend std::ostream& operator<<(std::ostream& os, const Bitset& b) {
        return os << b.to_string();
    }

    friend bool operator==(const Bitset& a, const Bitset& b) {
        if (a.size() != b.size() || a._static_bitset != b._static_bitset) {
            return false;
        }

        if (a._dynamic_bitset != NULL && b._dynamic_bitset != NULL) {
            return *a._dynamic_bitset == *b._dynamic_bitset;
        }
        return true;
    }

    friend bool operator!=(const Bitset& a, const Bitset& b) {
        return !(a == b);
    }

private:
    typedef unsigned long Block;  // choose block type for to_ulong. NOLINT(runtime/int)
    static const size_type kMaxStaticSize = sizeof(Block) * 8;

    typedef std::bitset<kMaxStaticSize> StaticBitset;
    typedef boost::dynamic_bitset<Block> DynamicBitset;

    StaticBitset static_mask() const {
        return static_mask(_static_size);
    }

    StaticBitset static_mask(size_type size) const {
        return StaticBitset(size < kMaxStaticSize ? (Block(1) << size) - 1 : Block(-1));
    }

    size_type _static_size;
    StaticBitset _static_bitset;
    DynamicBitset* _dynamic_bitset;
};

} // namespace util
} // namespace flume
} // namespace baidu

#endif  // FLUME_UTIL_BITSET_H
