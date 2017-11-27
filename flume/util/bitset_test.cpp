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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/util/bitset.h"

#include "boost/lexical_cast.hpp"
#include "gtest/gtest.h"

namespace baidu {
namespace flume {
namespace util {

TEST(BitsetTest, Construct) {
    EXPECT_EQ("", Bitset().to_string());
    EXPECT_EQ("0000", Bitset(4).to_string());
    EXPECT_EQ(std::string(128, '0'), Bitset(128).to_string());
    EXPECT_EQ("1010", Bitset("1010").to_string());
    EXPECT_EQ("0011", Bitset(4, 1024 + 3).to_string());
    EXPECT_NE("0011", Bitset(8, 1024 + 3).to_string());

    std::string big = std::string(32, '1') + std::string(32, '0');
    EXPECT_EQ(big, Bitset(big).to_string());

    std::string large = "111" + std::string(64, '0');
    EXPECT_EQ(large, Bitset(large).to_string());
}

TEST(BitsetTest, AsContainer) {
    Bitset b;
    EXPECT_EQ(0u, b.size());
    EXPECT_EQ(0u, b.count());

    b.push_back(true);
    EXPECT_EQ("1", b.to_string());
    EXPECT_EQ(1u, b.size());
    EXPECT_EQ(1u, b.count());

    b.push_back(false);
    EXPECT_EQ("01", b.to_string());
    EXPECT_EQ(2u, b.size());
    EXPECT_EQ(1u, b.count());

    while (b.size() < 64) {
        b.push_back(false);
    }
    EXPECT_EQ(std::string(63, '0') + "1", b.to_string());
    EXPECT_EQ(64u, b.size());
    EXPECT_EQ(1u, b.count());

    b.push_back(true);
    EXPECT_EQ("1" + std::string(63, '0') + "1", b.to_string());
    EXPECT_EQ(65u, b.size());
    EXPECT_EQ(2u, b.count());

    b.resize(66);
    EXPECT_EQ("01" + std::string(63, '0') + "1", b.to_string());
    EXPECT_EQ(66u, b.size());
    EXPECT_EQ(2u, b.count());

    b.resize(64);
    EXPECT_EQ(std::string(63, '0') + "1", b.to_string());
    EXPECT_EQ(64u, b.size());
    EXPECT_EQ(1u, b.count());

    b.resize(67, true);
    EXPECT_EQ("111" + std::string(63, '0') + "1", b.to_string());
    EXPECT_EQ(67u, b.size());
    EXPECT_EQ(4u, b.count());

    b.resize(65, false);
    EXPECT_EQ("1" + std::string(63, '0') + "1", b.to_string());
    EXPECT_EQ(65u, b.size());
    EXPECT_EQ(2u, b.count());

    b.resize(64, false);
    EXPECT_EQ(std::string(63, '0') + "1", b.to_string());
    EXPECT_EQ(64u, b.size());
    EXPECT_EQ(1u, b.count());

    b.resize(32, true);
    EXPECT_EQ(std::string(31, '0') + "1", b.to_string());
    EXPECT_EQ(32u, b.size());
    EXPECT_EQ(1u, b.count());

    b.clear();
    EXPECT_EQ("", b.to_string());
    EXPECT_EQ(0u, b.count());
}

TEST(BitsetTest, SetResetTest) {
    Bitset empty;
    EXPECT_EQ("", empty.set().to_string());
    EXPECT_EQ("", empty.reset().to_string());

    Bitset small("11001");
    EXPECT_EQ(3u, small.count());
    EXPECT_TRUE(small.test(0));
    EXPECT_FALSE(small.test(1));

    EXPECT_EQ("10001", small.set(3, false).to_string());
    EXPECT_EQ(2u, small.count());

    EXPECT_EQ("10011", small.set(1, true).to_string());
    EXPECT_EQ(3u, small.count());

    EXPECT_EQ("11111", small.set().to_string());
    EXPECT_EQ(5u, small.count());

    EXPECT_EQ("11011", small.reset(2).to_string());
    EXPECT_EQ(4u, small.count());

    EXPECT_EQ("00000", small.reset().to_string());
    EXPECT_EQ(0u, small.count());

    Bitset big("10" + std::string(62, '0') + "10");
    EXPECT_EQ(2u, big.count());
    EXPECT_FALSE(big.test(0));
    EXPECT_TRUE(big.test(1));
    EXPECT_FALSE(big.test(64));
    EXPECT_TRUE(big.test(65));

    EXPECT_EQ("101" + std::string(61, '0') + "10", big.set(63).to_string());
    EXPECT_EQ(3u, big.count());

    EXPECT_EQ("001" + std::string(61, '0') + "10", big.set(65, false).to_string());
    EXPECT_EQ(2u, big.count());

    EXPECT_EQ(std::string(66, '1'), big.set().to_string());
    EXPECT_EQ(66u, big.count());

    EXPECT_EQ("0" + std::string(65, '1'), big.reset(65).to_string());
    EXPECT_EQ(65u, big.count());

    EXPECT_EQ(std::string(66, '0'), big.reset().to_string());
    EXPECT_EQ(0u, big.count());
}

TEST(BitsetTest, FlipAnyNoneAll) {
    Bitset empty;
    EXPECT_EQ("", empty.flip().to_string());
    EXPECT_FALSE(empty.any());
    EXPECT_TRUE(empty.none());
    EXPECT_TRUE(empty.all());

    Bitset small("00000");
    EXPECT_FALSE(small.any());
    EXPECT_TRUE(small.none());
    EXPECT_FALSE(small.all());

    EXPECT_EQ("11111", small.flip().to_string());
    EXPECT_TRUE(small.any());
    EXPECT_FALSE(small.none());
    EXPECT_TRUE(small.all());

    EXPECT_EQ("11011", small.reset(2).to_string());
    EXPECT_TRUE(small.any());
    EXPECT_FALSE(small.none());
    EXPECT_FALSE(small.all());

    EXPECT_EQ("00100", small.flip().to_string());
    EXPECT_TRUE(small.any());
    EXPECT_FALSE(small.none());
    EXPECT_FALSE(small.all());

    Bitset big(std::string(66, '1'));
    EXPECT_TRUE(big.any());
    EXPECT_FALSE(big.none());
    EXPECT_TRUE(big.all());

    EXPECT_EQ(std::string(66, '0'), big.flip().to_string());
    EXPECT_FALSE(big.any());
    EXPECT_TRUE(big.none());
    EXPECT_FALSE(big.all());

    EXPECT_EQ("1" + std::string(65, '0'), big.set(65).to_string());
    EXPECT_TRUE(big.any());
    EXPECT_FALSE(big.none());
    EXPECT_FALSE(big.all());

    EXPECT_EQ("0" + std::string(65, '1'), big.flip().to_string());
    EXPECT_TRUE(big.any());
    EXPECT_FALSE(big.none());
    EXPECT_FALSE(big.all());
}

TEST(BitsetTest, BitwiseOperator) {
    EXPECT_EQ(Bitset(), Bitset() & Bitset());
    EXPECT_EQ(Bitset(), Bitset() | Bitset());
    EXPECT_EQ(Bitset(), Bitset() ^ Bitset());
    EXPECT_EQ(Bitset(), ~Bitset());

    EXPECT_EQ(Bitset("00100"), Bitset("10101") & Bitset("00110"));
    EXPECT_EQ(Bitset("10111"), Bitset("10101") | Bitset("00110"));
    EXPECT_EQ(Bitset("10011"), Bitset("10101") ^ Bitset("00110"));
    EXPECT_EQ(Bitset("01100"), ~Bitset("10011"));

    std::string zero(64, '0');
    std::string one(64, '1');
    EXPECT_EQ(Bitset("00" + zero), Bitset("01" + zero) & Bitset("10" + one));
    EXPECT_EQ(Bitset("11" + one), Bitset("01" + zero) | Bitset("10" + one));
    EXPECT_EQ(Bitset("11" + zero), Bitset("01" + one) ^ Bitset("10" + one));
    EXPECT_EQ(Bitset("00" + one), ~Bitset("11" + zero));
}

TEST(BitsetTest, Find) {
    EXPECT_EQ(Bitset::npos, Bitset().find_first());

    EXPECT_EQ(0u, Bitset("00001").find_first());
    EXPECT_EQ(1u, Bitset("00010").find_first());
    EXPECT_EQ(Bitset::npos, Bitset("00000").find_first());

    EXPECT_EQ(Bitset::npos, Bitset("00001").find_next(0));
    EXPECT_EQ(1u, Bitset("00011").find_next(0));
    EXPECT_EQ(2u, Bitset("00101").find_next(0));
    EXPECT_EQ(2u, Bitset("00101").find_next(1));

    EXPECT_EQ(0u, Bitset(std::string(64, '0') + "1").find_first());
    EXPECT_EQ(65u, Bitset("10" + std::string(64, '0')).find_first());
    EXPECT_EQ(Bitset::npos, Bitset(std::string(65, '0')).find_first());

    EXPECT_EQ(Bitset::npos, Bitset(std::string(64, '0') + "1").find_next(0));
    EXPECT_EQ(65u, Bitset("1" + std::string(64, '0') + "1").find_next(32));
    EXPECT_EQ(65u, Bitset("10" + std::string(64, '0')).find_next(64));
    EXPECT_EQ(Bitset::npos, Bitset("01" + std::string(64, '0')).find_next(64));
}

TEST(BitsetTest, AccessBit) {
    Bitset b("0101");
    EXPECT_TRUE(b[0]);
    EXPECT_FALSE(b[1]);
    EXPECT_FALSE(~b[2]);
    EXPECT_TRUE(~b[3]);

    b[3].flip();
    EXPECT_EQ(Bitset("1101"), b);

    b[2] = false;
    EXPECT_EQ(Bitset("1001"), b);

    b[0] = b[1];
    EXPECT_EQ(Bitset("1000"), b);

    b[1] |= true;
    EXPECT_EQ(Bitset("1010"), b);

    b[2] ^= true;
    EXPECT_EQ(Bitset("1110"), b);

    b[3] &= false;
    EXPECT_EQ(Bitset("0110"), b);
}

TEST(BitsetTest, Utility) {
    Bitset three(std::string(30, '0') + "11");
    EXPECT_EQ(3ul, three.to_ulong());
    LOG(INFO) << three << " is " << three.to_ulong();
}

} // namespace util
} // namespace flume
} // namespace baidu

