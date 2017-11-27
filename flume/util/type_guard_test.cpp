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

#include "flume/util/type_guard.h"

#include "gtest/gtest.h"

namespace baidu {
namespace flume {
namespace util {

class A : public TypeGuard<A> {
public:
    int member;
};

struct Base {
public:
    int member;
};

class B : public Base, public TypeGuard<B> {
public:
    int member_1;
};

TEST(TypeGuardTest, Normal) {
    A a;
    a.member = 2;

    A* ptr = A::Cast(a.ToPointer());
    ASSERT_EQ(&a, ptr);
    ASSERT_EQ(2, ptr->member);
}

TEST(TypeGuard, MultiInherit) {
    B b;
    b.member = 0;
    b.member_1 = 1;

    B* ptr = B::Cast(b.ToPointer());
    ASSERT_EQ(&b, ptr);
    ASSERT_EQ(0, ptr->member);
    ASSERT_EQ(1, ptr->member_1);
}

#ifndef NDEBUG
TEST(TypeGuard, WrongCastFromOtherTypeGuard) {
    A a;

    ASSERT_DEATH(B::Cast(a.ToPointer()), "Given pointer has wrong type: .*");
}

TEST(TypeGuard, WrongCastFromBasicType) {
    int i = 0;

    ASSERT_DEATH(A::Cast(&i), "Given pointer has wrong type: .*");
}
#endif

} // namespace util
} // namespace flume
} // namespace baidu

