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

#include "gtest/gtest.h"

#include "flume/util/intrusive_ptr_base.h"

namespace baidu {
namespace flume {
namespace util {

class Object : public IntrusivePtrBase<Object> {
public:
    Object() {}
};

TEST(IntrusivePtrBaseTest, Test) {
    Object* obj = new Object();  // no need to delete
    boost::intrusive_ptr<Object> obj1(obj);
    ASSERT_EQ(1, obj->ref_count());
    {
        boost::intrusive_ptr<Object> obj2(obj1);
        ASSERT_EQ(2, obj->ref_count());
    }
    ASSERT_EQ(1, obj->ref_count());
    boost::intrusive_ptr<Object> obj3(obj1);
    ASSERT_EQ(2, obj->ref_count());

    boost::intrusive_ptr<Object> obj4 = obj3->self();
    ASSERT_EQ(3, obj->ref_count());
}

}  // namespace util
}  // namespace flume
}  // namespace baidu
