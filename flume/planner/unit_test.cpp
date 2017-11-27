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
// Author: Wang Song <bigflow-opensource@baidu.com>
//

#include "flume/planner/unit.h"
#include "flume/planner/plan.h"

#include "gtest/gtest.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

struct Test1 {
    Test1() : value(0) {}
    int value;
};
struct Test2 {
    Test2() : value(0) {}
    int value;
};

}  // namespace

TEST(UnitTest, Clone) {
    Plan plan;

    Unit* u1 = plan.NewUnit("u1", false);
    Unit* u2 = plan.NewUnit("u2", true);
    Unit* u3 = plan.NewUnit("u3", true);
    Unit* u4 = plan.NewUnit("u4", true);

    plan.AddControl(u1, u2);
    plan.AddControl(u1, u3);
    plan.AddControl(u1, u4);
    plan.AddDependency(u2, u3);
    plan.AddDependency(u3, u4);

    Test1 t1;
    t1.value = 1;
    Test2 t2;
    t2.value = 2;

    u3->get<Test1>() = t1;
    u3->get<Test2>() = t2;
    EXPECT_EQ(u3->get<Test1>().value, t1.value);
    EXPECT_EQ(u3->get<Test2>().value, t2.value);

    Unit* clone_node = u3->clone();

    EXPECT_TRUE(clone_node->father() == NULL);
    EXPECT_EQ(u3->father(), u1);

    EXPECT_EQ(clone_node->direct_needs().size(), 0);
    EXPECT_EQ(u3->direct_needs().size(), 1);

    EXPECT_EQ(clone_node->direct_users().size(), 0);
    EXPECT_EQ(u3->direct_users().size(), 1);

    EXPECT_TRUE(clone_node->has<Test1>());
    EXPECT_TRUE(u3->has<Test1>());

    EXPECT_TRUE(clone_node->has<Test2>());
    EXPECT_TRUE(u3->has<Test2>());

    EXPECT_NE(&clone_node->get<Test1>(), &u3->get<Test1>());
    EXPECT_NE(&clone_node->get<Test2>(), &u3->get<Test2>());

    EXPECT_EQ(clone_node->get<Test1>().value, t1.value);
    EXPECT_EQ(clone_node->get<Test2>().value, t2.value);

    EXPECT_TRUE(u2->is_descendant_of(u1));
    EXPECT_FALSE(u2->is_descendant_of(u2));
    EXPECT_FALSE(u2->is_descendant_of(u3));
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

