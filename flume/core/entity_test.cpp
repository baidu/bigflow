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

#include "flume/core/entity.h"

#include <string>

#include "boost/lexical_cast.hpp"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"
// #include "bigflow_python/serde/cpickle_serde.h"
// #include "bigflow_python/python_util.h"

#include "flume/proto/entity.pb.h"

namespace baidu {
namespace flume {
namespace core {

struct Parameter {
public:
    int value;

public:
    explicit Parameter(int n) : value(n) {}
};

class Base {
public:
    virtual ~Base() {}

    virtual void Setup(const std::string& config) = 0;
    virtual void Setup(const std::string& config, const Parameter& p) = 0;
    virtual void Setup(const std::string& config, Parameter* p) = 0;
    virtual std::string ToString() const = 0;
};

class Derived : public Base {
public:
    virtual void Setup(const std::string& config) {
        m_config = config;
        m_value = 0;
    }

    virtual void Setup(const std::string& config, const Parameter& p) {
        m_config = config;
        m_value = p.value;
    }

    virtual void Setup(const std::string& config, Parameter* p) {
        m_config = config;
        m_value = p->value;
    }

    virtual std::string ToString() const {
        return "[" + m_config + "]: " + boost::lexical_cast<std::string>(m_value);
    }

private:
    std::string m_config;
    int m_value;
};

TEST(EntityTest, Empty) {
    Entity<Base> entity;
    ASSERT_TRUE(entity.empty());
}

TEST(EntityTest, Equality) {
    Entity<Base> entity_1("foo", "foo");
    Entity<Base> entity_2("foo", "foo");
    Entity<Base> entity_3("foo~", "foo");
    Entity<Base> entity_4("foo", "foo~");

    ASSERT_TRUE(entity_1 == entity_2);
    ASSERT_FALSE(entity_1 != entity_2);

    ASSERT_FALSE(entity_1 == entity_3);
    ASSERT_TRUE(entity_1 != entity_3);

    ASSERT_FALSE(entity_1 == entity_4);
    ASSERT_TRUE(entity_1 != entity_4);
}

TEST(EntityTest, Of) {
    Entity<Base> entity = Entity<Base>::Of<Derived>("test");

    ASSERT_FALSE(entity.empty());
    ASSERT_EQ(entity, Entity<Base>("baidu::flume::core::Derived", "test"));
}

TEST(EntityTest, ToProtoMessage) {
    Entity<Base> entity = Entity<Base>::Of<Derived>("test");

    PbEntity message = entity.ToProtoMessage();
    ASSERT_EQ(std::string("baidu::flume::core::Derived"), message.name());
    ASSERT_EQ(std::string("test"), message.config());
}

TEST(EntityTest, FromProtoMessage) {
    PbEntity message;
    message.set_name("baidu::flume::core::Derived");
    message.set_config("test");

    ASSERT_EQ(Entity<Base>::Of<Derived>("test"), Entity<Base>::From(message));
}

class EntityBaseHelper : public EntityBase {
public:
    std::string& get_name() {
        return m_name;
    }
    std::string& get_config() {
        return m_config;
    }
};

TEST(EntityBaseTest, FromProtoMessage) {
    PbEntity message;
    message.set_name("halo");
    message.clear_config();
    message.set_config_file("./testdata/pb_config_file.dat");
    EntityBaseHelper eb;
    eb.FromProtoMessage(message);
    std::string str("\x80\x2U\x1Cwelcome to the magic world 0q");
    ASSERT_STREQ(eb.get_config().c_str(), str.c_str());
}

TEST(EntityTest, CreateAndSetup) {
    Entity<Base> entity = Entity<Base>::Of<Derived>("test");

    // no extra parameters
    {
        toft::scoped_ptr<Base> ptr(entity.CreateAndSetup());
        ASSERT_TRUE(NULL != ptr.get());
        ASSERT_EQ(std::string("[test]: 0"), ptr->ToString());
    }

    // pass parameter by ref
    {
        Parameter p(1);
        toft::scoped_ptr<Base> ptr(entity.CreateAndSetup(p));
        ASSERT_TRUE(NULL != ptr.get());
        ASSERT_EQ(std::string("[test]: 1"), ptr->ToString());
    }

    // pass parameter by pointer
    {
        Parameter p(2);
        toft::scoped_ptr<Base> ptr(entity.CreateAndSetup(&p));
        ASSERT_TRUE(NULL != ptr.get());
        ASSERT_EQ(std::string("[test]: 2"), ptr->ToString());
    }
}

}  // namespace core
}  // namespace flume
}  // namespace baidu
