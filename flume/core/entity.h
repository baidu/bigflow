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
// Reflectable entity.

#ifndef FLUME_CORE_ENTITY_H_
#define FLUME_CORE_ENTITY_H_

#include <string>

#include "flume/util/reflection.h"

#include "glog/logging.h"

namespace baidu {
namespace flume {

class PbEntity;  // flume/proto/entity.proto

namespace core {

class EntityObject {
public:
    virtual void Setup(const std::string& ) = 0;
    virtual ~EntityObject() {}
};


class EntityBase {
public:
    EntityBase() {}
    EntityBase(const std::string& name, const std::string& config)
            : m_name(name), m_config(config) {}

    bool empty() const { return m_name.empty(); }

    std::string* mutable_config() { return &m_config; }

    void FromProtoMessage(const PbEntity& message);
    PbEntity ToProtoMessage() const;

protected:
    std::string m_name;
    std::string m_config;
};


template<typename T>
class Entity : public EntityBase {
public:
    typedef T BaseType;

    template<typename SubType>
    static Entity Of(const std::string& config) {
        using ::baidu::flume::Reflection;
        return Entity(Reflection<BaseType>::template TypeName<SubType>(), config);
    }

    static Entity From(const PbEntity& message) {
        return Entity(message);
    }

public:
    Entity() {}
    explicit Entity(const PbEntity& message) { FromProtoMessage(message); }
    Entity(const std::string& name, const std::string& config) : EntityBase(name, config) {}

    BaseType* CreateAndSetup() const {
        using ::baidu::flume::Reflection;
        BaseType* base = Reflection<BaseType>::New(this->m_name);
        //CHECK_NOTNULL(base);
        CHECK(NULL != base) << "Class: " << this->m_name << " not found";
        base->Setup(m_config);
        return base;
    }

    template<typename P>
    BaseType* CreateAndSetup(const P& param) const {
        using ::baidu::flume::Reflection;
        BaseType* base = Reflection<BaseType>::New(this->m_name);
        CHECK_NOTNULL(base);
        base->Setup(m_config, param);
        return base;
    }

    template<typename P>
    BaseType* CreateAndSetup(P* param) const {
        using ::baidu::flume::Reflection;
        BaseType* base = Reflection<BaseType>::New(this->m_name);
        CHECK_NOTNULL(base);
        base->Setup(m_config, param);
        return base;
    }

    bool operator==(const Entity<T>& other) const {
        return m_name == other.m_name && m_config == other.m_config;
    }

    bool operator!=(const Entity<T>& other) const {
        return !(*this == other);
    }
};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_ENTITY_H_
