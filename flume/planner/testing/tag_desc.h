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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>

#ifndef FLUME_PLANNER_TESTING_TAG_DESC_H_
#define FLUME_PLANNER_TESTING_TAG_DESC_H_

#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "boost/make_shared.hpp"
#include "boost/shared_ptr.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/pass.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace testing {

template<typename Impl>
static void PrintTo(const PolymorphicMatcher<Impl>& matcher, ::std::ostream* os) {
    matcher.impl().DescribeTo(os);
}

}  // namespace testing

namespace google {
namespace protobuf {

static inline void PrintTo(const Message& message, ::std::ostream* os) {
    *os << message.DebugString();
}

}  // namespace protobuf
}  // namespace google

namespace baidu {
namespace flume {
namespace planner {

class TagDesc {
public:
    virtual ~TagDesc() {}
    virtual void Set(Unit* unit)  = 0;
    virtual bool Test(Unit* unit) = 0;
};

typedef boost::shared_ptr<TagDesc> TagDescRef;

TagDescRef UnitType(Unit::Type type);
TagDescRef PbScopeTag(PbScope::Type type);
TagDescRef PbScopeTag(PbScope::Type type, int concurrency);
TagDescRef PbScopeSortedTag(bool is_sorted);
TagDescRef PbScopeStreamTag(bool is_stream);
TagDescRef PbNodeTag(PbLogicalPlanNode::Type type);
TagDescRef PbShuffleNodeTag(PbShuffleNode::Type type);
TagDescRef EffectiveKeyNum(int n);
TagDescRef ScopeLevelTag(int n);
TagDescRef Stage(OptimizingStage stage);

template<typename Tag>
TagDescRef NewTagDesc() {
    class TagDescImpl : public TagDesc {
    public:
        virtual void Set(Unit* unit) {
            unit->set<Tag>();
        }

        virtual bool Test(Unit* unit) {
            return unit->has<Tag>();
        }
    };

    TagDesc* desc = new TagDescImpl();
    return TagDescRef(desc);
}

template<typename Tag>
class TagDescImplForNewTagDesc : public TagDesc {
public:
    explicit TagDescImplForNewTagDesc(const Tag& value) : m_value(value) {}

    virtual void Set(Unit* unit) {
        unit->get<Tag>() = m_value;
    }

    virtual bool Test(Unit* unit) {
        return unit->has<Tag>() && unit->get<Tag>() == m_value;
    }

private:
    Tag m_value;
};

template<typename Tag>
TagDescRef NewTagDesc(const Tag& value) {
    TagDesc* desc = new TagDescImplForNewTagDesc<Tag>(value);
    return TagDescRef(desc);
}

template<typename Tag>
class TagDescImplForNewTagDescNoTest : public TagDesc {
public:
    explicit TagDescImplForNewTagDescNoTest(const Tag& value) : m_value(value) {}

    virtual void Set(Unit* unit) {
        unit->get<Tag>() = m_value;
    }

    virtual bool Test(Unit* unit) {
        return false;
    }

private:
    Tag m_value;
};

template<typename Tag>
TagDescRef NewTagDescNoTest(const Tag& value) {
    TagDesc* desc = new TagDescImplForNewTagDescNoTest<Tag>(value);
    return TagDescRef(desc);
}

template<typename Tag, typename T>
class TagDescImplForNewValueTag : public TagDesc {
public:
    explicit TagDescImplForNewValueTag(const T& value) : m_value(value) {}

    virtual void Set(Unit* unit) {
        *unit->get<Tag>() = m_value;
    }

    virtual bool Test(Unit* unit) {
        return unit->has<Tag>() && *unit->get<Tag>() == m_value;
    }

private:
    T m_value;
};

template<typename Tag, typename T>
TagDescRef NewValueTag(const T& value) {
    TagDesc* desc = new TagDescImplForNewValueTag<Tag, T>(value);
    return TagDescRef(desc);
}

template<typename T>
class TagDescImplForNewTaskSingleton : public TagDesc {
public:
    typedef TaskSingleton<T> Tag;

    explicit TagDescImplForNewTaskSingleton() {}

    virtual void Set(Unit* unit) {
        unit->task()->get<Tag>().Assign(unit);
    }

    virtual bool Test(Unit* unit) {
        if (unit->task() == NULL) {
            return false;
        }

        return unit->task()->template has<Tag>() && unit->task()->template get<Tag>() == unit;
    }
};

template<typename T>
TagDescRef NewTaskSingleton() {
    TagDesc* desc = new TagDescImplForNewTaskSingleton<T>();
    return TagDescRef(desc);
}

template<typename Tag, typename Matcher>
class TagDescImplForMatchTag : public TagDesc {
public:
    explicit TagDescImplForMatchTag(Matcher m) : _matcher(m) {}

    virtual void Set(Unit* unit) {
        using ::testing::ExplainMatchResult;
        using ::testing::StringMatchResultListener;

        // For matchers, Set is usually called when match failed.
        // explain match result here for debug.
        LOG(ERROR) << "Match " << _history.size() << " times: " << PrintToString(_matcher);
        for (size_t i = 0; i < _history.size(); ++i) {
            StringMatchResultListener listener;
            ExplainMatchResult(_matcher, _history[i].second, &listener);
            LOG(ERROR) << "match unit " << _history[i].first << ": " << listener.str();
        }
    }

    virtual bool Test(Unit* unit) {
        using ::testing::Matches;

        if (unit->has<Tag>()) {
            _history.push_back(std::make_pair(unit->identity(), unit->get<Tag>()));
            return Matches(_matcher)(unit->get<Tag>());
        } else {
            _history.push_back(std::make_pair(unit->identity(), Tag()));
            return false;
        }
    }

private:
    Matcher _matcher;
    std::vector< std::pair<std::string, Tag> > _history;
};

template<typename Tag, typename Matcher>
TagDescRef MatchTag(Matcher m) {
    TagDesc* desc = new TagDescImplForMatchTag<Tag, Matcher>(m);
    return TagDescRef(desc);
}

template<typename Tag>
class TagDescImplForNoTag : public TagDesc {
public:
    virtual void Set(Unit* unit) {
        unit->clear<Tag>();
    }

    virtual bool Test(Unit* unit) {
        return !unit->has<Tag>();
    }
};

template<typename Tag>
TagDescRef NoTag() {
    TagDesc* desc = new TagDescImplForNoTag<Tag>();
    return TagDescRef(desc);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_TESTING_TAG_DESC_H_
