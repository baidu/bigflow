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
// A simple Reflection mechanism in C++. This Reflection mechanism is based on
// template and virtual inheritance. If we have a SubClass inherited from BaseType,
// we can use Reflection to get an identity of SubClass as a BaseType, then use
// this identity to create any quantity of SubClass instances at any time. For
// example:
//
//      /* basetype.h */
//      class BaseType {
//      public:
//          virtual std::string ToString() {
//              return "hello, base";
//          }
//      };
//
//      ...
//
//      /* subclass.h */
//      #include "basetype.h"
//
//      class SubClass : public BaseType {
//      public:
//          virtual std::string ToString() {
//              return "hello, sub";
//          }
//      };
//
//      ...
//
//      /* source file */
//      #include "subclass.h"
//
//      std::string type = Reflection<BaseType>::TypeName<SubClass>();
//      BaseType *object = Reflection<BaseType>::New(type);
//
//      std::cout << object->ToString() << std::endl;
//      /* should output "hello, sub" */
//
//
// Profits:
//
//      1. Pure template implemention, fit for metaprogramming.
//      2. All primitives are provided as expression. Compare to class_registry.h
//         in toft, Reflection is more flexible to use.
//      3. Mention is Registration. The existence of TypeName<SubClass>() itself
//         make New("SubClass") usable, the TypeName method need ***NOT*** to be
//         really execuated. For example:
//
//              int main(int argc, char* argv[]) {
//                  std::string type;
//                  if (argc == 1) {
//                      std::cout << Reflection<BaseTye>::TypeName<SubClass>()
//                                << std::endl;
//                  } else {
//                      std::cin >> type;
//                      std::cout << Reflection<BaseType>>::New(type)->ToString()
//                                << std::endl;
//                  }
//              }
//
//          Then, we run as this:
//
//              a.out | a.out run
//
//          Finally we get 'hello, sub' as output. This feature makes it easy to
//          dispatch execuation plan in distribution environments. Master and
//          worker usually share the same binary, however, they may run in
//          different branches.
//
//
// Limitations:
//
//      1. To avoid RTTI, we use gcc specific macros to get class name in
//         compile time. This makes Reflection less portable, while more clean
//         and effective.
//      2. Reflection does not support classes defined inside function. C++ forbids
//         function-level class to be used as template parameter.
//      3. Reflection are not designed to be used before or after the execution of main.
//         So do not use Reflection in ctors and dtors of global variables.
//      4. If use Reflection with dlopen, remember to make sure the latest dlopend
//         share-library dlclosed first. However, dlopen is not encouraged to used, unless
//         you really know what you are doing.

#ifndef FLUME_UTIL_REFLECTION_H_
#define FLUME_UTIL_REFLECTION_H_

#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include <iostream>
#include <stdio.h>
#include <typeinfo>

#include "flume/util/compiler.h"

#if FLUME_GCC_VERSION == 30405
#define FLUME_ANONYMOUT_PREFIX "<unnamed>"
#elif FLUME_GCC_VERSION >= 40800
#define FLUME_ANONYMOUT_PREFIX "{anonymous}"
#elif defined(__clang__)
// todo: maybe more work is needed to support reflection with clang
// reflection_test needs to be refactored to generate dynamic library by demand
#define FLUME_ANONYMOUT_PREFIX "(anonymous namespace)"
#else
#error "Reflection only work on GCC 3.4.5 or above GCC 4.8."
#endif

namespace baidu {
namespace flume {

// A little cookie shipped with Reflection. Return identical while still human
// readable type name.
template<typename T>
const std::string PrettyTypeName();
template<typename T>
const std::string PrettyTypeName(const T&);

// Reflection is a static class, provides all APIs as static method. See comment
// at top of file for a complete.
template<typename BaseType>
class Reflection {
public:
    // Return an unique type name for SubType, can be used latter for New an
    // instance of SubType. Note that SubType must be child of BaseType.
    template<typename SubType>
    static const std::string TypeName() {
        Type<SubType>::s_register.Mention();
        return UniqueTypeName<SubType>();
    }

    // Create an instance of SubType as BaseType and return. Callers are
    // responsible for recycling returned object.
    static BaseType* New(const std::string& type);

    // Return all sub-class names registered for BaseType.
    static const std::vector<std::string> TypeList();

private:
    template<typename SubType>
    class TypeRegister {
    public:
        TypeRegister();
        ~TypeRegister();

        void Mention() const {}
    };

    template<typename SubType>
    struct Type {
    public:
        // static varibles in class scope has global life time. Mention to
        // s_register will cause TypeRegister's ctor be called before main's
        // execuation.
        static TypeRegister<SubType> s_register;
        static const char* s_name;
    };

    class ObjectFactoryBase {
    public:
        virtual BaseType* New() const = 0;
        virtual ~ObjectFactoryBase() {}
    };
    // if user use dlopen, especially when there are same Reflection Type in different
    // share libraries, TypeRegister's ctor and dtor may be called multi-times in some
    // rare situation. So we use a stack to manage register conflicts;
    class FactoryHolder {
    public:
        FactoryHolder() {
            m_current_factory = NULL;
        }

        ~FactoryHolder() {
            for (size_t i = 0; i < m_factory_stack.size(); ++i) {
                delete m_factory_stack[i].second;
            }
        }

        void RegisterFactory(void* entry, ObjectFactoryBase* factory) {
            m_factory_stack.push_back(std::make_pair(entry, factory));
            m_current_factory = factory;
        }

        void RemoveFactory(void* entry) {
            typename FactoryStack::iterator it = m_factory_stack.end();
            while (--it >= m_factory_stack.begin()) {
                if (it->first == entry) {
                    delete it->second;
                    m_factory_stack.erase(it);
                    break;
                }
            }
            if (m_factory_stack.empty()) {
                m_current_factory = NULL;
            } else {
                m_current_factory = m_factory_stack.back().second;
            }
        }

        const ObjectFactoryBase* GetFactory() const {
            return m_current_factory;
        }

    private:
        typedef std::vector<std::pair<void*, ObjectFactoryBase*> > FactoryStack;
        FactoryStack m_factory_stack;
        const ObjectFactoryBase* m_current_factory;
    };
    typedef std::map<std::string, FactoryHolder*> FactoryMap;

    template<typename SubType>
    class ObjectFactory : public ObjectFactoryBase {
    public:
        virtual BaseType* New() const {
            return new SubType;
        }
    };

private:
    template<typename SubType>
    static const std::string UniqueTypeName();

private:
    static FactoryMap* s_factories;
};

template<typename BaseType>
template<typename SubType>
Reflection<BaseType>::TypeRegister<SubType>
Reflection<BaseType>::Type<SubType>::s_register;

template<typename BaseType>
template<typename SubType>
const char* Reflection<BaseType>::Type<SubType>::s_name = NULL;

template<typename BaseType>
typename Reflection<BaseType>::FactoryMap* Reflection<BaseType>::s_factories = NULL;

namespace util {

class TypeRegisterGuard {
public:
    TypeRegisterGuard();
    ~TypeRegisterGuard();
};

class TypeLookupGuard {
public:
    TypeLookupGuard();
    ~TypeLookupGuard();
};

}  // namespace util

template<typename BaseType>
BaseType* Reflection<BaseType>::New(const std::string& type) {
    util::TypeLookupGuard guard;

    if (NULL == s_factories) {
        return NULL;
    }

    typename FactoryMap::iterator it = s_factories->find(type);
    if (it != s_factories->end()) {
        return it->second->GetFactory()->New();
    } else {
        return NULL;
    }
}

template<typename BaseType>
const std::vector<std::string> Reflection<BaseType>::TypeList() {
    util::TypeLookupGuard guard;

    std::vector<std::string> result;
    if (NULL != s_factories) {
        typename FactoryMap::iterator it = s_factories->begin();
        while (it != s_factories->end()) {
            result.push_back(it->first);
            ++it;
        }
    }
    return result;
}

template<typename BaseType>
template<typename SubType>
Reflection<BaseType>::TypeRegister<SubType>::TypeRegister() {
    util::TypeRegisterGuard guard;
    const std::string& type = UniqueTypeName<SubType>();

    // the initialation order of global-scoped variables are random, so
    // we define s_factories as a pointer, to avoid s_factory constructs
    // before s_factories.
    if (NULL == s_factories) {
        s_factories = new FactoryMap;
    }
    typename FactoryMap::iterator it = s_factories->find(type);
    if (it != s_factories->end()) {
        // In general software design, new statement should be placed inside RegisterFactory.
        // To keep symbol consistency during dlopen, we new factory in TypeTegister.
        it->second->RegisterFactory(this, new ObjectFactory<SubType>());
    } else {
        const size_t name_length = type.length() + 1;
        char* name = new char[name_length];
        memcpy(name, type.c_str(), name_length);
        Type<SubType>::s_name = name;

        FactoryHolder* holder = new FactoryHolder();
        holder->RegisterFactory(this, new ObjectFactory<SubType>());

        s_factories->insert(std::make_pair(type, holder));
    }
}

template<typename BaseType>
template<typename SubType>
Reflection<BaseType>::TypeRegister<SubType>::~TypeRegister() {
    util::TypeRegisterGuard guard;
    const std::string& type = UniqueTypeName<SubType>();

    typename FactoryMap::iterator it = s_factories->find(type);
    if (it != s_factories->end()) {
        it->second->RemoveFactory(this);
        if (it->second->GetFactory() == NULL) {
            delete it->second;
            s_factories->erase(it);

            delete[] Type<SubType>::s_name;
            Type<SubType>::s_name = NULL;
        }
    }
    if (s_factories->empty()) {
        delete s_factories;
        s_factories = NULL;
    }
}

template<typename BaseType>
template<typename SubType>
const std::string Reflection<BaseType>::UniqueTypeName() {
    // Usually PrettyTypeName will return unique name for each type.
    // However, if two cc files all define types with same name inside
    // unnamed namespace, they will conflict each other.
    std::string type = PrettyTypeName<SubType>();
    static const char kUnNamed[] = FLUME_ANONYMOUT_PREFIX;
    size_t start_index = type.find(kUnNamed);
    while (start_index != std::string::npos) {
        type = type.replace(start_index,
                            sizeof(kUnNamed) - 1,   // skip remain tailing "\0"
                            __BASE_FILE__);         // replace with filename
        start_index = type.find(kUnNamed);
    }
    return type;
}

// TODO(flyingwen): multiple compiler support
// We make use of gcc specific macro: __PRETTY_FUNCTION__, it only print function
// name. If T is int, then __PRETTY_FUNCTION__ returns something likes
// "void PrettyTypeName<T>() [with T = int]". We extract the real type name
// 'int' and return.
template<typename T>
const std::string PrettyTypeName() {
    const char* begin_ptr = __PRETTY_FUNCTION__;

    // skip after typename `T'
    bool found = false;
#if defined(__clang__)
    const char preceding_char = '[';
#else
    const char preceding_char = ' ';
#endif
    bool preceding_char_matched = false;
    while (!found && '\0' != *begin_ptr) {
        switch (*begin_ptr) {
        case preceding_char:
            preceding_char_matched = true;
            break;

        case 'T':
            if (preceding_char_matched && (' ' == *(begin_ptr + 1))) {
                found = true;
            }
            // fall through
        default:
            preceding_char_matched = false;
        }
        ++begin_ptr;  // skip `T' itself if found
    }

    // skip whitespace after typename `T'
    while (' ' == *begin_ptr) {
        ++begin_ptr;
    }

    // skip `=' sign
    if ('=' == *begin_ptr) {
        ++begin_ptr;
    }

    // skip whitespace after `='
    while (' ' == *begin_ptr) {
        ++begin_ptr;
    }

    const char* end_ptr = begin_ptr;
    while (']' != *end_ptr && ';' != *end_ptr && '\0' != *end_ptr) {
        ++end_ptr;
    }

    return std::string(begin_ptr, end_ptr);
}

template<typename T>
const std::string PrettyTypeName(const T& t) {
    return PrettyTypeName<T>();
}

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_UTIL_REFLECTION_H_
