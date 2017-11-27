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

#include <dlfcn.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/util/reflection.h"
#include "flume/util/reflection_test_helper.h"

using baidu::flume::PrettyTypeName;
using baidu::flume::Reflection;

TEST(PrettyTypeName, BasicTypes) {
    int i = 0;
    ASSERT_EQ(std::string("int"), PrettyTypeName(i))
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";
    ASSERT_EQ(std::string("int"), PrettyTypeName<int>())
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";

    unsigned u = 0;
    ASSERT_EQ(std::string("unsigned int"), PrettyTypeName(u))
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";
    ASSERT_EQ(std::string("unsigned int"), PrettyTypeName<unsigned>())
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";

    long l = 0; // NOLINT
    ASSERT_EQ(std::string("long int"), PrettyTypeName(l))
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";
    ASSERT_EQ(std::string("long int"), PrettyTypeName<long>())  // NOLINT
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";

    unsigned long ul = 0;  // NOLINT
    ASSERT_EQ(std::string("long unsigned int"), PrettyTypeName(ul))
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";
    ASSERT_EQ(std::string("long unsigned int"), PrettyTypeName<unsigned long>())  // NOLINT
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";

    float f = 0;
    ASSERT_EQ(std::string("float"), PrettyTypeName(f))
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";
    ASSERT_EQ(std::string("float"), PrettyTypeName<float>())
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";

    double lf = 0;
    ASSERT_EQ(std::string("double"), PrettyTypeName(lf))
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";
    ASSERT_EQ(std::string("double"), PrettyTypeName<double>())
            << "PrettyTypeName behave strangely, maybe because of incompatible compiler";
}

// target types for latter test
namespace pretty {

class SomeClass {
};

struct SomeStruct {
};

template<typename T, int N>
class SomeTemplate {
};
typedef SomeTemplate<int, 1> SomeType;

}  // namespace pretty

namespace {

class UnNamedClass {
};

}  // namespace

TEST(PrettyTypeName, CombineType) {
    using pretty::SomeClass;
    using pretty::SomeStruct;
    using pretty::SomeTemplate;
    using pretty::SomeType;

    // pretty class name
    SomeClass c;
    ASSERT_EQ(std::string("pretty::SomeClass"), PrettyTypeName(c))
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
    ASSERT_EQ(std::string("pretty::SomeClass"), PrettyTypeName<SomeClass>())
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";

    // pretty struct name
    SomeStruct s;
    ASSERT_EQ(std::string("pretty::SomeStruct"), PrettyTypeName(s))
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
    ASSERT_EQ(std::string("pretty::SomeStruct"), PrettyTypeName<SomeStruct>())
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";

    // pretty template name
    SomeTemplate<SomeClass, 2> t;
    ASSERT_EQ(std::string("pretty::SomeTemplate<pretty::SomeClass, 2>"),
              PrettyTypeName(t))
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
    // ',' inside template parameter list will mislead c preprocessor, avoid it
    const std::string &type = PrettyTypeName<SomeTemplate<int, 2> >();
    ASSERT_EQ(std::string("pretty::SomeTemplate<int, 2>"), type)
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";

    // pretty typedef name. Note that it will return the typedef name, not
    // origin name.
    SomeType d;
#if FLUME_GCC_VERSION == 30405
    ASSERT_EQ(std::string("pretty::SomeType"), PrettyTypeName(d))
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
    ASSERT_EQ(std::string("pretty::SomeType"), PrettyTypeName<SomeType>())
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
#elif FLUME_GCC_VERSION == 40802
    ASSERT_EQ(std::string("pretty::SomeTemplate<int, 1>"), PrettyTypeName(d))
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
    ASSERT_EQ(std::string("pretty::SomeTemplate<int, 1>"), PrettyTypeName<SomeType>())
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
#endif // FLUME_GCC_VERSION

    // class name inside unnamed namespace
    UnNamedClass u;
    ASSERT_EQ(std::string(FLUME_ANONYMOUT_PREFIX "::UnNamedClass"), PrettyTypeName(u))
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
    ASSERT_EQ(std::string(FLUME_ANONYMOUT_PREFIX "::UnNamedClass"),
              PrettyTypeName<UnNamedClass>())
            << "PrettyTypeName behave strangely, maybe because incompatible compiler";
}

TEST(ReflectionTest, EmptyReflection) {
    Fn *fn = Reflection<Fn>::New("NULL");
    ASSERT_TRUE(NULL == fn);

    TFn<float> *tfn = Reflection< TFn<float> >::New("NULL");
    ASSERT_TRUE(NULL == tfn);
}

class NormalFn : public Fn {
public:
    virtual std::string ToString() {
        return "normal";
    }

    virtual ~NormalFn() {}
};

namespace inside {

class InsideFn : public Fn {
public:
    virtual std::string ToString() {
        return "inside";
    }

    virtual ~InsideFn() {}
};

}  // namespace inside

TEST(ReflectionTest, BasicReflection) {
    Fn *ptr = NULL;

    ASSERT_EQ(std::string("NormalFn"), Reflection<Fn>::TypeName<NormalFn>());
    ptr = Reflection<Fn>::New("NormalFn");
    ASSERT_TRUE(NULL != ptr);
    ASSERT_EQ(std::string("normal"), ptr->ToString());
    delete ptr;
    ptr = NULL;

    ASSERT_EQ(std::string("inside::InsideFn"),
              Reflection<Fn>::TypeName<inside::InsideFn>());
    ptr = Reflection<Fn>::New("inside::InsideFn");
    ASSERT_TRUE(NULL != ptr);
    ASSERT_EQ(std::string("inside"), ptr->ToString());
    delete ptr;
    ptr = NULL;
}

template<int N>
class IntFn : public TFn<int> {
public:
    virtual int Value() {
        return N;
    }

    virtual ~IntFn() {}
};

TEST(ReflectionTest, TemplateReflection) {
    ASSERT_EQ(std::string("IntFn<5>"),
                          Reflection<TFn<int> >::TypeName<IntFn<5> >());
    TFn<int> *five = Reflection<TFn<int> >::New("IntFn<5>");
    ASSERT_TRUE(NULL != five);
    ASSERT_EQ(5, five->Value());
    delete five;
    five = NULL;
};

class UnCalledFn : public Fn {
public:
    virtual std::string ToString() {
        return "uncalled";
    }

    virtual ~UnCalledFn() {}
};

void UnCalledFunction() {
    Reflection<Fn>::TypeName<UnCalledFn>();
}

TEST(ReflectionTest, ReflectionWithoutCall) {
    // Do ***NOT*** need to execuate TypeName()
    // UnCalledTypeName();
    Fn *ptr = Reflection<Fn>::New("UnCalledFn");
    ASSERT_TRUE(NULL != ptr);
    ASSERT_EQ(std::string("uncalled"), ptr->ToString());
    delete ptr;
    ptr = NULL;
}

namespace {

class UnNamedFn : public Fn {
public:
    virtual std::string ToString() {
        return "local-unnamed-fn";
    }

    virtual ~UnNamedFn() {}
};

}  // namespace

TEST(ReflectionTest, UnNamedReflection) {
    // unnamed typename would look like: "<path/to/source.cpp>::UnNamedFn",
    // user should not rely on 'guessed' name, always use the return value of
    // TypeName.

    std::string helper_typename_1 = GetUnNamedType();
    std::string helper_typename_2 = GetUnNamedType();
    ASSERT_EQ(helper_typename_1, helper_typename_2);
    Fn *helper_ptr = Reflection<Fn>::New(helper_typename_1);
    ASSERT_TRUE(NULL != helper_ptr);
    ASSERT_EQ(std::string("helper-unnamed-fn"), helper_ptr->ToString());
    delete helper_ptr;
    helper_ptr = NULL;

    std::string local_typename = Reflection<Fn>::TypeName<UnNamedFn>();
    ASSERT_NE(helper_typename_1, local_typename);
    Fn *local_ptr = Reflection<Fn>::New(local_typename);
    ASSERT_TRUE(NULL != local_ptr);
    ASSERT_EQ(std::string("local-unnamed-fn"), local_ptr->ToString());
    delete local_ptr;
    local_ptr = NULL;
}

TEST(ReflectionTest, TypeList) {
    const std::vector<std::string> &fn_list = Reflection<Fn>::TypeList();
    ASSERT_EQ(6U, fn_list.size());
    ASSERT_TRUE(std::find(fn_list.begin(), fn_list.end(),
                          "NormalFn") != fn_list.end());
    ASSERT_TRUE(std::find(fn_list.begin(), fn_list.end(),
                          "CommonFn") != fn_list.end());
    ASSERT_TRUE(std::find(fn_list.begin(), fn_list.end(),
                          "inside::InsideFn") != fn_list.end());
    ASSERT_TRUE(std::find(fn_list.begin(), fn_list.end(),
                          "UnCalledFn") != fn_list.end());
    ASSERT_TRUE(std::find(fn_list.begin(), fn_list.end(),
                          GetUnNamedType()) != fn_list.end());
    ASSERT_TRUE(std::find(fn_list.begin(), fn_list.end(),
                          Reflection<Fn>::TypeName<UnNamedFn>()) != fn_list.end());

    const std::vector<std::string> &tfn_list = Reflection< TFn<int> >::TypeList();
    ASSERT_EQ(1U, tfn_list.size());
    ASSERT_EQ("IntFn<5>", tfn_list[0]);

    const std::vector<std::string> &empty_list =
        Reflection< TFn<float> >::TypeList();
    ASSERT_EQ(0U, empty_list.size());
}

TEST(ReflectionTest, Dlopen) {
    toft::scoped_ptr<Fn> fn;

    ASSERT_EQ(6U, Reflection<Fn>::TypeList().size());

    fn.reset(Reflection<Fn>::New("CommonFn"));
    ASSERT_TRUE(NULL != fn.get());
    ASSERT_EQ(std::string("common-fn"), fn->ToString());
    fn.reset(NULL);

    fn.reset(Reflection<Fn>::New("DlopenFn"));
    ASSERT_TRUE(NULL == fn.get());

    LOG(INFO) << "load so 01" << std::endl;
    void* handle1 = dlopen("testdata/libreflection_dlopen_test_so_01.so", RTLD_NOW | RTLD_LOCAL);
    ASSERT_TRUE(NULL != handle1) << dlerror();
    ASSERT_EQ(7U, Reflection<Fn>::TypeList().size());

    fn.reset(Reflection<Fn>::New("DlopenFn"));
    ASSERT_TRUE(NULL != fn.get());
    ASSERT_EQ(std::string("dlopen-fn"), fn->ToString());
    fn.reset(NULL);

    LOG(INFO) << "load so_02" << std::endl;
    void* handle2 = dlopen("testdata/libreflection_dlopen_test_so_02.so", RTLD_NOW | RTLD_LOCAL);
    ASSERT_TRUE(NULL != handle2) << dlerror();
    ASSERT_EQ(7U, Reflection<Fn>::TypeList().size());

    // NOTICE: if we unload so_01 here, the following reflection may core
    LOG(INFO) << "unload so_02" << std::endl;
    int ret2 = dlclose(handle2);
    ASSERT_EQ(0, ret2) << dlerror();
    ASSERT_EQ(7U, Reflection<Fn>::TypeList().size());

    fn.reset(Reflection<Fn>::New("DlopenFn"));
    ASSERT_TRUE(NULL != fn.get());
    ASSERT_EQ(std::string("dlopen-fn"), fn->ToString());
    fn.reset(NULL);

    fn.reset(Reflection<Fn>::New("CommonFn"));
    ASSERT_TRUE(NULL != fn.get());
    ASSERT_EQ(std::string("common-fn"), fn->ToString());
    fn.reset(NULL);

    // TODO:
    // LOG(INFO) << "unload so 01" << std::endl;
    // int ret1 = dlclose(handle1);
    // ASSERT_EQ(0, ret1) << dlerror();
    // ASSERT_EQ(6U, Reflection<Fn>::TypeList().size());

    // fn.reset(Reflection<Fn>::New("DlopenFn"));
    // ASSERT_TRUE(NULL == fn.get());

    // fn.reset(Reflection<Fn>::New("CommonFn"));
    // ASSERT_TRUE(NULL != fn.get());
    // ASSERT_EQ(std::string("common-fn"), fn->ToString());
    // fn.reset(NULL);
}
