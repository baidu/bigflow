/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
/**
* @author:      zhangyuncong (bigflow-opensource@baidu.com)
*/

#include "bigflow_python/processors/accumulate_processor.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "flume/core/iterator.h"
#include "flume/core/testing/mock_emitter.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/python_interpreter.h"

namespace baidu {
namespace bigflow {
namespace python {

class AccumulateProcessorTest : public ::testing::Test {
protected:
    AccumulateProcessorTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};

class TestZeroFn : public Functor {
public:
    virtual void setup(const std::string& config) {
    }

    virtual void call(void* object, flume::core::Emitter* emitter) {
        boost::python::object zero(0);
        emitter->Emit(&zero);
    }
};

class TestAccumulateFn : public Functor {
public:
    virtual void setup(const std::string& config) {
    }

    virtual void call(void* object, flume::core::Emitter* emitter) {
        PythonArgs* args = static_cast<PythonArgs*>(object);
        CHECK_NOTNULL(args->args);
        boost::python::object a = (*args->args)[0];
        boost::python::object b = (*args->args)[1];
        boost::python::object c = a + b;
        emitter->Emit(&c);
    }
};

TEST_F(AccumulateProcessorTest, TestAll) {
    try {
        // register entity
        PbPythonProcessorConfig pb_config;
        baidu::flume::PbEntity* entity = pb_config.add_functor();
        std::string name = flume::Reflection<Functor>::TypeName<TestZeroFn>();
        entity->set_name(name);
        entity->set_config("");
        entity = pb_config.add_functor();
        name = baidu::flume::Reflection<Functor>::TypeName<TestAccumulateFn>();
        entity->set_name(name);
        entity->set_config("");

        std::string config;
        pb_config.SerializeToString(&config);

        flume::MockEmitter<boost::python::object> emitter;
        boost::python::object three(3);
        EXPECT_CALL(emitter, EmitValue(three));
        ProcessorImpl<AccumulateProcessor> impl;
        const char* kKeys[] = {"split1", "split2"};

        std::vector<flume::core::Iterator*> vec_iter;
        vec_iter.push_back(NULL);

        impl.Setup(config);
        impl.BeginGroup(std::vector<toft::StringPiece>(kKeys, kKeys + TOFT_ARRAY_SIZE(kKeys)),
            vec_iter,
            &emitter);

        boost::python::object one(1);
        boost::python::object two(2);

        impl.Process(1, &one);
        impl.Process(1, &two);
        impl.EndGroup();
    }catch(...) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
