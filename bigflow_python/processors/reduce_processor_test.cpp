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
* @created:     2015/06/18
* @filename:    reduce_processor_test.cpp
* @author:      fangjun02@baidu.com
* @brief:       unittest for reduce processor
*/

#include "bigflow_python/processors/reduce_processor.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/python_args.h"

#include "flume/core/iterator.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

class TestReduceFn : public Functor {
public:
    virtual void setup(const std::string& config) {}
    virtual void call(void* object, flume::core::Emitter* emitter) {
        PythonArgs* args = static_cast<PythonArgs*>(object);
        CHECK_NOTNULL(args->args);
        boost::python::object a = (*args->args)[0];
        boost::python::object b = (*args->args)[1];
        boost::python::object c = a + b;
        emitter->Emit(&c);
    }
};

class ReduceProcessorTest : public ::testing::Test {
protected:
    ReduceProcessorTest() {}
    virtual ~ReduceProcessorTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};

TEST_F(ReduceProcessorTest, TestReduce) {
    try {
        // register entity
        PbPythonProcessorConfig pb_config;
        baidu::flume::PbEntity* pb_entity = pb_config.add_functor();
        std::string name = baidu::flume::Reflection<Functor>::TypeName<TestReduceFn>();
        pb_entity->set_name(name);
        pb_entity->set_config("");
        std::string config;
        pb_config.SerializeToString(&config);

        const char* kKeys[] = {"split1", "split2"};
        std::vector<flume::core::Iterator*> vec_iter;

        boost::python::object ob1(1);
        boost::python::object ob2(2);

        flume::MockEmitter<boost::python::object> emitter;
        boost::python::object three(3);
        EXPECT_CALL(emitter, EmitValue(three));

        std::vector<Functor*> fns;
        TestReduceFn fn;
        fns.push_back(&fn);
        ProcessorContext context(&emitter, vec_iter);

        ReduceProcessor impl;
        impl.setup(fns, config);
        impl.begin_group(std::vector<toft::StringPiece>(kKeys, kKeys + TOFT_ARRAY_SIZE(kKeys)),
                        &context);
        impl.process(&ob1);
        impl.process(&ob2);
        impl.end_group();
    } catch(...) {
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
