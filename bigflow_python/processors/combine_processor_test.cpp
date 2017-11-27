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

#include "bigflow_python/processors/combine_processor.h"

#include "boost/iterator/indirect_iterator.hpp"
#include "boost/python/stl_iterator.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "flume/core/iterator.h"
#include "flume/core/testing/mock_emitter.h"

#include "bigflow_python/common/iterator.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/python_interpreter.h"

namespace baidu {
namespace bigflow {
namespace python {

class CombineProcessorTest : public ::testing::Test {
protected:
    CombineProcessorTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};

class TestCombineFn : public Functor {
public:
    virtual void setup(const std::string& config) {
    }

    virtual void call(void* object, flume::core::Emitter* emitter) {
        PythonArgs* args = static_cast<PythonArgs*>(object);
        CHECK_NOTNULL(args->args);
        boost::python::object input = (*args->args)[0];
        uint64_t ret = 0;
        boost::python::stl_input_iterator<int> it(input);
        for (; it != boost::python::stl_input_iterator<int>(); ++it) {
            ret += *it;
        }
        boost::python::object tmp(ret);
        emitter->Emit(&tmp);
    }
};

TEST_F(CombineProcessorTest, TestAll) {
    try {
        // register entity
        PbPythonProcessorConfig pb_config;
        baidu::flume::PbEntity* entity = pb_config.add_functor();
        std::string name = flume::Reflection<Functor>::TypeName<TestCombineFn>();
        entity->set_name(name);
        entity->set_config("");
        pb_config.add_side_input_type(PCOLLECTION_TYPE);
        std::string config;
        pb_config.SerializeToString(&config);

        flume::MockEmitter<boost::python::object> emitter;
        boost::python::object three(3);
        EXPECT_CALL(emitter, EmitValue(three));
        ProcessorImpl<CombineProcessor, 0> impl;
        const char* kKeys[] = {"split1", "split2"};

        std::vector<boost::python::object> objects;
        objects.push_back(boost::python::object(1));
        objects.push_back(boost::python::object(2));
        std::auto_ptr<flume::core::Iterator> iter = baidu::bigflow::python::iterator(
                objects.begin(),
                objects.end()
        );

        std::vector<flume::core::Iterator*> vec_iter;
        vec_iter.push_back(iter.get());

        impl.Setup(config);
        impl.BeginGroup(std::vector<toft::StringPiece>(kKeys, kKeys + TOFT_ARRAY_SIZE(kKeys)),
            vec_iter,
            &emitter);

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
