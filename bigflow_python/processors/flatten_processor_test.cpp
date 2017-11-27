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
* @filename:    flatmap_processor_test.cpp
* @author:      fangjun02@baidu.com
* @brief:       unittest for flatmap processor
*/

#include "bigflow_python/processors/flatten_processor.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

class TestObjector : public flume::core::Objector {
    virtual void Setup(const std::string& config){}

    // save key to buffer if has enough buffer
    // return key size
    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size) {
        return -1;
    }

    // return an address to deserialized object.
    // buffer will have longger lifetime than returned object.
    virtual void* Deserialize(const char* buffer, uint32_t buffer_size) {
        return new boost::python::long_(boost::python::str(buffer, buffer_size));
    }

    // release resources hold by object
    virtual void Release(void* object){
        delete static_cast<boost::python::object*>(object);
    }
};

class FlattenProcessorTest : public ::testing::Test {
protected:
    FlattenProcessorTest() {}
    virtual ~FlattenProcessorTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};


#define EXPECT_EMIT_TUPLE(left, right) \
    EXPECT_CALL(emitter, \
        EmitValue( \
            boost::python::make_tuple( \
                boost::python::object(left),\
                boost::python::object(right) \
            ) \
        )\
    )


std::string get_config(std::string config) {
    boost::python::str conf_str(config);
    boost::python::object s = CPickleSerde().dumps(conf_str);
    return boost::python::extract<std::string>(s);
}

TEST_F(FlattenProcessorTest, TestFlatten) {
    try {

        const char* kKeys[] = {"123", "456"};
        std::vector<flume::core::Iterator*> vec_iter;
        flume::MockEmitter<boost::python::object> emitter;

        boost::python::object one(1);
        boost::python::object two(2);
        boost::python::object thr(3);

        std::vector<Functor*> fns;
        ProcessorContext context(&emitter, vec_iter);

        FlattenProcessor impl;

        flume::PbEntity entity
                = flume::core::Entity<flume::core::Objector>::Of<TestObjector>("").ToProtoMessage();
        std::string config = get_config(entity.SerializeAsString());
        impl.setup(fns, config);
        impl.begin_group(std::vector<toft::StringPiece>(kKeys, kKeys + TOFT_ARRAY_SIZE(kKeys)),
                        &context);
        {
            testing::InSequence inseq;
            EXPECT_EMIT_TUPLE(456, one);
            EXPECT_EMIT_TUPLE(456, two);
            EXPECT_EMIT_TUPLE(456, thr);
            impl.process(&one);
            impl.process(&two);
            impl.process(&thr);
        }
        impl.end_group();

        impl.begin_group(std::vector<toft::StringPiece>(kKeys, kKeys + TOFT_ARRAY_SIZE(kKeys)),
                        &context);
        {
            testing::InSequence inseq;
            EXPECT_EMIT_TUPLE(456, one);
            EXPECT_EMIT_TUPLE(456, two);
            EXPECT_EMIT_TUPLE(456, thr);
            impl.process(&one);
            impl.process(&two);
            impl.process(&thr);
        }

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
