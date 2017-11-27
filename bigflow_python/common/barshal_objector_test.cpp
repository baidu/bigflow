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
* @created:     2015/07/21
* @filename:    barshal_objector_test.cpp
* @author:      panyunhong@baidu.com
* @brief:       unittest for barshal objector
*/

#include "bigflow_python/python_interpreter.h"

#include <iostream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "toft/base/array_size.h"
#include "toft/base/scoped_ptr.h"

#include "bigflow_python/common/barshal_objector.h"

#include "flume/core/iterator.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

class BarshalObjectorTest : public ::testing::Test {
protected:
    BarshalObjectorTest() {}
    virtual ~BarshalObjectorTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
        _objector.reset(new BarshalObjector());
        _objector->Setup("");
    }

    virtual void TearDown() {
    }
    toft::scoped_ptr<flume::core::Objector> _objector;
};


TEST_F(BarshalObjectorTest, TestIntObjectSerialize) {
    try {
        boost::python::object key(100);
        const static int BUFFER_SIZE = 128;
        char buffer[BUFFER_SIZE];
        uint32_t serialize_size = _objector->Serialize(&key, buffer, BUFFER_SIZE);
        ASSERT_EQ(serialize_size, 5);

        boost::python::object* deserialized =
            (boost::python::object *)_objector->Deserialize(buffer, serialize_size);
        int key_int = boost::python::extract<int>(*deserialized);
        ASSERT_EQ(key_int, 100);
        _objector->Release(deserialized);

    } catch(boost::python::error_already_set&) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

TEST_F(BarshalObjectorTest, TestStringObjectSerialize) {
    try {
        std::string test_str = "abcdefgjksdfs";
        boost::python::object key(test_str);
        const static int BUFFER_SIZE = 128;
        char buffer[BUFFER_SIZE];
        uint32_t serialize_size = _objector->Serialize(&key, buffer, BUFFER_SIZE);
        //serialize buffer compose of one byte of data type, 4 bytes string len and
        //the string
        ASSERT_EQ(serialize_size, 5+test_str.length());

        boost::python::object* deserialized =
            (boost::python::object *)_objector->Deserialize(buffer, serialize_size);
        std::string key_str = boost::python::extract<std::string>(*deserialized);
        ASSERT_EQ(key_str, test_str);
        _objector->Release(deserialized);

    } catch(boost::python::error_already_set&) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

TEST_F(BarshalObjectorTest, TestTupleObjectSerialize) {
    try {
        std::string test_str = "abcdefgjksdfs";
        boost::python::tuple key = boost::python::make_tuple(123, 345);

        const static int BUFFER_SIZE = 128;
        char buffer[BUFFER_SIZE];
        uint32_t serialize_size = _objector->Serialize(&key, buffer, BUFFER_SIZE);

        unsigned one_element_size = 5;
        //tuple serialize buffer is compose of 1 byte flag, 4 bytes tuple len and
        //the sum of elements len
        ASSERT_EQ(serialize_size, one_element_size*2+1+4);

        boost::python::object* deserialized =
            (boost::python::object *)_objector->Deserialize(buffer, serialize_size);
        int first_element = boost::python::extract<int>((*deserialized)[0]);
        ASSERT_EQ(first_element, 123);
        _objector->Release(deserialized);

    } catch(boost::python::error_already_set&) {
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
