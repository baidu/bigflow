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
// Author: Pan Yunhong <bigflow-opensource@baidu.com>
//

#include "bigflow_python/python_interpreter.h"

#include <iostream>

#include "gtest/gtest.h"

#include "bigflow_python/python_client.h"
#include "bigflow_python/objectors/set_serde.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class SetSerdeTest : public ::testing::Test {
protected:
    SetSerdeTest() {}
    virtual ~SetSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }
    std::string build_python_set_serde(const std::string& type) {
        boost::python::object set_python_serde;
        if (type == "str") {
            boost::python::object str_type = boost::python::str().attr("__class__");
            set_python_serde =
                boost::python::import("bigflow.serde").attr("SetSerde")(str_type);
        } else {
            boost::python::object int_type = boost::python::object(0).attr("__class__");
            set_python_serde =
                boost::python::import("bigflow.serde").attr("SetSerde")(int_type);
        }
        return boost::python::extract<std::string>(CPickleSerde().dumps(set_python_serde));
    }
    boost::python::object build_set_serde_obj(const std::string& type) {
        if (type == "str") {
            boost::python::object str_type = boost::python::str().attr("__class__");
            return boost::python::import("bigflow.serde").attr("SetSerde")(str_type);
        } else {
            boost::python::object int_type = boost::python::object(0).attr("__class__");
            return boost::python::import("bigflow.serde").attr("SetSerde")(int_type);
        }
    }
    template<typename T>
    boost::python::object build_set(const std::vector<T>& input_strs) {
        boost::python::object set_obj((boost::python::handle<>(PySet_New(NULL))));
        for (size_t i = 0; i < input_strs.size(); ++i) {
            boost::python::object set_element(input_strs[i]);
            PySet_Add(set_obj.ptr(), set_element.ptr());
        }
        return set_obj;
    }
};

TEST_F(SetSerdeTest, TestCppAndPythonImplementationConsistentForInt) {
    try {
        std::string set_python_serde_str = build_python_set_serde("int");
        PythonObjector* objector = new SetSerde();
        objector->setup(set_python_serde_str);

        const static int BUFFER_SIZE = 128;
        char *serialize_buf = new char[BUFFER_SIZE];
        std::vector<int64_t> expect_ints;
        expect_ints.push_back(123456);
        expect_ints.push_back(654321);
        expect_ints.push_back(0);
        expect_ints.push_back(-1);
        expect_ints.push_back(-10);

        boost::python::object set_obj = build_set(expect_ints);
        uint32_t serialize_size = objector->serialize(&set_obj, serialize_buf, BUFFER_SIZE);
        EXPECT_GT(serialize_size, 0);
        boost::python::str buf_obj(serialize_buf, serialize_size);
        LOG(INFO) << "serialize int set size = " << serialize_size;

        boost::python::object set_serde_obj = build_set_serde_obj("int");
        boost::python::object set_deser_obj = set_serde_obj.attr("deserialize")(buf_obj);

        PyObject* it = PyObject_GetIter(set_deser_obj.ptr());
        PyObject* value = NULL;
        std::vector<int64_t> results;
        while ((value = PyIter_Next(it)) != NULL) {
            boost::python::object item = boost::python::object(boost::python::handle<>(value));
            int64_t item_int = boost::python::extract<int64_t>(item);
            results.push_back(item_int);
            LOG(INFO) << "get item int = " << item_int;
        }
        sort(expect_ints.begin(), expect_ints.end());
        sort(results.begin(), results.end());
        EXPECT_EQ(expect_ints, results);

        delete objector;
    } catch (...) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

TEST_F(SetSerdeTest, TestCppAndPythonImplementationConsistentForStr) {
    try {
        std::string set_python_serde_str = build_python_set_serde("str");
        PythonObjector* objector = new SetSerde();
        objector->setup(set_python_serde_str);

        const static int BUFFER_SIZE = 128;
        char *serialize_buf = new char[BUFFER_SIZE];
        std::vector<std::string> expect_strs;
        expect_strs.push_back("ghjklmnopqrst");
        expect_strs.push_back("abcdefg");
        expect_strs.push_back("1234");
        expect_strs.push_back("a");

        boost::python::object set_obj = build_set(expect_strs);
        uint32_t serialize_size = objector->serialize(&set_obj, serialize_buf, BUFFER_SIZE);
        EXPECT_GT(serialize_size, 0);
        boost::python::str buf_obj(serialize_buf, serialize_size);

        boost::python::object set_serde_obj = build_set_serde_obj("str");
        boost::python::object set_deser_obj = set_serde_obj.attr("deserialize")(buf_obj);

        PyObject* it = PyObject_GetIter(set_deser_obj.ptr());
        PyObject* value = NULL;
        std::vector<std::string> results;
        while ((value = PyIter_Next(it)) != NULL) {
            boost::python::object item = boost::python::object(boost::python::handle<>(value));
            std::string item_str = boost::python::extract<std::string>(item);
            results.push_back(item_str);
        }
        sort(expect_strs.begin(), expect_strs.end());
        sort(results.begin(), results.end());
        EXPECT_EQ(expect_strs, results);

        delete objector;
    } catch (...) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

TEST_F(SetSerdeTest, TestSetStrSerde) {
    try {
        std::string set_python_serde_str = build_python_set_serde("str");
        PythonObjector* objector = new SetSerde();
        objector->setup(set_python_serde_str);

        const static int BUFFER_SIZE = 128;
        char *serialize_buf = new char[BUFFER_SIZE];
        std::string str1 = "ghjklmnopqrst";
        std::string str2 = "abcdefg";
        std::vector<std::string> expect_strs;
        expect_strs.push_back(str1);
        expect_strs.push_back(str2);

        boost::python::object set_obj = build_set(expect_strs);
        uint32_t serialize_size = objector->serialize(&set_obj, serialize_buf, BUFFER_SIZE);
        EXPECT_GT(serialize_size, 0);

        void* deserialized = objector->deserialize(serialize_buf, serialize_size);
        boost::python::object* deserialize_obj = static_cast<boost::python::object*>(deserialized);

        PyObject* set_value = NULL;
        std::vector<std::string> actual_strs;
        PyObject *it = PyObject_GetIter(deserialize_obj->ptr());
        while ((set_value = PyIter_Next(it)) != NULL) {
            boost::python::object set_item = \
                boost::python::object(boost::python::handle<>(set_value));
            std::string set_value_str = boost::python::extract<std::string>(set_item);
            actual_strs.push_back(set_value_str);
        }
        sort(actual_strs.begin(), actual_strs.end());
        sort(expect_strs.begin(), expect_strs.end());
        ASSERT_EQ(actual_strs.size(), expect_strs.size());
        for (size_t i = 0; i < actual_strs.size(); ++i) {
            ASSERT_EQ(actual_strs[i], expect_strs[i]);
        }

        objector->release(deserialized);
        delete objector;
    } catch (...) {
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
