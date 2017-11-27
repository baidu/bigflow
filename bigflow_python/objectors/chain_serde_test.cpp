/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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


#include "bigflow_python/objectors/chain_serde.h"

#include <iostream>
#include "gtest/gtest.h"

#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/python_client.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"
#include "bigflow_python/objectors/idl_packet_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

class ChainSerdeTest : public ::testing::Test {
protected:
    ChainSerdeTest() {}
    virtual ~ChainSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
        register_classes();
    }
    virtual void TearDown() {}

    std::string build_python_chain_packet_serde() {
        boost::python::object idl_serde(
                boost::python::import("bigflow.serde").attr("IdlPacketSerde")("log_text"));
        boost::python::object int_type = boost::python::object(0).attr("__class__");
        boost::python::object chain_serde(
                boost::python::import("bigflow.serde").attr("ChainSerde")(int_type, idl_serde));
        return boost::python::extract<std::string>(CPickleSerde().dumps(chain_serde));
    }

    boost::python::object build_types_chain() {
        boost::python::object idl_serde(
                boost::python::import("bigflow.serde").attr("IdlPacketSerde")("log_text"));
        boost::python::object int_type = boost::python::object(0).attr("__class__");

        boost::python::object chain_serde(
                boost::python::import("bigflow.serde").attr("ChainSerde")(int_type, idl_serde));
        return chain_serde;
    }
};

TEST_F(ChainSerdeTest, TestChainSerdeImplementation) {
    try {
        std::string chain_serde_str = build_python_chain_packet_serde();
        ChainSerde* objector = new ChainSerde();
        objector->setup(chain_serde_str);
        const static int BUFFER_SISE = 1024;
        char* serialize_buff = new char[BUFFER_SISE];

        int64_t expect_int = 11235;
        boost::python::object py_int(expect_int);

        int serialize_size = objector->serialize(&py_int, serialize_buff, BUFFER_SISE);
        LOG(ERROR) << "serialize_size:" << serialize_size;
        EXPECT_TRUE(serialize_size > 0);

        boost::python::object* out_int_py =
            static_cast<boost::python::object*>(
                    objector->deserialize(serialize_buff, serialize_size));
        int64_t out_int = boost::python::extract<int64_t>(*out_int_py);
        LOG(ERROR) << "out_int:" << out_int;
        EXPECT_EQ(out_int, expect_int);

        //boost::python::object chain_serde_pyobj = build_types_chain();
        //boost::python::str buf_obj(serialize_buff, serialize_size);
        //boost::python::object out_int_py2 = chain_serde_pyobj.attr("deserialize")(buf_obj);
        //int64_t out_int2 = boost::python::extract<int64_t>(out_int_py2);
        //LOG(ERROR) << "out_int2:" << out_int2;
        //EXPECT_EQ(out_int2, expect_int);
        delete objector;
    } catch (...) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

} // namespace baidu
} // namespace bigflow
} // namespace baidu

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
