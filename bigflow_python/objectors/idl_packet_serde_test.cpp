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

#include "bigflow_python/objectors/idl_packet_serde.h"
#include "gtest/gtest.h"

#include "bigflow_python/objectors/packet.idl.h"

#include <iostream>
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/python_client.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class IDLPacketSerdeTest : public ::testing::Test {
protected:
    IDLPacketSerdeTest() {}
    virtual ~IDLPacketSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }
    virtual void TearDown() {}

    std::string build_python_idl_packet_serde(const std::string& log_type) {
        boost::python::object idl_packet_serde =
            boost::python::import("bigflow.serde").attr("IDLPacketSerde")(log_type);

        return boost::python::extract<std::string>(CPickleSerde().dumps(idl_packet_serde));
    }

    boost::python::object build_idl_packet_serde_obj(const std::string& log_type) {
        return boost::python::import("bigflow.serde").attr("IDLPacketSerde")(log_type);
    }
};

TEST_F(IDLPacketSerdeTest, TestIDLPacketSerdeForLOGTEXT) {
    try {
        std::string idl_packet_serde_str = build_python_idl_packet_serde("log_text");
        PythonObjector* objector = new IdlPacketSerde();
        objector->setup(idl_packet_serde_str);

        const static int BUFFER_SISE = 1024;
        char* serialize_buff = new char[BUFFER_SISE];

        std::string text_info = "idl_packet_test_text_info";
        boost::python::object str_obj =
            boost::python::str(text_info.data(), text_info.size());
        uint32_t serialize_size = objector->serialize(&str_obj, serialize_buff, BUFFER_SISE);
        LOG(ERROR) << "serialize_size:" << serialize_size;
        EXPECT_GT(serialize_size, 0);

        boost::python::object* out_text =
            static_cast<boost::python::object*>(
                    objector->deserialize(serialize_buff, serialize_size));
        char* tmp_buff = NULL;
        int64_t size = 0;
        PyString_AsStringAndSize(out_text->ptr(), &tmp_buff, &size);
        std::string out_string(tmp_buff, size);
        LOG(ERROR) << "deserialize:" << out_string;
        EXPECT_STREQ(out_string.c_str(), text_info.c_str());
    } catch (...) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

} // namespace python
} // namespace bigflow
} // namespace baidu

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
