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
// Author: Wang Cong <wangcong09@baidu.com>

#include "toft/storage/seqfile/local_sequence_file_writer.h"

namespace baidu {
namespace bigflow {
namespace service {

void ServiceImpl::write_local_seqfile(
        ::google::protobuf::RpcController* controller,
        const WriteLocalSeqFileRequest* request,
        VoidResponse* response,
        ::google::protobuf::Closure* done) {
    LOG(INFO) << "Received writing request: " << request->file_path();
    typedef toft::LocalSequenceFileWriter Writer;

    ResponseStatus* status = response->mutable_status();
    status->set_success(true);

    CHECK(request->key_size() == request->value_size());
    toft::scoped_ptr<toft::File> file(toft::File::Open(request->file_path(), "w"));
    toft::scoped_ptr<Writer> writer(new Writer(file.release()));
    CHECK(writer->Init());

    for (int i = 0; i < request->key_size(); ++i) {
        writer->WriteRecord(request->key(i), request->value(i));
    }
    if (!writer->Close()) {
        status->set_success(false);
        status->set_reason("Error closing file");
    }

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

}  // namespace service
}  // namespace bigflow
}  // namespace baidu

