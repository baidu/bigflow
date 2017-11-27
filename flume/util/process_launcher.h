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
// Author: Wen Xiang <wenxiang@baidu.com>
//

#ifndef FLUME_UTIL_PROCESS_LAUNCHER_H_
#define FLUME_UTIL_PROCESS_LAUNCHER_H_

#include <string>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/system/process/sub_process.h"
#include "toft/system/threading/thread.h"

namespace baidu {
namespace flume {
namespace util {

class ProcessLauncher {
public:
    typedef toft::Closure<void (const std::string&)> InfoReader;

    ProcessLauncher();
    ~ProcessLauncher();

    int Run(const std::vector<std::string>& args,
            toft::SubProcess::CreateOptions options,
            const std::string& log_title,
            InfoReader* stdout_reader,
            InfoReader* stderr_reader);

    int RunAndWaitExit(const std::vector<std::string>& args,
                       toft::SubProcess::CreateOptions options,
                       const std::string& log_title,
                       InfoReader* stdout_reader,
                       InfoReader* stderr_reader);

    void Stop();

    bool Terminate();

    bool IsAlive();

private:
    struct OutputChannel {
        int write_fd;
        bool is_closed;
        toft::scoped_ptr<std::streambuf> streambuf;
        toft::scoped_ptr<std::istream> stream;
        toft::scoped_ptr<toft::Closure<void (const std::string&)> > reader;
        toft::Thread thread;

        OutputChannel() : write_fd(-1), is_closed(false) {}

        ~OutputChannel() {
            if (write_fd >= 0) close(write_fd);
        }
    };

private:
    OutputChannel* CreateOutputChannel(const std::string& title,
                                       toft::Closure<void (const std::string&)>* reader);

    void CloseOutputChannel(OutputChannel* channel);

    void OutputReadRoutine(const std::string& title, OutputChannel* channel);

private:
    toft::SubProcess m_process;
    toft::scoped_ptr<OutputChannel> m_stdout_channel;
    toft::scoped_ptr<OutputChannel> m_stderr_channel;
};

}  // namespace util
}  // namespace flume
}  // namespace baidu

#endif // FLUME_UTIL_PROCESS_LAUNCHER_H_
