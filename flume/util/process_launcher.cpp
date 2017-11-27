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

#include "flume/util/process_launcher.h"

#include <unistd.h>
#include <arpa/inet.h>
#ifndef __APPLE__
#include <ext/stdio_filebuf.h>
#endif
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <cstdlib>
#include <iostream>  // NOLINT
#include <vector>

#include "glog/logging.h"
#include "toft/system/process/sub_process.h"
#include "toft/system/process/this_process.h"

// the retry times of launching sub process
static const int RETRY_TIMES = 5;

namespace baidu {
namespace flume {
namespace util {

ProcessLauncher::ProcessLauncher() {}

ProcessLauncher::~ProcessLauncher() {
    Terminate();
}

int ProcessLauncher::RunAndWaitExit(const std::vector<std::string>& args,
                                    toft::SubProcess::CreateOptions options,
                                    const std::string& title,
                                    InfoReader* stdout_reader,
                                    InfoReader* stderr_reader) {
    int ret = Run(args, options, title, stdout_reader, stderr_reader);
    if (ret != 0 || !m_process.WaitForExit()) {
        return -1;
    }

    CloseOutputChannel(m_stdout_channel.get());
    (*m_stdout_channel).thread.Join();

    CloseOutputChannel(m_stderr_channel.get());
    (*m_stderr_channel).thread.Join();

    return m_process.ExitCode();
}

int ProcessLauncher::Run(const std::vector<std::string>& args,
                         toft::SubProcess::CreateOptions options,
                         const std::string& title,
                         InfoReader* stdout_reader,
                         InfoReader* stderr_reader) {
    m_stdout_channel.reset(CreateOutputChannel(title + " STDOUT: ", stdout_reader));
    options.RedirectStdOutput(m_stdout_channel->write_fd);

    m_stderr_channel.reset(CreateOutputChannel(title + " STDERR: ", stderr_reader));
    options.RedirectStdError(m_stderr_channel->write_fd);

    for (int i = 0; i < RETRY_TIMES; ++i) {
        if(m_process.Create(args, options)) {
            return 0;
        }
        LOG(WARNING) << "launch sub process failed, retry " << i << " times";
        sleep(1 * RETRY_TIMES);
    }
    return -1;
}

ProcessLauncher::OutputChannel*
ProcessLauncher::CreateOutputChannel(const std::string& title,
                                     toft::Closure<void (const std::string&)>* reader) {
    toft::scoped_ptr<OutputChannel> channel(new OutputChannel());

    int fds[2];
    PLOG_IF(FATAL, pipe(fds) != 0) << "Fail to create pipe: ";

    channel->is_closed = false;
    channel->write_fd = fds[1];
    // todo: remove stdio_filebuf, we can use boost stream instead
#ifndef __APPLE__
    channel->streambuf.reset(new __gnu_cxx::stdio_filebuf<char>(fds[0], std::ios::in));
#else
    channel->streambuf.reset();
#endif
    channel->stream.reset(new std::istream(channel->streambuf.get()));
    channel->reader.reset(reader);
    channel->thread.Start(std::bind(&ProcessLauncher::OutputReadRoutine,
                                    this,
                                    title,
                                    channel.get()));

    return channel.release();
}

void ProcessLauncher::CloseOutputChannel(OutputChannel* channel) {
    if (channel->write_fd != -1) {
        close(channel->write_fd);
        channel->write_fd = -1;
    }
    channel->is_closed = true;
}

void ProcessLauncher::OutputReadRoutine(const std::string& title, OutputChannel* channel) {
    std::string line;
    while (std::getline(*channel->stream, line)) {
        if (channel->is_closed) {
            return;
        }
        LOG(INFO) << title << line;
        if (channel->reader) {
            channel->reader->Run(line);
        }
    }
}

bool ProcessLauncher::IsAlive() {
    return m_process.IsAlive();
}

void ProcessLauncher::Stop() {
    Terminate();
}

bool ProcessLauncher::Terminate() {
    if (m_process.IsAlive()) {
        LOG(INFO) << "SubProcess is Alive, Terminating...";
        CloseOutputChannel(m_stdout_channel.get());
        CloseOutputChannel(m_stderr_channel.get());
        if (!m_process.Terminate()) {
            LOG(WARNING) << "Process Terminate failed! Send SIGKILL";
            m_process.SendSignal(9);
            return false;
        }

        LOG(INFO) << "SubProcess Terminate successfully!";
    }

    return true;
}

}  // namespace util
}  // namespace flume
}  // namespace baidu


