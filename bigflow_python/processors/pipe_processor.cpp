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
* @created:     2015/06/19
* @filename:    pipe_processor.cpp
* @author:      xuyao02@baidu.com
* @brief:       pipe processor implementation
*/

#include "bigflow_python/processors/pipe_processor.h"

#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include "boost/thread/thread.hpp"
#include "boost/algorithm/string.hpp"
#include "boost/lockfree/spsc_queue.hpp"
#include "boost/atomic.hpp"
#include "boost/shared_ptr.hpp"

#include "glog/logging.h"

#include "toft/base/scoped_array.h"
#include "toft/base/string/number.h"
#include "toft/storage/file/file.h"
#include "toft/system/threading/thread.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/functors/functor.h"
#include "bigflow_python/serde/cpickle_serde.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

static bool get_open_fds(pid_t pid, std::list<int>* fds) {
    char fd_path[PATH_MAX];
    size_t path_size = snprintf(fd_path, sizeof(fd_path), "/proc/%d/fd/", pid);
    toft::scoped_ptr<toft::FileIterator> it(toft::File::Iterate(std::string(fd_path, path_size)));
    fds->clear();
    toft::FileEntry entry;
    while (it->GetNext(&entry)) {
        if (entry.type != toft::FileType_Link) {
            continue;
        }
        int fd = -1;
        if (toft::StringToNumber(entry.name, &fd, 10)) {
            fds->push_back(fd);
        }
    }
    return true;
}

static int popen3(
        const char* command,
        FILE** sub_stdin, FILE** sub_stdout, FILE** sub_stderr,
        int* sub_stdin_fd, int* sub_stdout_fd, int* sub_stderr_fd) {
    int out_fd[2];
    int in_fd[2];
    int err_fd[2];

    std::list<int> open_fds;
    CHECK(get_open_fds(::getpid(), &open_fds)) << "get_open_fds failed";

    pipe(out_fd);
    pipe(in_fd);
    pipe(err_fd);

    int pid = fork();
    if (-1 == pid) {
        close(out_fd[0]);
        close(out_fd[1]);
        close(in_fd[0]);
        close(in_fd[1]);
        close(err_fd[0]);
        close(err_fd[1]);
        LOG(FATAL) << "Failed to fork, errno[" << errno << "], "
                << "strerror[" << strerror(errno) << "]";
    } else if (0 == pid) {
        // start subprocessor
        close(0);
        close(1);
        close(2);
        dup2(out_fd[0], 0); // Make the read end of out_fd pipe as stdin
        dup2(in_fd[1], 1); // Make the write end of in_fd pipe as stdout
        dup2(err_fd[1], 2); // stderr

        for (std::list<int>::const_iterator it = open_fds.begin(); it != open_fds.end(); ++it) {
            int fd = *it;
            if (fd > 2) {
                close(fd);
            }
        }

        close(out_fd[0]);
        close(out_fd[1]);
        close(in_fd[0]);
        close(in_fd[1]);
        close(err_fd[0]);
        close(err_fd[1]);

        const char* args[] = {"/bin/bash", "-c", command, NULL};
        if (0 != execv(args[0], (char* const*)args)) {
            LOG(FATAL) << "Failed to exec command[" << command << "], "
                    << "errno[" << errno << "], "
                    << "strerror[" << strerror(errno) << "]";
        }
    } else {
        close(out_fd[0]);
        close(in_fd[1]);
        close(err_fd[1]);
        *sub_stdin = fdopen(out_fd[1], "w");
        *sub_stdout = fdopen(in_fd[0], "r");
        *sub_stderr = fdopen(err_fd[0], "r");
        *sub_stdin_fd = out_fd[1];
        *sub_stdout_fd = in_fd[0];
        *sub_stderr_fd = err_fd[0];
    }

    return pid;
}

void set_fl(int fd, int flags) {
    int val;
    val = fcntl(fd, F_GETFL, 0);
    CHECK_LE(0, val) << "fcntl F_GETFL error, errno[" << errno << "], "
            << "strerror[" << strerror(errno) << "]";

    val |= flags;

    CHECK_LE(0, fcntl(fd, F_SETFL, val)) << "fcntl F_SETFL error, errno[" << errno << "], "
            << "strerror[" << strerror(errno) << "]";
}

void clr_fl(int fd, int flags) {
    int val;
    val = fcntl(fd, F_GETFL, 0);
    CHECK_LE(0, val) << "fcntl F_GETFL error, errno[" << errno << "], "
            << "strerror[" << strerror(errno) << "]";

    val &= ~flags;

    CHECK_LE(0, fcntl(fd, F_SETFL, val)) << "fcntl F_SETFL error, errno[" << errno << "], "
            << "strerror[" << strerror(errno) << "]";
}

class PipeProcessor::Impl : public Processor {
public:
    Impl();
    virtual ~Impl();

public:
    virtual void setup(const std::vector<Functor*>& fns, const std::string& config);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys,
                             ProcessorContext* context);

    virtual void process(void* object);

    virtual void end_group();

private:
    typedef std::vector<std::string> Record;

private:
    void read_stdout_func();
    void read_stderr_func();

    void emit();

    void writeline(const std::vector<toft::StringPiece>& fields);
    void writedata(const std::vector<toft::StringPiece>& fields);

    void write2pipe(const void* buf, size_t count);

    bool readline(Record* record_ptr);
    bool readdata(Record* record_ptr);

    bool get_field(std::string* field);

private:
    ProcessorContext* _context;
    int _sub_pid;
    FILE* _sub_stdin;
    FILE* _sub_stdout;
    FILE* _sub_stderr;
    int _sub_stdin_fd;
    int _sub_stdout_fd;
    int _sub_stderr_fd;

    bool _is_nested_ptype;
    int32_t _input_fields_num;
    int32_t _output_fields_num;
    int32_t _buffer_size;
    std::string _command;
    std::string _field_delimiter;
    std::string _line_delimiter;

    toft::scoped_array<char> _read_buffer;

    std::vector<boost::python::str> _write_str_objs;
    std::vector<toft::StringPiece> _write_str_fields;

    Record* _record_array[8192];
    boost::lockfree::spsc_queue<Record*, boost::lockfree::capacity<8192> > _record_queue;
    boost::atomic<bool> _read_done;

    std::function<void (const std::vector<toft::StringPiece>&)> _write_func;
    std::function<bool (Record*)> _read_func;

    toft::scoped_ptr<toft::Thread> _read_stdout_thread;
    toft::scoped_ptr<toft::Thread> _read_stderr_thread;
};

PipeProcessor::Impl::Impl() :
    _sub_pid(-1),
    _sub_stdin(NULL),
    _sub_stdout(NULL),
    _sub_stderr(NULL),
    _sub_stdin_fd(-1),
    _sub_stdout_fd(-1),
    _sub_stderr_fd(-1) {
}

PipeProcessor::Impl::~Impl() {}

void PipeProcessor::Impl::setup(const std::vector<Functor*>& fns, const std::string& config) {
    boost::python::object obj = CPickleSerde().loads(config);

    // get pipe_processor's is_nested_ptype
    _is_nested_ptype = boost::python::extract<bool>(obj["is_nested_ptype"]);

    // get pipe_processor's command
    _command = boost::python::extract<std::string>(obj["command"]);

    // get pipe_processor's buffer_size
    _buffer_size = boost::python::extract<int32_t>(obj["buffer_size"]);
    _read_buffer.reset(new char[_buffer_size + 1]);

    // get pipe_processor's input_key_num
    _input_fields_num = boost::python::extract<int32_t>(obj["input_fields_num"]);
    CHECK_LE(1, _input_fields_num);

    // get pipe_processor's output_key_num
    _output_fields_num = boost::python::extract<int32_t>(obj["output_fields_num"]);
    CHECK_LE(1, _output_fields_num);

    // get pipe_processor's type
    std::string type = boost::python::extract<std::string>(obj["type"]);
    if (type == "streaming") {
        // get streaming key_delimiter
        _field_delimiter = boost::python::extract<std::string>(obj["field_delimiter"]);

        // get streaming line_delimiter
        _line_delimiter = boost::python::extract<std::string>(obj["line_delimiter"]);

        _write_func = boost::bind(&PipeProcessor::Impl::writeline, this, _1);
        _read_func = boost::bind(&PipeProcessor::Impl::readline, this, _1);
    } else if (type == "bistreaming") {
        _write_func = boost::bind(&PipeProcessor::Impl::writedata, this, _1);
        _read_func = boost::bind(&PipeProcessor::Impl::readdata, this, _1);
    } else {
        CHECK(false) << "Unknown PipeProcessor type[" << type << "]";
    }
}

void PipeProcessor::Impl::begin_group(
        const std::vector<toft::StringPiece>& keys,
        ProcessorContext* context) {
    _context = context;

    _read_stdout_thread.reset(new toft::Thread());
    _read_stderr_thread.reset(new toft::Thread());
    _sub_pid = popen3(_command.c_str(),
            &_sub_stdin, &_sub_stdout, &_sub_stderr,
            &_sub_stdin_fd, &_sub_stdout_fd, &_sub_stderr_fd);

    set_fl(_sub_stdin_fd, O_NONBLOCK);

    _read_stdout_thread->Start(boost::bind(&PipeProcessor::Impl::read_stdout_func, this));
    _read_stderr_thread->Start(boost::bind(&PipeProcessor::Impl::read_stderr_func, this));

    _read_done = false;
}

void PipeProcessor::Impl::process(void* object) {
    boost::python::object obj = *static_cast<boost::python::object*>(object);
    _write_str_objs.clear();
    _write_str_fields.clear();
    if (1 != _input_fields_num) {
        CHECK_EQ(_input_fields_num, object_len(obj));
        if (_is_nested_ptype) {
            // [key1, key2, key3, [value1, value2, value3]]
            for (int32_t i = 0; i < _input_fields_num - 1; ++i) {
                boost::python::str str_obj(list_get_item(obj, i));
                toft::StringPiece str_field = str_get_buf(str_obj);
                _write_str_objs.push_back(str_obj);
                _write_str_fields.push_back(str_field);
            }
            boost::python::object value = list_get_item(obj, _input_fields_num - 1);
            int32_t value_num = object_len(value);
            for (int32_t i = 0; i < value_num; ++i) {
                boost::python::str str_obj(list_get_item(value, i));
                toft::StringPiece str_field = str_get_buf(str_obj);
                _write_str_fields.push_back(str_field);
                _write_func(_write_str_fields);
                _write_str_fields.pop_back();
            }
        } else {
            // (key1, key2, key3, value)
            for (int32_t i = 0; i < _input_fields_num; ++i) {
                boost::python::str str_obj(tuple_get_item(obj, i));
                toft::StringPiece str_field = str_get_buf(str_obj);
                _write_str_objs.push_back(str_obj);
                _write_str_fields.push_back(str_field);
            }
            _write_func(_write_str_fields);
        }
    } else {
        boost::python::str str_obj(obj);
        toft::StringPiece str_field = str_get_buf(str_obj);
        _write_str_fields.push_back(str_field);
        _write_func(_write_str_fields);
    }
    emit();
}

void PipeProcessor::Impl::end_group() {
    clr_fl(_sub_stdin_fd, O_NONBLOCK);

    if (NULL != _sub_stdin) {
        CHECK_EQ(0, fclose(_sub_stdin)) << "Failed to fclose, errno[" << errno << "], "
                << "strerror[" << strerror(errno) << "]";
        _sub_stdin = NULL;
        _sub_stdin_fd = -1;
    }
    while (!_read_done) {
        emit();
        DLOG(INFO) << "record queue ringbuffer is empty, wait 1 millisecond";
        usleep(1000);
    }
    CHECK(_read_stdout_thread->Join());
    if (NULL != _sub_stdout) {
        CHECK_EQ(0, fclose(_sub_stdout)) << "Failed to fclose, errno[" << errno << "], "
                << "strerror[" << strerror(errno) << "]";
        _sub_stdout = NULL;
        _sub_stdout_fd = -1;
    }
    CHECK(_read_stderr_thread->Join());
    if (NULL != _sub_stderr) {
        CHECK_EQ(0, fclose(_sub_stderr)) << "Failed to fclose, errno[" << errno << "], "
                << "strerror[" << strerror(errno) << "]";
        _sub_stderr = NULL;
        _sub_stderr_fd = -1;
    }
    emit();

    int status = 0;
    int pid = waitpid(_sub_pid, &status, 0);
    CHECK_EQ(0, status) << "subprocess aborted, status[" << status
            << "], pid[" << pid << "], subpid[" << _sub_pid
            << "], errno[" << errno
            << "], strerror[" << strerror(errno) << "]";
    LOG(INFO) << "subprocess exited";
}

void PipeProcessor::Impl::writeline(const std::vector<toft::StringPiece>& fields) {
    CHECK_EQ(static_cast<size_t>(_input_fields_num), fields.size());
    std::vector<toft::StringPiece>::const_iterator field_iter = fields.begin();
    while (field_iter != fields.end()) {
            write2pipe(field_iter->data(), field_iter->size());
        ++field_iter;
        if (field_iter != fields.end()) {
            write2pipe(_field_delimiter.data(), _field_delimiter.size());
        } else {
            write2pipe(_line_delimiter.data(), _line_delimiter.size());
        }
    }
}

void PipeProcessor::Impl::writedata(const std::vector<toft::StringPiece>& fields) {
    CHECK_EQ(static_cast<size_t>(_input_fields_num), fields.size());
    uint32_t size = 0;
    std::vector<toft::StringPiece>::const_iterator field_iter = fields.begin();
    while (field_iter != fields.end()) {
        size = field_iter->size();
        write2pipe(&size, sizeof(size));
        if (size != 0) {
            write2pipe(field_iter->data(), size);
        }
        ++field_iter;
    }
}

void PipeProcessor::Impl::write2pipe(const void *buf, size_t count) {
    const char* ptr = static_cast<const char*>(buf);
    ssize_t write_count = static_cast<ssize_t>(count);
    ssize_t written_count = 0;

    CHECK_LE(0, write_count) << "The count[" << count << "] exceed a maximum value of ssize_t";
    while (write_count > 0) {
        errno = 0;
        written_count = write(_sub_stdin_fd, ptr, write_count);
        if (-1 == written_count) {
            if (errno == EAGAIN) {
                emit();
            } else {
                LOG(FATAL) << "Failed to write, errno[" << errno << "], "
                        << "strerror[" << strerror(errno) << "]";
            }
        }

        if (written_count > 0) {
            ptr += written_count;
            write_count -= written_count;
        }
    }
}

bool PipeProcessor::Impl::readline(Record* record_ptr) {
    CHECK_NOTNULL(record_ptr);
    size_t length = 0;
    do {
        if (NULL == fgets(_read_buffer.get(), _buffer_size, _sub_stdout)) {
            if (feof(_sub_stdout)) {
                LOG(INFO) << "End-of-file reached.";
                return false;
            } else {
                LOG(FATAL) << "Failed to fgets, errno[" << errno << "], "
                        << "strerror[" << strerror(errno) << "]";
            }
        }
        length = strlen(_read_buffer.get());
        while (length > 0 && (_read_buffer[length-1] == '\r' || _read_buffer[length-1] == '\n')) {
            _read_buffer[--length] = '\0';
        }
    } while (length == 0);  // filter empty line

    toft::StringPiece buffer(_read_buffer.get(), length);
    toft::StringPiece field_delimiter(_field_delimiter.c_str(), _field_delimiter.size());

    record_ptr->clear();
    if (1 == _output_fields_num) {
        record_ptr->push_back(buffer.as_string());
    } else {
        // TODO: Maybe use toft::StringPiece better
        size_t pos = 0;
        size_t found = buffer.find(field_delimiter);
        while (found != std::string::npos) {
            record_ptr->push_back(buffer.substr(pos, found - pos).as_string());
            pos = found + field_delimiter.size();
            if (record_ptr->size() == static_cast<size_t>(_output_fields_num - 1)) {
                break;
            }
            found = buffer.find(field_delimiter, pos);
        }
        record_ptr->push_back(buffer.substr(pos, buffer.size() - pos).as_string());
    }
    CHECK_EQ(static_cast<size_t>(_output_fields_num), record_ptr->size()) << "Invalid pipe output";

    return true;
}

bool PipeProcessor::Impl::get_field(std::string* field) {
    int32_t data_size = 0;
    if (1 != fread(&data_size, sizeof(data_size), 1, _sub_stdout)) {
        if (feof(_sub_stdout)) {
            LOG(INFO) << "End-of-file reached.";
            return false;
        } else {
            LOG(FATAL) << "Failed to fread, errno[" << errno << "], "
                    << "strerror[" << strerror(errno) << "]";
        }
    }
    if (data_size == 0) {
        return true;
    }
    CHECK_LE(data_size, _buffer_size) << "Data too large";
    if (1 != fread(_read_buffer.get(), data_size, 1, _sub_stdout)) {
        if (feof(_sub_stdout)) {
            LOG(INFO) << "End-of-file reached.";
            return false;
        } else {
            LOG(FATAL) << "Failed to fread, errno[" << errno << "], "
                    << "strerror[" << strerror(errno) << "]";
        }
    }
    field->assign(_read_buffer.get(), data_size);
    return true;
}

bool PipeProcessor::Impl::readdata(Record* record_ptr) {
    CHECK_NOTNULL(record_ptr);
    bool is_first_field = true;
    record_ptr->clear();
    for (int i = 0; i < _output_fields_num; ++i) {
        std::string field;
        if (!get_field(&field)) {
            CHECK(is_first_field) << "Invalid pipe output";
            return false;
        }
        record_ptr->push_back(field);
        is_first_field = false;
    }
    return true;
}

void PipeProcessor::Impl::read_stdout_func() {
    LOG(INFO) << "read_stdout_func thread begin...";
    while (true) {
        Record* record_ptr = new Record();
        if (_read_func(record_ptr)) {
            while (!_record_queue.push(record_ptr)) {
                DLOG(INFO) << "record queue ringbuffer is full, wait 1 millisecond";
                usleep(1000);
            }
        } else {
            LOG(INFO) << "read stdout done";
            _read_done = true;
            break;
        }
    }
    LOG(INFO) << "read_stdout_func thread end...";
}

void PipeProcessor::Impl::emit() {
    Record* record_ptr = NULL;

    size_t pop_size = 0;
    size_t array_size = sizeof(_record_array) / sizeof(_record_array[0]);
    pop_size = _record_queue.pop(_record_array, array_size);
    for (size_t i = 0; i < pop_size; ++i) {
        record_ptr = _record_array[i];
        CHECK_EQ(static_cast<size_t>(_output_fields_num), record_ptr->size());
        if (1 == _output_fields_num) {
            const std::string& field = record_ptr->operator[](0);
            boost::python::str value_obj(field.data(), field.size());
            _context->emit(&value_obj);
        } else {
            boost::python::tuple tuple_obj = new_tuple(_output_fields_num);
            for (int32_t i = 0; i < _output_fields_num; ++i) {
                const std::string& field = record_ptr->operator[](i);
                boost::python::str field_obj(field.data(), field.size());
                tuple_set_item(tuple_obj, i, field_obj);
            }
            _context->emit(&tuple_obj);
        }
        if (record_ptr != NULL) {
            delete record_ptr;
            record_ptr = NULL;
        }
    }
}

void PipeProcessor::Impl::read_stderr_func() {
    LOG(INFO) << "read_stderr_func thread begin...";
    char* buffer = NULL;
    size_t buffer_size = 0;
    while (true) {
        ssize_t r = getline(&buffer, &buffer_size, _sub_stderr);
        if (-1 == r) {
            break;
        }
        fwrite(buffer, 1, r, stderr);
    }
    if (NULL != buffer) {
        free(buffer);
    }
    fflush(stderr);
    LOG(INFO) << "read_stderr_func thread end...";
}

PipeProcessor::PipeProcessor() : _impl(new PipeProcessor::Impl()) {}
PipeProcessor::~PipeProcessor() {}

void PipeProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    _impl->setup(fns, config);
}

void PipeProcessor::begin_group(
        const std::vector<toft::StringPiece>& keys,
        ProcessorContext* context) {
    _impl->begin_group(keys, context);
}

void PipeProcessor::process(void* object) {
    _impl->process(object);
}

void PipeProcessor::end_group() {
    _impl->end_group();
}

} // namespace python
} // namespace bigflow
} // namespace baidu

