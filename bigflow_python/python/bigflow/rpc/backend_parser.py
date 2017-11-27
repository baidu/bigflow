#
# Copyright (c) 2015 Baidu, Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
"""
Backend log parser, in progress

"""
from bigflow.util.log import logger


class _BackendLogParseBase(object):

    def accept(self, line):
        """accept
        """
        return False

    def act(self, line):
        """act
        """
        pass


class _SimpleParser(_BackendLogParseBase):
    def __init__(self, substr, replace):
        self.substr = substr
        self.replace = replace

    #Override
    def accept(self, line):
        return self.substr in line

    #Override
    def act(self, line):
        if isinstance(self.replace, list):
            for r in self.replace:
                logger.info(r)
        else:
            logger.info(self.replace)


class _HadoopLogParser(_BackendLogParseBase):

    hadoop_stderr_msg = "Hadoop client STDERR:"

    #Override
    def accept(self, line):
        return _HadoopLogParser.hadoop_stderr_msg in line

    #Override
    def act(self, line):
        pos = line.find(_HadoopLogParser.hadoop_stderr_msg)
        logger.info(line[pos:])


class _SparkDriverParser(_BackendLogParseBase):
    spark_log_levels = [" INFO ", " ERROR ", " WARN "]
    hadoop_client_stderr = "Hadoop client STDERR:"

    class ConsecutiveLinesWithLeadingSpaces(object):
        """ Accumulator for consecutive lines which have leading space. The line before the block of
        consecutive lines is stored as `pre_mismatched_line` if any.

        This accumulator is used to capture java's exception msg, spark driver's additional msg.

        todo(yexianjin): This is a quick hack, should be re-implemented in a proper parser.
        """

        def __init__(self, space_num):
            self.pre_mismatched_line = None
            self.leading_w_space = "\t"
            self.space_num = space_num
            self.lines = []

        def accept(self, line):
            # line starts with at least `space_num` white spaces or starts with '\t'
            matched = len(line[:self.space_num].strip()) == 0 or \
                      line.startswith(self.leading_w_space)
            if not matched:
                if not self.empty():  # buffered log should be flushed
                    logger.info(self.msg())
                    self.reset()
                self.pre_mismatched_line = line
            return matched

        def append(self, line):
            self.lines.append(line)

        def reset(self, space_num=None):
            self.lines[:] = []
            self.pre_mismatched_line = None
            if space_num and isinstance(space_num, int):
                self.space_num = space_num

        def reset_mismatch_line(self):
            self.pre_mismatched_line = None

        def empty(self):
            return len(self.lines) == 0

        def msg(self):
            lines = ([self.pre_mismatched_line] if self.pre_mismatched_line else []) + self.lines
            return "\n".join(lines)

    def __init__(self):
        # at least two leading white spaces
        self._con_lines_with_space = _SparkDriverParser.ConsecutiveLinesWithLeadingSpaces(2)

    def accept(self, line):
        matched = any(level in line for level in _SparkDriverParser.spark_log_levels) and \
                  _SparkDriverParser.hadoop_client_stderr not in line
        if self._con_lines_with_space.accept(line) and not matched:
            self._con_lines_with_space.append(line)
        return matched

    def act(self, line):
        pos = 0
        for level in _SparkDriverParser.spark_log_levels:
            pos = line.find(level)
            if pos != -1:
                break
        self._con_lines_with_space.reset_mismatch_line()
        logger.info(line[pos+1:])


class _LocalInputUriParser(_BackendLogParseBase):

    #Override
    def accept(self, line):
        return "io_format.cpp" in line and "split uri" in line

    #Override
    def act(self, line):
        pos = line.find("split uri : ") + len("split uri : ")
        logger.info("Reading input: %s" % line[pos:])


_PARSERS = [
        _SimpleParser("start to launch job...", "Start new job"),
        _SimpleParser("Planner start optimizing", "Backend planner start optimizing"),
        _SimpleParser(
            "Planner finished optimizing",
            ["Backend planner finished optimizing", "Start running job"]),
        _HadoopLogParser(),
        _LocalInputUriParser(),
        _SparkDriverParser(),
        ]


def backend_parser(line):
    """backend_parser
    """
    line = line.rstrip("\n")
    for parser in _PARSERS:
        if parser.accept(line):
            parser.act(line)


if __name__ == "__main__":
    import sys
    for line in sys.stdin:
        backend_parser(line)
