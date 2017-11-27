# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/env python
"""
mock hadoop client module
"""

import json
import argparse
import json
import os
import sys
import uuid


class HadoopCmd(object):
    """ Hadoop Cmd """

    def __init__(self, args):
        parser = argparse.ArgumentParser('hadoop')
        parser.add_argument('-conf', action='append')
        parser.add_argument('-jobconf', dest='jobconf', action='append')
        parser.add_argument('-D', dest='jobconf', action='append')
        parser.add_argument('-get', dest='fs_op', action='store_const', const='get')
        parser.add_argument('-put', dest='fs_op', action='store_const', const='put')
        parser.add_argument('-du', dest='fs_op', action='store_const', const='du')
        parser.add_argument('-status', dest='counter_op', action='store_const', const='status')
        parser.add_argument('-rmr', dest='fs_op', action='store_const', const='rmr')
        parser.add_argument('-touchz', dest='fs_op', action='store_const', const='touchz')
        parser.add_argument('-test', dest='fs_op', action='store_const', const='test')
        parser.add_argument('-e', action='store_true')
        parser.add_argument('-cacheArchive', dest='cache_archive', action='append')
        parser.add_argument('-cacheFile', dest='cache_file', action='append')
        parser.add_argument('-mapdebug', nargs='?')
        parser.add_argument('-reducedebug', nargs='?')
        parser.add_argument('-input', nargs='?')
        parser.add_argument('-output', nargs='?')
        parser.add_argument('-vertex', nargs='?')
        parser.add_argument('-file', action='append')
        parser.add_argument('remain', nargs = argparse.REMAINDER)

        parsed = parser.parse_args(args[1:])
        self.type = args[0]
        self.fs_op = parsed.fs_op
        if parsed.jobconf is None:
            parsed.jobconf = []
        self.jobconf = dict(s.split('=') for s in parsed.jobconf)
        self.cache_archive = parsed.cache_archive
        self.cache_file = parsed.cache_file
        self.remain = parsed.remain
        self.file = parsed.file

    def vertex_num(self):
        """ get vertex num """
        return int(self.jobconf['abaci.dag.vertex.num'])

    def vertex_memory(self, num, dft=None):
        """ get vertex memory """
        ret = self.jobconf.get('hadoop.hce.memory.limit.%d' % num, None)
        if ret is None:
            return dft
        else:
            return int(ret)

    def vertex_concurrency(self, num, dft=None):
        """ get vertex concurrency """
        ret = self.jobconf.get('mapred.reduce.tasks.%d' % num, None)
        ret = self.jobconf.get('mapred.map.tasks.%d' % num, ret)
        if ret is None:
            return dft
        else:
            return int(ret)

    def __str__(self):
        return str(self.__dict__)

import os

tmp_path = os.path.dirname(os.path.abspath(__file__)) + '/bigflow-hadoop-cmds'


class MockHadoopClient(object):
    """ MockHadoopClient """
    def __init__(self):
        self._uuid = str(uuid.uuid4())
        self.hadoop_cmd_file = tmp_path + "." + self._uuid
        abs_dir = os.path.dirname(os.path.abspath(__file__))
        self.mock_hadoop_client_path = os.path.join(abs_dir, "mock_hadoop_client.py_" + self._uuid)
        # os.path.abspath(__file__) could be mock_hadoop_client.pyc
        os.link(os.path.join(abs_dir, "mock_hadoop_client.py"), self.mock_hadoop_client_path)

    def reset(self):
        if os.path.isfile(self.hadoop_cmd_file):
            os.remove(self.hadoop_cmd_file)

    def __del__(self):
        if os.path.exists(self.mock_hadoop_client_path):
            os.unlink(self.mock_hadoop_client_path)
        if os.path.isfile(self.hadoop_cmd_file):
            os.remove(self.hadoop_cmd_file)

    def recent_cmds(self, fn=None):
        """ get all cmds """
        if fn is None:
            fn = lambda arg: True
        args = map(json.loads, file(self.hadoop_cmd_file))
        args = [HadoopCmd(arg) for arg in args]
        return filter(fn, args)

    def recent_fs_cmds(self):
        """ get recent fs cmds """
        return self.recent_cmds(lambda arg: arg.type == 'fs')

    def recent_job_cmds(self):
        """ get recent job cmds """
        return self.recent_cmds(lambda arg: arg.type == 'hce')

if __name__ == "__main__":
    hadoop_cmd_file = tmp_path
    mock_client_name = "mock_hadoop_client.py"
    client_pos = sys.argv[0].find(mock_client_name)
    if client_pos >= 0 and len(sys.argv[0][client_pos + len(mock_client_name):]):
        _uuid = sys.argv[0][client_pos + len(mock_client_name) + 1:]
        hadoop_cmd_file = hadoop_cmd_file + "." + _uuid

    ofile = open(hadoop_cmd_file, 'a+')
    print >> ofile, json.dumps(sys.argv[1:])


