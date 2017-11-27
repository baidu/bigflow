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

#!filecoding:utf-8
"""
输入文件内容为，每行代表网址被某个人的一次访问，按tab分隔：
baidu.com   lucy
g.cn    lily
163.com xiaoming
qq.com  xiaoming
baidu.com   xiaoming
163.com xiaoming
qq.com  lucy
baidu.com   lily

计算得出每个网址访问的uv（被不同人访问的次数）:
g.cn    1
qq.com  2
baidu.com   3
163.com 1
"""

import os
from bigflow import base, input, output, transforms
#输入是pcollection，对其做distinct和count，即求每个网址的uv
def count_distinct(p):
    return p.distinct().count()
#创建pipeline
_pipeline = base.Pipeline.create("LOCAL")

dir = os.path.dirname(os.path.abspath(__file__)) + "/data"
input_path = dir + "/" + "uv.text"
#读取输入并格式化
col = _pipeline.read(input.TextFile(input_path))
col = col.map(lambda x:x.split())
#按网址分组，并对每个网址求uv
col = col.group_by_key().apply_values(count_distinct).flatten()
col = col.map(lambda x:x[0] + "\t" + str(x[1]))
#写输出
_pipeline.write(col, output.TextFile("/tmp/website_uv"))
_pipeline.run()
