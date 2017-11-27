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
现在有一个文件，文件里有一堆数字，以空格和换行隔开。
求这个文件中所有数字的平均值(下取整）
"""

import os
from bigflow import base, input, output
#从标准输入读取文件列表，文件之间空格隔开
pipeline = base.Pipeline.create('LOCAL')

dir = os.path.dirname(os.path.abspath(__file__)) + "/data"

numbers = pipeline.read(input.TextFile(dir + "/" + "number.text"))\
         .flat_map(lambda line: line.split()) \
         .map(lambda n: int(n))
def avg(p):
    return p.sum() / p.count()

print numbers.apply(avg).get()
