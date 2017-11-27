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

# coding: utf-8
from bigflow import input, output, base, transforms
import random

# 设置spark配置
spark_conf = {
    "spark.app.name": "Bigflow on Spark:  data generator for group_top_n",
    "spark.master": "yarn",
}

pipeline = base.Pipeline.create(
    # 指定计算引擎为"spark"或"SPARK"
    "spark",

    # 指定tmp_data_path
    tmp_data_path="hdfs:///app/dc/bigflow/tmp",

    # 指定spark配置
    spark_conf=spark_conf,

    # default_concurrency不是必须选项，该example数据量小可以设置小一些
    default_concurrency=250,
    )

#pipeline = base.Pipeline.create('local')

#set input
input_path ="hdfs:///input_count"

#set output
output_path = "hdfs:///group_top_n_input/"

def f(x):
    #count = 100000000 #55G
    count = 200000000 #110G
    pos = 0
    l =[]
    while( pos < count):
        pid = random.randint(0,200000)
        score = random.randint(0,50000)
        l.append(str(pid)+" "+ str(score))
        pos = pos + 1
    return l

lines = pipeline.read(input.TextFile(input_path))
lines = lines.flat_map(f)
pipeline.write(lines, output.TextFile(output_path))
pipeline.run()
