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

###############
# example_uv.py
###############

from bigflow import base
from bigflow import input
from bigflow import output
import sys

# 设置spark配置
spark_conf = {
    "spark.app.name": "Bigflow on Spark: group_top_n " ,
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

#case_str = "case4_2"
input_path = sys.argv[1]
output_path = sys.argv[2]

# 可通过 parallelize 构造P类型

data = pipeline.read(input.TextFile(input_path))
# 在P类型上应用transforms
result = data.map(lambda x: x.split()).group_by_key()\
        .apply_values(lambda x: x.max_elements(5, lambda x: x)).flatten()\
        .map(lambda t: "%s %s" % (t[0], t[1]))

# 当前预览版不支持get操作，只能通过pipelined的write方法将P类型写入文件系统
pipeline.write(result, output.TextFile(output_path))
pipeline.run()
