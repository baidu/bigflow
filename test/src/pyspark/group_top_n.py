#!/usr/bin/python
# encoding:utf-8

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

# usage: spark-submit wordCount.py

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
import re,sys
from operator import add
from pyspark.serializers import MarshalSerializer

def main():
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    conf = SparkConf().setAppName("PySpark: group_top_n ")#.set("spark.defalut.parallelism","250")
    sc = SparkContext(conf = conf)
    #sc = SparkContext(conf = conf,serializer=MarshalSerializer())
    sqlCtx = SQLContext(sc)

    lines = sc.textFile(input_path,250)
    res = lines.map(lambda line: re.split(' ', line))\
            .groupByKey()\
            .mapValues(lambda x :  sorted(x,reverse=True)[:5])\
            .flatMapValues(lambda x:x)\
            .map(lambda x: str(x[0]) +" "+ str(x[1]))

    res.saveAsTextFile(output_path)

if __name__ == '__main__':
    main()
