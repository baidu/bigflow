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

#!/usr/bin/python
# usage: spark-submit wordCount.py

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
import re
from operator import add

def f(num):
    #l = 400000#139G
    l = 157500#55G
    #l = 10
    return range( l*num,l*(num + 1))

def main():
    output_path = "hdfs:///group_top_n_input/"
    #output_path = "file:///home/miaodongdong/code/bigflow/test/src/generator/outout"
    conf = SparkConf().setAppName("PySpark: group_top_n_conut generator ")
    sc = SparkContext(conf = conf)
    sqlCtx = SQLContext(sc)

    lines = sc.parallelize(range(0,250),250)
    res = lines.flatMap(f).flatMap(lambda x:[str(x)+" "+str(i) for i in xrange(10000,10100)])

    res.saveAsTextFile(output_path)

if __name__ == '__main__':
    main()
