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
    #l = 25000000#50G
    l = 27500000#55G
    #l = 10
    return range( l*num,l*(num + 1))

def main():
    #set ouput path
    output_path = "hdfs:///group_top_n_input/"
    conf = SparkConf().setAppName("PySpark: group_top_n_conut generator "+case_str)
    sc = SparkContext(conf = conf)
    sqlCtx = SQLContext(sc)

    lines = sc.parallelize(range(0,100),50)
    res = lines.flatMap(f).map(lambda x:str(x)+" "+str(x))

    res.saveAsTextFile(output_path)

if __name__ == '__main__':
    main()
