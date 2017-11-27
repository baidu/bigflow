#!/bin/bash
ROOT_DIR=$(cd `dirname $0`; pwd)
SCALA_DIR=$ROOT_DIR/src/scala
BIGFLOW_DIR=$ROOT_DIR/src/bigflow
PYSPARK_DIR=$ROOT_DIR/src/pyspark

#IN_CASE="case6"#150M
#OUT_CASE="4g_75_2"
#case7 102G case8 50G
#case5 11G key/value 10w/10w
#case10 55G/1key  case9 110G key/value 20w/5w  case12 value 100
IN_CASE="case12"
OUT_CASE="4g_75_2_3"

INPUT=$ROOT_DIR"/testdata/group_top_n_input/"
OUTPUT=$ROOT_DIR"/testdata/group_top_n_output/"

cd $SCALA_DIR
spark-submit --name "scala group_top_n $IN_CASE $OUT_CASE" --class com.baidu.inf.GroupTopN ./target/spark-starter2-1.0-SNAPSHOT.jar $INPUT $OUTPUT/scala 5 &

cd $BIGFLOW_DIR
pyrun group_top_n.py $INPUT $OUTPUT/bigflow &

cd $PYSPARK_DIR
pyspark --name "pyspark group_top_n $IN_CASE $OUT_CASE" group_top_n.py $INPUT $OUTPUT/pyspark&


