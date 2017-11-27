## How to build

    mvn package

## How to run

    spark-submit ./target/

>> make sure you have installed maven & Spark Client. 
>> Ref: http://baidu.com/display/SPRK/Getting+Started

## com.baidu.inf.WordCount

## com.baidu.inf.PBLogReader

read protobuf log in spark

usage: 

```
park-submit --driver-class-path target/spark-starter2-1.0-SNAPSHOT-jar-with-dependencies.jar \
            --class com.baidu.inf.PBLogReader \
            target/spark-starter2-1.0-SNAPSHOT-jar-with-dependencies.jar \
            /app/dc/spark/zhenpeng/casio-pb.log
```
