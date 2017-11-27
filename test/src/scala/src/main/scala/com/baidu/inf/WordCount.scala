package com.baidu.inf

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("usage: spark-submit com.baidu.inf.WordCount <input> <output>")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.setAppName("WordCount-" + System.getenv("USER"))

    val sc = new SparkContext(conf)

    // input file uri, could be any hdfs uri OR local fs uri.
    // for example:
    //  hdfs://szwg-ecomon-hdfs.dmop.baidu.com:54310/app/dc/spark/test.log
    //  file:///tmp/test.log
    // Note: local fs uri could only be used in spark local mode(set spark.master=local)
    val input = args(0)

    // output file uri, could be any hdfs uri OR local fs uri.
    val output = args(1)


    val file = sc.textFile(input)
    val counts = file.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
    counts.saveAsTextFile(output)

    counts.take(10).foreach(println)
    System.exit(0)
  }
}
