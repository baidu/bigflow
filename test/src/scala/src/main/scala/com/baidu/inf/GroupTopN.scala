package com.baidu.inf

import org.apache.spark._
import org.apache.spark.SparkContext._

object GroupTopN {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("usage: spark-submit com.baidu.inf.WordCount <input> <output>")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.setAppName("group_top_n-" + System.getenv("USER"))

    val sc = new SparkContext(conf)

    // input file uri, could be any hdfs uri OR local fs uri.
    // for example:
    //  hdfs://baidu.com:1234/app/dc/spark/test.log
    //  file:///tmp/test.log
    // Note: local fs uri could only be used in spark local mode(set spark.master=local)
    val input = args(0)

    // output file uri, could be any hdfs uri OR local fs uri.
    val output = args(1)

    val topcount = args(2).toInt


    val file = sc.textFile(input,250)
    val counts = file.map(line => line.split(" "))
                     .map(x => (x(0),x(1)))
                     .groupByKey()
                     .mapValues(iter => iter.toList.sorted(Ordering[String].reverse).take(topcount))
                     .flatMapValues( iter =>iter )
                     .map(x => x._1 + " " + x._2 )
    //counts.foreach(println)
    counts.saveAsTextFile(output)

    System.exit(0)
  }
}
