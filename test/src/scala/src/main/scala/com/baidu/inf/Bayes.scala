package com.baidu.inf

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext,SparkConf}

object Bayes {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("usage: spark-submit com.baidu.inf.WordCount <input> <output>")
      System.exit(1)
    }
    val conf =new SparkConf()
    val sc =new SparkContext(conf)
    
    var input_path = args(0)
    var output_path = args(1)

    //读入数据
    val data = sc.textFile(input_path)
    val parsedData =data.map { line =>
      val parts =line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }
    // 把数据的60%作为训练集，40%作为测试集.
    val splits = parsedData.randomSplit(Array(0.6,0.4),seed = 11L)
    val training =splits(0)
    val test =splits(1)

    //获得训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改
    val model =NaiveBayes.train(training,lambda = 1.0)

    //对模型进行准确度分析
    val predictionAndLabel= test.map(p => (model.predict(p.features),p.label))
    val accuracy =1.0 *predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println("accuracy-->"+accuracy)
    val result = model.predict(Vectors.dense(0.0,2.0,0.0,1.0))
    println("Predictionof (0.0, 2.0, 0.0, 1.0):"+ result)

    // Save and load model
    //model.save(sc, "target/tmp/myNaiveBayesModel")
    //val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
    val pv = (accuracy,result)
    val out = sc.parallelize( List(pv) )
    
    out.saveAsTextFile(output_path)
  }
}
