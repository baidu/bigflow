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

from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint


def parseLine(line):
    parts = line.split(',')
    label = float(parts[0])
    features = Vectors.dense([float(x) for x in parts[1].split(' ')])
    return LabeledPoint(label, features)

if __name__ == "__main__":
    input_path = "hdfs:///bayes_input/"
    output_path = "hdfs:///bayes_output/"

    sc = SparkContext(appName="PythonNaiveBayesExample")

    # $example on$
    data = sc.textFile(input_path).map(parseLine)

    # Split data aproximately into training (60%) and test (40%)
    training, test = data.randomSplit([0.6, 0.4], seed=0)

    # Train a naive Bayes model.
    model = NaiveBayes.train(training, 1.0)

    # Make prediction and test accuracy.
    predictionAndLabel = test.map(lambda p: (model.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()

    result = model.predict(Vectors.dense(0.0,2.0,0.0,1.0))

    print("accuracy-->" + str(accuracy))
    print("Predictionof (0.0, 2.0, 0.0, 1.0):"+ str(result))

    # Save and load model
    #model.save(sc, "target/tmp/myNaiveBayesModel")
    #val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
    pv = (accuracy,result)
    out = sc.parallelize( [pv] )
    out.saveAsTextFile(output_path)
    # Save and load model
    #model.save(sc, "/home/miaodongdong/data/bayes/myNaiveBayesModel")
    #sameModel = NaiveBayesModel.load(sc, "/home/miaodongdong/data/bayes/myNaiveBayesModel")
