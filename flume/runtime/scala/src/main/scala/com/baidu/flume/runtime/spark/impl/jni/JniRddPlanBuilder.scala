/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

package com.baidu.flume.runtime.spark.impl.jni

import baidu.flume.PhysicalPlan.{PbJob, PbSparkRDD, PbSparkTask}
import com.baidu.flume.runtime.spark.impl.FlumePartitioner
import com.baidu.flume.runtime.spark.impl.util.Utils
import com.baidu.flume.runtime.spark.{Logging, RddPlanBuilder}
import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{SequenceFileAsBinaryInputFormat, TextInputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.reflect.ClassTag

/**
  * A [[RddPlanBuilder]] that build JNI-based RDD plans
  *
  * @author Wang, Cong(wangcong09@baidu.com)
  */
class JniRddPlanBuilder extends RddPlanBuilder with Logging {

  type CacheRddMap = RddPlanBuilder.CacheRddMap

  implicit def ArrayByteOrdering: Ordering[Array[Byte]] = Ordering.fromLessThan[Array[Byte]](
    // TODO(wangcong09) Performance may suffer
    UnsignedBytes.lexicographicalComparator().compare(_, _) < 0
  )

  /**
    * Utility wrapper for SparkContext.union, which only unions (>=2 size) rdd list
    * @param sc SparkContext
    * @param rdds A list of RDDs
    * @tparam T
    * @return Union RDD or first rdd in the one-sized rdd list
    */
  private def scUnionOrGet[T: ClassTag](sc: SparkContext, rdds: Seq[RDD[T]]): RDD[T] = {
    if (rdds.size == 1) {
      rdds.head
    } else {
      sc.union(rdds)
    }
  }

  override def build(context: SparkContext, pbJob: PbJob, cacheRdds: CacheRddMap)
        : Option[RDD[(Array[Byte], Array[Byte])]] = {
    val indexToRdd = scala.collection.mutable.Map[Int, RDD[(Array[Byte], Array[Byte])]]()
    assert(pbJob != null)
    val pbSparkJob = pbJob.getSparkJob
    val indexToPbRdd = pbSparkJob.getRddList.asScala map (pbRDD => pbRDD.getRddIndex -> pbRDD) toMap

    def buildOrGet(pbSparkRdd: PbSparkRDD): RDD[(Array[Byte], Array[Byte])] = {
      assert(pbSparkRdd.getTaskCount > 0)

      def create_input_rdd(pbTask: PbSparkTask): RDD[(BytesWritable, BytesWritable)] = {
        if (pbTask.hasHadoopInput) {
          val hadoopInput = pbTask.getHadoopInput
          val uris = hadoopInput.getUriList.asScala
          val path = uris.mkString(",")
          val displayName = Utils.truncatedString(uris, ",")
          val inputFormatString = hadoopInput.getInputFormat
          inputFormatString match {
            case "TextInputFormat" => context
              .hadoopFile(
                path,
                classOf[TextInputFormat],
                classOf[LongWritable],
                classOf[Text],
                context.defaultMinPartitions
              )
              .setName(displayName)
              .map(pair => (new BytesWritable(), new BytesWritable(pair._2.getBytes, pair._2.getLength)))
            case "SequenceFileAsBinaryInputFormat" => context
              .hadoopFile(path,
                classOf[SequenceFileAsBinaryInputFormat],
                classOf[BytesWritable],
                classOf[BytesWritable],
                context.defaultMinPartitions
              )
              .setName(displayName)
              .map(pair => (pair._1, pair._2))
          }
        } else {
          assert(pbTask.hasCacheInput)
          val cacheNodeId = pbTask.getCacheInput.getCacheNodeId
          cacheRdds(cacheNodeId)._2.map(pair => (new BytesWritable(pair._1), new BytesWritable(pair._2)))
        }
      }

      def buildInputsForRdd(pbSparkRdd: PbSparkRDD): RDD[(Array[Byte], Array[Byte])] = {
        val filterDataForThisRdd = new FlumeFilterBeforeRepartition(pbSparkRdd)
        val parents = pbSparkRdd.getParentRddIndexList.asScala.map(index => buildOrGetByIndex(index.intValue()))
        scUnionOrGet(context, parents).filter(p => filterDataForThisRdd(p._1, p._2))
      }

      val getRdd = indexToRdd.get(pbSparkRdd.getRddIndex)
      if (getRdd.isDefined) {
        getRdd.get
      } else {
        val rddType = pbSparkRdd.getTask(0).getType
        val rdd = rddType match {
          case PbSparkTask.Type.INPUT =>
            assert(pbSparkRdd.getTaskCount == 1, "Input Rdd can have only 1 task")
            val pbTask = pbSparkRdd.getTask(0)
            val result = create_input_rdd(pbTask)
              .mapPartitionsWithIndex(
                new FlumeTaskInputFunction(pbSparkJob.getJobInfo.toByteArray, pbTask.toByteArray)
              ).setName(s"BigflowRDD[${pbSparkRdd.getRddIndex}]")
            log.info(s"Partitions of input: ${result.partitions}")
            result

          case PbSparkTask.Type.GENERAL =>
            buildInputsForRdd(pbSparkRdd)
              .repartitionAndSortWithinPartitions(
                new FlumePartitioner(pbSparkRdd.getConcurrency)
              ).setName("Shuffle")
              .mapPartitionsWithIndex(
                new FlumeTaskGeneralFunction(pbSparkJob.getJobInfo.toByteArray, pbSparkRdd.toByteArray)
              ).setName(s"BigflowRDD[${pbSparkRdd.getRddIndex}]")

          case PbSparkTask.Type.CACHE =>
            val cacheRdd = buildInputsForRdd(pbSparkRdd)
            val task = pbSparkRdd.getTaskList.asScala(0)
            cacheRdds.put(task.getCacheNodeId, (task.getCacheEffectiveKeyNumber, cacheRdd))
            cacheRdd.cache()
        }
        indexToRdd.put(pbSparkRdd.getRddIndex, rdd)
        rdd
      }
    }

    def buildOrGetByIndex(rddIndex: Int) : RDD[(Array[Byte], Array[Byte])] = {
      buildOrGet(indexToPbRdd(rddIndex))
    }

    var rdd: RDD[(Array[Byte], Array[Byte])] = null
    for (pbSparkRDD <- pbSparkJob.getRddList.asScala) {
      rdd = buildOrGet(pbSparkRDD)
    }
    if (rdd != null) {
      val withoutOutDegreePbSparkRdds = JniRddPlanBuilder.findWithoutOutDegreeRDDs(pbSparkJob.getRddList.asScala)
      log.info(s"withoutOutDegreePbSparkRdds.length = ${withoutOutDegreePbSparkRdds.length}")
      assert(withoutOutDegreePbSparkRdds.nonEmpty)

      val sink = scUnionOrGet(context, withoutOutDegreePbSparkRdds.map(buildOrGet))
        .setName("FakeRDDUnionZeroOutDegreeRDD.")
        .filter(_ => false)
      Some(sink)
    } else {
      None
    }
  }
}

object JniRddPlanBuilder extends Logging {

  def findWithoutOutDegreeRDDs(rddList: Iterable[PbSparkRDD]): Seq[PbSparkRDD] = {
    val allRddIndices = rddList.map(pbRDD => pbRDD.getRddIndex).toSeq.distinct

    val outDegreeRddIndices = rddList.flatMap(pbRDD => pbRDD.getParentRddIndexList.asScala.map(_.intValue))
      .toSeq.distinct

    val withoutOutDegreeRddIndices = allRddIndices.diff(outDegreeRddIndices)

    val rddIndexToProto = rddList map (pbRDD => pbRDD.getRddIndex -> pbRDD) toMap

    val ret = withoutOutDegreeRddIndices.map(index => rddIndexToProto(index))

    ret
  }

}

