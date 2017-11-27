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

package com.baidu.flume.runtime.spark

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import baidu.flume.Config.PbKVConfig
import baidu.flume.PhysicalPlan.PbJob
import com.baidu.flume.runtime.BigflowRuntimeException
import com.baidu.flume.runtime.spark.impl.util.Utils
import com.google.protobuf.GeneratedMessage
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, IOUtils}

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path, LocalFileSystem}

/**
  * Entry point of a Spark Job on Scala side
  *
  * @author Wang, Cong(bigflow-opensource@baidu.com)
  */
class SparkJob(pbJob: PbJob, preparedArchivePath: String, applicationArchive: String) extends Logging {
  private val conf = new SparkConf()

  val mergedConf: SparkConf = bigflowApplicationConfPass(conf)
  val sparkYarnArchiveKey = "spark.yarn.dist.archives"
  val mergedArchives: String = Utils.mergeCacheFileLists(mergedConf.get(sparkYarnArchiveKey, ""),
                                                 s"$preparedArchivePath#__bigflow_on_spark__",
                                                 s"$applicationArchive#__bigflow_on_spark_application__")
  mergedConf.set(sparkYarnArchiveKey, mergedArchives)
  private val sc = new SparkContext(mergedConf)


  private def bigflowApplicationConfPass(conf: SparkConf): SparkConf = {
    if (pbJob.getType == PbJob.Type.SPARK && pbJob.hasJobConfig) {
      val jobConfig = pbJob.getJobConfig
      for (item: PbKVConfig <- jobConfig.getKvConfigList.asScala
           if item.hasKey && !item.getKey.toStringUtf8.startsWith("spark.")){ // use toStringUtf8 blindly
        val key = item.getKey.toStringUtf8
        lazy val value = item.getValue.toStringUtf8
        key match {
          case hadoop if key.startsWith("hadoop.") => conf.set(hadoop, value)
          // todo: handle cache file and cache archive for all supported cluster managers
          case BigflowConstants.userCacheArchiveKey =>
            val cacheList = Utils.mergeCacheFileLists(conf.get("spark.yarn.dist.archives", ""), value)
            if (cacheList != null) {
              conf.set("spark.yarn.dist.archives", cacheList)
            }
          case BigflowConstants.userCacheFileKey =>
            val cacheList = Utils.mergeCacheFileLists(conf.get("spark.yarn.dist.files", ""), value)
            if (cacheList != null) {
              conf.set("spark.yarn.dist.files", cacheList)
            }
          case _ if key.startsWith(BigflowConstants.confKeyPrefix) => conf.set(key, value)
          case _ =>
        }
      }
    }
    conf
  }

  def run(runType: String, getNodeId: String = null): Boolean = {
//    log.info(s"Start running, runtime type: $runType")
//    if (runType != "PIPE" && runType != "JNI") {
//      throw new BigflowRuntimeException(s"Invalid run-type: $runType")
//    }
//
//
//    val sink = RddPlanBuilder(runType).build(sc, pbJob, getNodeId)
//    val debugOutput = sink.collect()
//
//    assert(debugOutput != null)
//    val uri = "./get-object-for-test"
//    val fs = FileSystem.getLocal(new Configuration())
//    val path = new Path(uri)
//    val key = new BytesWritable
//    val value = new BytesWritable
//
//    val writer = SequenceFile.createWriter(fs, new Configuration(), path, classOf[BytesWritable], classOf[BytesWritable])
//    log.info("created writer")
//    debugOutput.foreach(kv => {
//        key.set(kv._1, 0, kv._1.length)
//        value.set(kv._2, 0, kv._2.length)
//        writer.append(key, value)
//    })
//    debugOutput.foreach(println)
//    IOUtils.closeStream(writer)
    val debugOutput = null
    debugOutput != null
  }
}

object SparkJob extends Logging {

  type OptionMap = Map[Symbol, String]

  def showUsageAndExit(extra: String = ""): Unit = {
    val usage =
      """
      Usage: SparkJob <--pb_job_message path> <--type type>
      """.stripMargin

    if (!extra.isEmpty) {
      Console.err.println(extra)
    }
    Console.err.println(usage)
    sys.exit(-1)
  }

  def parseArgs(argList: List[String]): OptionMap = {

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--pb_job_message" :: value :: tail =>
          nextOption(map ++ Map('pb_job_message -> value.toString), tail)
        case "--type" :: value :: tail =>
          nextOption(map ++ Map('type -> value.toString), tail)
        case "--prepared_archive_path" :: value :: tail =>
          nextOption(map ++ Map('prepared_archive_path -> value.toString), tail)
        case "--application_archive" :: value :: tail =>
          nextOption(map ++ Map('application_archive -> value.toString), tail)
        case "--get_node_id" :: value :: tail =>
          nextOption(map ++ Map('get_node_id -> value.toString), tail)
        case option :: _ =>
          showUsageAndExit(s"Unknown option: $option")
          null
      }
    }

    val parsed = nextOption(Map(), argList)

    if (!(parsed.contains('pb_job_message) && parsed.contains('type) && parsed.contains('prepared_archive_path))) {
      showUsageAndExit()
    }
    parsed
  }

  def main(args: Array[String]): Unit = {
    log.info("Protobuf library path:" + classOf[GeneratedMessage].getProtectionDomain
      .getCodeSource.getLocation)
    val argList = args.toList

    val options = parseArgs(argList)

    val pbJobMessageFile = options('pb_job_message)
    val runType = options('type)
    val preparedArchivePath = options('prepared_archive_path)
    val applicationArchive = options('application_archive)

    val pbJob = PbJob.parseFrom(Files.readAllBytes(Paths.get(pbJobMessageFile)))
    val sparkJob = new SparkJob(pbJob, preparedArchivePath, applicationArchive)
    val getNodeId = options.getOrElse('get_node_id, null)
    sparkJob.run(runType, getNodeId)
  }
}
