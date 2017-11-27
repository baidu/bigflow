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

package com.baidu.flume.runtime.spark.backend

import java.nio.ByteBuffer

import baidu.flume.PhysicalPlan.{PbJob, PbSparkRDD, PbSparkTask}

import baidu.flume.thrift_gen._
import com.baidu.flume.runtime.spark.{Logging, RddPlanBuilder, SparkContextWrapper}
import com.google.protobuf.ByteString
import org.apache.thrift.async.AsyncMethodCallback
import org.apache.spark.rdd.RDD

/**
  * Concrete thrift service implementation of Flume Backend Server
  *
  * Flume Backend Server provides 3 kinds of RPC requests, namely:
  * - runJob: run a spark job mapped by physical plan
  * - getCachedData: get cached node data in previous iterations
  * - stop: stop the thrift server
  *
  * @author Ye, Xianjin(yexianjin@baidu.com)
            Zhang, Yuncong(zhangyuncong@baidu.com)
*/


private[backend] class FlumeBackendServiceImpl(val flumeBackend: FlumeBackendServer)
  extends TBackendService.AsyncIface with Logging {

  type VoidHandlerType = AsyncMethodCallback[TVoidResponse]
  type CacheHandlerType = AsyncMethodCallback[TGetCachedDataResponse]

  val planBuilder = RddPlanBuilder("JNI")
  val cacheRdds = scala.collection.mutable.Map[String, (Int, RDD[(Array[Byte], Array[Byte])])]()

  /**
    * Run a spark job mapped by the physical job. Response are sent back to client through thrift
    * transport. The TResponseStatus will be set succeed if spark job finishes normally, otherwise
    * failure is set and exception msg is set as reason.
    *
    * @param physical_job binary data, serialization of flume's physical job
    * @param resultHandler thrift's callback handler, used to sent back response.
    */
  override def runJob(physical_job: ByteBuffer, resultHandler: AsyncMethodCallback[_]): Unit = {
    log.info("RunJob request")
    val status = new TResponseStatus()
    status.setSuccess(true)
    try {

      val pbJob = PbJob.parseFrom(ByteString.copyFrom(physical_job))
      SparkContextWrapper.pbJobConfig = pbJob.getJobConfig
      val sc = SparkContextWrapper.getSparkContext()
      val sink = planBuilder.build(sc, pbJob, cacheRdds)
      sink.map(_.collect()).orNull
    } catch {
      case e: Exception =>
        e.printStackTrace()
        log.error(e.getLocalizedMessage)
        status.setSuccess(false)
        status.setReason(e.getLocalizedMessage)
        resultHandler.asInstanceOf[VoidHandlerType].onComplete(new TVoidResponse().setStatus(status))
        return
    }
    resultHandler.asInstanceOf[VoidHandlerType].onComplete(new TVoidResponse().setStatus(status))
  }

  /**
    * Get cached node data and send back to client.
    *
    * In flume, a node represents an operation along with its result. Node(except SinkNode) can be
    * viewed as data source. Cached data means the node's result is cached after
    * operation(/computation), the corresponding result can be retrieved without recomputing.
    *
    * When running on Spark, cached node maps to a new RDD(which is cached by Spark's own cache
    * interface). node_id identifies the node during the whole session, we use node_id to find the
    * cached RDD, an collect operation is issued to get data and then send back to client
    *
    * @param node_id uuid string, identifies the node
    * @param resultHandler thrift's callback handler, used to sent back response
    */
  override def getCachedData(node_id: String, resultHandler: AsyncMethodCallback[_]): Unit = {
    def parseKey(keyBuffer: ByteBuffer, keyNum: Int): List[ByteBuffer] = {
      def getBuffer(inputBuf: ByteBuffer, length: Int): ByteBuffer = {
        val buf = new Array[Byte](length)
        inputBuf.get(buf)
        ByteBuffer.wrap(buf)
      }
      var ret = List[ByteBuffer]()
      for (i <- 1 to keyNum - 1) {
        val len = keyBuffer.getInt()
        assert(len < keyBuffer.remaining(), s"expect len at least [${len}], and real is [${keyBuffer.remaining()}]")
        ret = ret :+ getBuffer(keyBuffer, len)
      }
      ret = ret :+ getBuffer(keyBuffer, keyBuffer.remaining())
      ret
    }

    log.info("GetCachedData request")
    val (keyNum, rdd) = cacheRdds(node_id)
    val output = rdd.collect()
    val status: TResponseStatus = new TResponseStatus().setSuccess(true)
    val response = new TGetCachedDataResponse().setStatus(status)
    val shuffleHeaderLength = 3
    for  ((key, value) <- output) {
      val keysValue = new TKeysValue()
      // include global key
      val keyStripHeader = key.slice(shuffleHeaderLength, key.length)
      for (keyElem <- parseKey(ByteBuffer.wrap(keyStripHeader), keyNum + 1)) {
        keysValue.addToKeys(keyElem)
      }
      keysValue.value = ByteBuffer.wrap(value)
      response.addToData(keysValue)
    }
    resultHandler.asInstanceOf[CacheHandlerType].onComplete(response)
  }

  /**
    * Stop thrift service
    *
    * @param resultHandler thrift's callback handler
    */
  override def stop(resultHandler: AsyncMethodCallback[_]): Unit = {
    log.info("Received stop request")
    resultHandler.asInstanceOf[AsyncMethodCallback[Void]].onComplete(null)
    flumeBackend.exit()
  }

}
