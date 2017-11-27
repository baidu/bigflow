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

import baidu.flume.PhysicalPlan.{PbSparkRDD, PbSparkTask, PbJob, PbSparkJob}
import com.baidu.flume.runtime.spark.Logging
import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.TaskContext

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * File description
  *
  * @author Wang, Cong(wangcong09@baidu.com)
  */
class FlumeTask(val selfPtr: Long, outputSuffix: String = "Global") {
  val outputBuffer = new KVBuffer(FlumeTask.jniGetOutputBufferPtr(selfPtr))

  FlumeTask.jniRun(selfPtr, outputSuffix)

  def processInput(key: Array[Byte], keyLength: Int, value: Array[Byte], valueLength: Int): Unit =
    FlumeTask.jniProcessInput(selfPtr, key, keyLength, value, valueLength)

  def processInputKey(key: Array[Byte], keyLength: Int): Unit = {
    FlumeTask.jniProcessKey(selfPtr, key, keyLength)
  }

  def processInputValue(value: Array[Byte], valueLength: Int): Unit = {
    FlumeTask.jniProcessValue(selfPtr, value, valueLength)
  }

  def inputDone(): Unit = FlumeTask.jniInputDone(selfPtr)

  def getOutputBuffer: KVBuffer = outputBuffer
}

object FlumeTask extends JniObject {

  def buildFrom(pbSparkJobInfo: PbSparkJob.PbSparkJobInfo, pbSparkTask: PbSparkTask, partitionId: Int): FlumeTask = {
    val suffix: String = {
      val tc = TaskContext.get()
      if (tc eq null) {
        "global"
      } else {
        tc.taskAttemptId().toString
      }
    }

    val taskCtxBuilder = PbSparkTask.PbRuntimeTaskContext.newBuilder().setTaskAttemptId(suffix)
    val newPbSparkTask = PbSparkTask.newBuilder(pbSparkTask).setRuntimeTaskCtx(taskCtxBuilder).build()
    val pbBytes = newPbSparkTask.toByteArray
    new FlumeTask(jniBuildTask(pbSparkJobInfo.toByteArray, pbBytes, partitionId), suffix)
  }

  def release(flumeTask: FlumeTask): Unit = jniReleaseTask(flumeTask.selfPtr)

  @native protected def jniBuildTask(pbSparkJobInfo: Array[Byte], pbSparkRDD: Array[Byte], partitionId: Int): Long

  @native protected def jniReleaseTask(ptr: Long): Unit

  @native protected def jniGetOutputBufferPtr(ptr: Long): Long

  @native protected def jniRun(selfPtr: Long, info: String): Unit

  @native protected def jniProcessInput(selfPtr: Long, key: Array[Byte], keyLength: Int, value:
  Array[Byte], valueLength: Int): Unit

  @native protected def jniProcessKey(selfPtr: Long, key: Array[Byte], keyLength: Int): Unit

  @native protected def jniProcessValue(selfPtr: Long, value: Array[Byte], valueLength: Int): Unit

  @native protected def jniInputDone(selfPtr: Long): Unit
}

abstract private[jni] class FlumeTaskFunction[InputValueType, OutputValueType] extends
  Function2[Int, Iterator[InputValueType], Iterator[OutputValueType]] with Serializable with
  Logging {

  object TaskStatus extends Enumeration {
    type TaskStatus = Value
    val INIT, INPUT_DONE, ALL_DONE = Value
  }

  protected abstract class FlumeTaskRunnerIterator(partitionIndex: Int, input:
  Iterator[InputValueType])
    extends
      Iterator[OutputValueType] with Serializable with Logging {

    def newTask(): FlumeTask

    def processInput(inputValue: InputValueType): Unit

    private var status = TaskStatus.INIT

    @transient val flumeTask: FlumeTask = newTask()
    @transient val outputBuffer: KVBuffer = flumeTask.getOutputBuffer

    TaskContext.get().addTaskCompletionListener(_ => {
      log.warn(s"Releasing FlumeTask at ${flumeTask.selfPtr}")
      FlumeTask.release(flumeTask)
      log.warn(s"Done releasing FlumeTask.")
    })

//     TaskContext.get().addTaskCompletionListener(_ => {
//      log.info(s"Releasing FlumeTask at ${flumeTask.selfPtr}")
//      FlumeTask.release(flumeTask)
//      log.info(s"Done releasing FlumeTask.")
//    })

    @tailrec
    override final def hasNext: Boolean = {
      status match {
        case TaskStatus.INIT =>
          if (outputBuffer.hasNext()) {
            true
          } else {
            while (input.hasNext && !outputBuffer.hasNext()) {
              outputBuffer.reset()
              processInput(input.next())
            }
            if (!input.hasNext) {
              flumeTask.inputDone()
              status = TaskStatus.INPUT_DONE
              hasNext
            } else {
              // Now outputBuffer.hasNext is true
              true
            }
          }
        case TaskStatus.INPUT_DONE =>
          if (outputBuffer.hasNext()) {
            true
          } else {
            status = TaskStatus.ALL_DONE
            // TODO(wangcong09) Release flumeTask through TaskContext callback
            // FlumeTask.release(flumeTask)
            false
          }
        case TaskStatus.ALL_DONE =>
          false
      }
    }

    override def next(): OutputValueType = {
      outputBuffer.next()
      (outputBuffer.key(), outputBuffer.value()).asInstanceOf[OutputValueType]
    }
  }

  def createIterator(partitionIndex: Int, input: Iterator[InputValueType]):
  Iterator[OutputValueType]

  override def apply(partitionIndex: Int, input: Iterator[InputValueType]):
  Iterator[OutputValueType] = {
    log.info("Start running a new iterator")
    val iterator = createIterator(partitionIndex, input)
    log.info(s"Create a new $iterator, current thread: " + Thread.currentThread().getName)
    iterator
  }
}

class FlumeTaskInputFunction(pbJobInfoBytes: Array[Byte], pbSparkTaskBytes: Array[Byte]) extends
  FlumeTaskFunction[(BytesWritable, BytesWritable), (Array[Byte], Array[Byte])] {

  assert(pbJobInfoBytes != null)
  assert(pbSparkTaskBytes != null)

  // private val emptyKey = "".getBytes()

  override def createIterator(partitionIndex: Int, input: Iterator[(BytesWritable, BytesWritable)]) = new
      FlumeTaskRunnerIterator(partitionIndex, input) {

    override def processInput(inputValue: (BytesWritable, BytesWritable)): Unit = {
      flumeTask.processInput(
        inputValue._1.getBytes,
        inputValue._1.getLength,
        inputValue._2.getBytes,
        inputValue._2.getLength)
    }

    override def newTask(): FlumeTask = FlumeTask.buildFrom(
      PbSparkJob.PbSparkJobInfo.parseFrom(pbJobInfoBytes),
      PbSparkTask.parseFrom(pbSparkTaskBytes),
      // Noted: partitionIndex should be used instead of TaskContext's partitionID which represents
      // final RDD's partition rather than this RDD's parent RDD.
      partitionIndex)
  }
}

class FlumeTaskGeneralFunction(pbSparkJobInfoBytes: Array[Byte], pbSparkRDDBytes: Array[Byte]) extends
  FlumeTaskFunction[(Array[Byte], Array[Byte]), (Array[Byte], Array[Byte])] {

  private val comparator = UnsignedBytes.lexicographicalComparator()

  override def createIterator(partitionIndex: Int, input: Iterator[(Array[Byte], Array[Byte])]) =
    new
        FlumeTaskRunnerIterator(partitionIndex, input) {
      var previousKey: Array[Byte] = _

      override def newTask(): FlumeTask = {
        val pbSparkRDD = PbSparkRDD.parseFrom(pbSparkRDDBytes)
        val tasks = pbSparkRDD.getTaskList.asScala.filter(
          p => {
            partitionIndex >= p.getPartitionOffset && partitionIndex < p.getPartitionOffset + p
              .getConcurrency
          }
        )
        require(tasks.size == 1, s"Cannot get task from partition [$partitionIndex], RDD: " +
          s"\n$pbSparkRDD")
        FlumeTask.buildFrom(PbSparkJob.PbSparkJobInfo.parseFrom(pbSparkJobInfoBytes), tasks.head, partitionIndex)
      }

      override def processInput(inputValue: (Array[Byte], Array[Byte])): Unit = {
        def equalBytes(o1: Array[Byte], o2: Array[Byte]) = comparator.compare(o1, o2) == 0

        if (previousKey == null || !equalBytes(previousKey, inputValue._1)) {
          previousKey = inputValue._1
          flumeTask.processInputKey(previousKey, previousKey.length)
        }
        flumeTask.processInputValue(inputValue._2, inputValue._2.length)
      }
    }
}

