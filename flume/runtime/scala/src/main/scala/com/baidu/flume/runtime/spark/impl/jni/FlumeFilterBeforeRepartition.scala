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


import baidu.flume.PhysicalPlan.{PbSparkRDD, PbSparkTask}
import com.baidu.flume.runtime.spark.impl.ShuffleHeader

import scala.collection.BitSet

/** Filter function for BigFlow's merged RDD
  *
  * @author Ye, Xianjin(bigflow-opensource@baidu.com)
  *         Zheng, Gonglin(bigflow-opensource@baidu.com)
  *
  */
class FlumeFilterBeforeRepartition(pbRDD: PbSparkRDD) extends Serializable {
  val taskIndicesSet: BitSet = BitSet() ++ pbRDD
    .getTaskList.toArray
    .map(p => p.asInstanceOf[PbSparkTask].getTaskIndex)

  def apply(key: Array[Byte], value: Array[Byte]): Boolean = {
    taskIndicesSet.contains(ShuffleHeader.taskIndex(key))
  }
}
