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

package com.baidu.flume.runtime.spark.impl

import org.apache.spark.Partitioner

import com.baidu.flume.runtime.spark.Logging

/**
  * Flume Partitioner which reads key's shuffle header and extract corresponding partition.
  * @author Wang, Cong(wangcong09@baidu.com)
  *         Ye, Xianjin(yexianjin@baidu.com)
  *
  */
class FlumePartitioner(partitions: Int) extends Partitioner with Logging {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    ShuffleHeader.partition(key.asInstanceOf[Array[Byte]])
  }
}

