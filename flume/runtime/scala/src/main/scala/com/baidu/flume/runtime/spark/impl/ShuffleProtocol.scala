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

/**
  * A JVM's implementation of BigFlow/DCE's shuffle protocol
  *
  * Shuffle Header layout:
  * task_index(uint8) | (partition|place_hold(1 bit))(uint16) | data
  * @todo create write function if needed
  *
  * @author Ye, Xianjin(bigflow-opensource@baidu.com)
  */
object ShuffleHeader {
  def taskIndex(key: Array[Byte]): Int = {
    assert(key.length >= 3)
    key(0) & 0xff
  }

  /**
    * Extract partition info from key ByteArray
    * @param key: ByteArray
    * @return partition info encoded in the key's shuffle header
    */
  def partition(key: Array[Byte]): Int = {
    assert(key.length >= 3)
    ((key(1) & 0xff) << 8 | (key(2) & 0xff)) >> 1
  }

  def isPlaceHolder(key: Array[Byte]): Boolean = {
    assert(key.length >= 3)
    (key(2) & 1) == 0
  }

}
