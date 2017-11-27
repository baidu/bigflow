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

/**
 * For JVM-native data access(temporarily)
 *
 * @author Wang, Cong(bigflow-opensource@baidu.com)
 */

class KVBuffer(ptr: Long) extends JniObject {
  def reset(): Unit = KVBuffer.jniReset(ptr)

  def hasNext(): Boolean = KVBuffer.jniHasNext(ptr)

  def next(): Unit = KVBuffer.jniNext(ptr)

  def key(): Array[Byte] = KVBuffer.jniKey(ptr)

  def value(): Array[Byte] = KVBuffer.jniValue(ptr)
}

object KVBuffer {
  @native def jniReset(ptr: Long): Unit

  // call next
  @native def jniNext(ptr: Long): Unit

  // call hasNext
  @native def jniHasNext(ptr: Long): Boolean

  // call key
  @native def jniKey(ptr: Long): Array[Byte]

  // call value
  @native def jniValue(ptr: Long): Array[Byte]
}
