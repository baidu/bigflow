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
  * A simple wrapper of sun.misc.Unsafe for direct memory access
  *
  * @author Wang, Cong(wangcong09@baidu.com)
  */
object Unsafe {

  private val Unsafe = {
    val f = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    val unsafe = f.get(null).asInstanceOf[sun.misc.Unsafe]
    unsafe
  }

  val SizeOfInt = 4
  val ByteArrayOffset: Int = Unsafe.arrayBaseOffset(classOf[Array[Byte]])

  def allocateMemory(size: Long): Long = Unsafe.allocateMemory(size)
  def freeMemory(address: Long): Unit = Unsafe.freeMemory(address)
  def copyMemory(src: AnyRef, srcOffset: Long, dst: AnyRef, dstOffset: Long, length: Long): Unit = {
    // TODO(wangcong09) Overlap?
    Unsafe.copyMemory(src, srcOffset, dst, dstOffset, length)
  }

  def putInt(address: Long, data: Int): Unit = Unsafe.putInt(address, data)
  def getInt(address: Long): Int = Unsafe.getInt(address)
  def putLong(address: Long, data: Long): Unit = Unsafe.putLong(address, data)
  def getLong(address: Long): Long = Unsafe.getLong(address)
  def putByte(address: Long, data: Byte): Unit = Unsafe.putByte(address, data)
  def getByte(address: Long): Byte = Unsafe.getByte(address)

  def putBytes(address: Long, data: Array[Byte]): Long = putBytes(address, data, data.length)
  def putBytes(address: Long, data: Array[Byte], length: Int): Long = {
    putInt(address, length)
    copyMemory(data, ByteArrayOffset, null, address + SizeOfInt, length)
    address + SizeOfInt + length
  }

  def getBytes(address: Long): Array[Byte] = {
    val data = new Array[Byte](getInt(address))
    copyMemory(null, address + SizeOfInt, data, ByteArrayOffset, data.length)
    data
  }
}
