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

import com.baidu.flume.runtime.spark.Logging

/**
  * File description
  *
  * @author Wang, Cong(wangcong09@baidu.com)
  */
object LibraryLoader extends Logging {
  val libraryPath = System.getProperty("java.library.path")
  log.info(s"Current java.library.path: $libraryPath")

  log.info("Trying to load Bigflow Python runtime(libbflpyrt.so)...")
  System.loadLibrary("bflpyrt")
  log.info("Loaded Bigflow Python runtime")

  def mention(): Unit = {
    // Do nothing but to make sure LibraryLoader is initialized
  }
}
