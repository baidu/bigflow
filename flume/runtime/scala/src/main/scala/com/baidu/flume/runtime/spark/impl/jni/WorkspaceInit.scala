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

import java.io.{File, IOException}
import java.nio.file.Files

import com.baidu.flume.runtime.spark.Logging
import com.baidu.flume.runtime.spark.impl.util.Utils


/**
  * Initialize workspace for bigflow on spark application
  *
  * @author Ye, Xianjin(yexianjin@baidu.com)
  */
object WorkspaceInit extends Logging {
  val currentWorkDir = new File(".").getAbsoluteFile
  val bigflowOnSparkApp = new File(currentWorkDir, "__bigflow_on_spark_application__")

  log.debug(s"current wok dir: $currentWorkDir, application dir: $bigflowOnSparkApp")

  if (!bigflowOnSparkApp.exists() || bigflowOnSparkApp.isFile) {
    throw new IOException("__bigflow_on_spark_application dir doesn't exist")
  }
  val commonItems = Utils.dirCommonItems(currentWorkDir, bigflowOnSparkApp)
  if (commonItems.nonEmpty) {
    throw new IOException("BigFlow on spark application's cache files or resource files collide with cache archive, " +
      s"collision items are ${commonItems.mkString(",")}")
  }
  // soft link all items under bigflow on spark application to current work dir.
  for (item <- bigflowOnSparkApp.listFiles()) {
    Files.createSymbolicLink(currentWorkDir.toPath.resolve(item.getName), item.toPath)
  }

  // todo: determine whether to delete the soft links or not
  def mention(): Unit = {
    // do nothing but make sure workspace is initialized.
  }
}
