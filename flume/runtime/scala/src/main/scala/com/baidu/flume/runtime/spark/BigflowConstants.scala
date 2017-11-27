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

/**
  * Constants used by Bigflow's spark runtime
  *
  * @author Ye, Xianjin(yexianjin@baidu.com)
  */
object BigflowConstants {
  val confKeyPrefix = "__bigflow_on_spark__."
  val userCacheFileKey = "__bigflow_on_spark__.cache.files"
  val userCacheArchiveKey = "__bigflow_on_spark__.cache.archives"

  val exceptionPathEnvKey = "BIGFLOW_PYTHON_EXCEPTION_TOFT_STYLE_PATH"

  /**
    * exception path. Suffix password is needed to hide real value in Spark UI. This should be
    * unnecessary after Spark 2.1.2+
    */
  val exceptionPathEnvShadedKey = "BIGFLOW_PYTHON_EXCEPTION_TOFT_STYLE_PATH_WITH_PASSWORD"
}
